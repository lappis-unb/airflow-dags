import logging
import time
from datetime import datetime, timezone

from plugins.postgres_data_ingestion.connection import close_connection, get_connection
from plugins.postgres_data_ingestion.decoder import MessageDecoder


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def start_replication(config, publication_name, slot_name, status_interval=2, timeout_seconds=10):
    conn = get_connection(config)
    cur = conn.cursor()
    options = {"publication_names": publication_name, "proto_version": "1"}
    cur.start_replication(slot_name=slot_name, decode=False, options=options, status_interval=status_interval)

    read_started_at = datetime.now(timezone.utc)
    last_message_received_at = datetime.now(timezone.utc)
    decoded_messages = []
    try:
        while True:
            message = cur.read_message()
            if message:
                last_message_received_at = datetime.now(timezone.utc)
                logging.info("Message received")
                decoder = MessageDecoder(pgoutput_message=_message, starting_position=0)
                decoded_message = decoder.decode_pgoutput()
                if (
                    decoded_message["type"] == "B"
                    and decoded_message.get("commit_timestamp", datetime(1970, 1, 1)).replace(
                        tzinfo=timezone.utc
                    )
                    >= read_started_at
                ):
                    logging.info("Message sent after replication was started")
                    break

                decoded_messages.append(decoded_message)
                message.cursor.send_feedback(flush_lsn=message.data_start)
            else:
                if (datetime.now(timezone.utc) - last_message_received_at).total_seconds() >= timeout_seconds:
                    break
                else:
                    time.sleep(1)
    except Exception as e:
        raise e
    finally:
        time.sleep(status_interval + 5)
        logging.info("Closing connection")
        close_connection(cur, conn)

    return decoded_messages


if __name__ == "__main__":
    import os

    import dotenv

    dotenv.load_dotenv()

    config = {
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "database": "postgres",
    }

    publication_name = "test_publication"
    slot_name = "test_slot"

    messages = start_replication(
        config=config, publication_name=publication_name, slot_name=slot_name, timeout_seconds=5
    )
    for message in messages:
        print(message)
