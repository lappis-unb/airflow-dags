from datetime import datetime, timedelta, timezone


class MessageDecoder:
    """Helper class to decode byte messages from pgoutput."""

    def __init__(self, pgoutput_message, starting_position=0):
        """
        Initialize the MessageDecoder with a pgoutput message.

        Args:
        ----
            pgoutput_message: The pgoutput message to decode.
            starting_position (int, optional): The initial position to start decoding from. Defaults to 0.
        """
        self.position = starting_position
        self.message = pgoutput_message.payload
        self.data_size = pgoutput_message.data_size
        self.data_start = pgoutput_message.data_start
        self.send_time = pgoutput_message.send_time
        self.wal_end = pgoutput_message.wal_end
        self.message_type = self.read_bytes(bytes_to_read=1, decode_as="str")

    def decode_string(self, bytes_to_decode):
        """Decode bytes to a UTF-8 string."""
        return bytes_to_decode.decode("utf-8")

    def decode_int(self, bytes_to_decode):
        """Decode bytes to an integer."""
        return int.from_bytes(bytes_to_decode, byteorder="big", signed=True)

    def read_bytes(self, bytes_to_read, decode_as):
        """
        Read and decode a specified number of bytes.

        Args:
        ----
            bytes_to_read (int): Number of bytes to read.
            decode_as (str): The format to decode the bytes ('str' or 'int').

        Returns:
        -------
            Decoded value.
        """
        decode_functions = {
            "str": self.decode_string,
            "int": self.decode_int,
        }
        decode_function = decode_functions[decode_as]
        decoded_byte = decode_function(self.message[self.position : (self.position + bytes_to_read)])
        self.position += bytes_to_read
        return decoded_byte

    def read_string(self) -> str:
        """Read a null-terminated string from the message."""
        output = bytearray()
        char = self.message[self.position : (self.position + 1)]
        while char != b"\x00":
            output += char
            self.position += 1
            char = self.message[self.position : (self.position + 1)]
        self.position += 1  # Skip the null terminator
        return self.decode_string(output)

    def convert_pg_ts(self, pg_ts: int) -> datetime:
        """Convert PostgreSQL timestamp to a datetime object."""
        ts = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        return ts + timedelta(microseconds=pg_ts)

    def read_tuple_data(self):
        """Read and decode tuple data."""
        number_of_columns = self.read_bytes(bytes_to_read=2, decode_as="int")
        column_values = {}
        for c in range(number_of_columns):
            tuple_type = self.read_bytes(bytes_to_read=1, decode_as="str")
            if tuple_type == "t":
                text_length = self.read_bytes(bytes_to_read=4, decode_as="int")
                col_value = self.read_bytes(bytes_to_read=text_length, decode_as="str")
                column_values[f"column_{c}"] = col_value
            elif tuple_type == "n":
                column_values[f"column_{c}"] = None
            elif tuple_type == "u":
                column_values[f"column_{c}"] = "TOASTED COLUMN"
            else:
                raise ValueError("Unexpected tuple type")
        return column_values

    def decode_begin_message(self):
        """Decode a BEGIN message."""
        lsn = self.read_bytes(bytes_to_read=8, decode_as="int")
        pg_ts = self.read_bytes(bytes_to_read=8, decode_as="int")
        transaction_id = self.read_bytes(bytes_to_read=4, decode_as="int")
        commit_timestamp = self.convert_pg_ts(pg_ts)
        return {
            "lsn": lsn,
            "transaction_id": transaction_id,
            "commit_timestamp": commit_timestamp,
        }

    def decode_truncate_message(self):
        """Decode a TRUNCATE message."""
        number_of_relations = self.read_bytes(bytes_to_read=4, decode_as="int")
        truncate_option = self.read_bytes(bytes_to_read=2, decode_as="int")
        truncated_relations = [
            self.read_bytes(bytes_to_read=4, decode_as="int") for _ in range(number_of_relations)
        ]
        return {
            "number_of_relations": number_of_relations,
            "truncate_option": truncate_option,
            "truncated_relations": truncated_relations,
        }

    def decode_commit_message(self):
        """Decode a COMMIT message."""
        is_unused = self.read_bytes(bytes_to_read=1, decode_as="int")
        commit_lsn = self.read_bytes(bytes_to_read=8, decode_as="int")
        transaction_lsn = self.read_bytes(bytes_to_read=8, decode_as="int")
        pg_ts = self.read_bytes(bytes_to_read=8, decode_as="int")
        commit_timestamp = self.convert_pg_ts(pg_ts)
        return {
            "is_unused": is_unused,
            "commit_lsn": commit_lsn,
            "transaction_lsn": transaction_lsn,
            "commit_timestamp": commit_timestamp,
        }

    def decode_relation_message(self):
        """Decode a RELATION message."""
        relation_id = self.read_bytes(bytes_to_read=4, decode_as="int")
        namespace = self.read_string()
        relation_name = self.read_string()
        identity_setting = self.read_bytes(bytes_to_read=1, decode_as="int")
        number_of_columns = self.read_bytes(bytes_to_read=2, decode_as="int")
        columns_info = {}
        for c in range(number_of_columns):
            is_key = self.read_bytes(bytes_to_read=1, decode_as="int")
            column_name = self.read_string()
            col_dtype = self.read_bytes(bytes_to_read=4, decode_as="int")
            type_modifier = self.read_bytes(bytes_to_read=4, decode_as="int")
            columns_info[f"column_{c}"] = {
                "column_name": column_name,
                "key": is_key,
                "data_type": col_dtype,
                "data_type_modifier": type_modifier,
            }
        return {
            "relation_id": relation_id,
            "namespace": namespace,
            "relation_name": relation_name,
            "identity_setting": identity_setting,
            "columns": columns_info,
        }

    def decode_insert_message(self):
        """Decode an INSERT message."""
        relation_id = self.read_bytes(bytes_to_read=4, decode_as="int")
        new_tuple_identifier = self.read_bytes(bytes_to_read=1, decode_as="str")
        if new_tuple_identifier != "N":
            raise ValueError(f"Expected 'N' for new tuple, got {new_tuple_identifier}")
        inserted_row = self.read_tuple_data()
        return {"relation_id": relation_id, "inserted_row": inserted_row}

    def decode_update_message(self):
        """Decode an UPDATE message."""
        relation_id = self.read_bytes(bytes_to_read=4, decode_as="int")
        tuple_identifier = self.read_bytes(bytes_to_read=1, decode_as="str")
        replica_identity_changed = tuple_identifier == "K"
        old_column_values = None
        if tuple_identifier in ["O", "K"]:
            old_column_values = self.read_tuple_data()
            tuple_identifier = self.read_bytes(bytes_to_read=1, decode_as="str")
        if tuple_identifier == "N":
            new_column_values = self.read_tuple_data()
        else:
            raise ValueError(f"Expected 'N' for new tuple, got {tuple_identifier}")
        return {
            "relation_id": relation_id,
            "replica_identity_changed": replica_identity_changed,
            "old_column_values": old_column_values,
            "new_column_values": new_column_values,
        }

    def decode_delete_message(self):
        """Decode a DELETE message."""
        relation_id = self.read_bytes(bytes_to_read=4, decode_as="int")
        tuple_identifier = self.read_bytes(bytes_to_read=1, decode_as="str")
        replica_identity_changed = tuple_identifier == "K"
        if tuple_identifier in ["O", "K"]:
            deleted_column_values = self.read_tuple_data()
        else:
            raise ValueError(f"Expected 'O' or 'K' for tuple identifier, got {tuple_identifier}")
        return {
            "relation_id": relation_id,
            "replica_identity_changed": replica_identity_changed,
            "deleted_column_values": deleted_column_values,
        }

    def decode_message_payload(self):
        """Decode the message payload based on the message type."""
        decode_functions = {
            "B": self.decode_begin_message,
            "R": self.decode_relation_message,
            "I": self.decode_insert_message,
            "U": self.decode_update_message,
            "D": self.decode_delete_message,
            "C": self.decode_commit_message,
            "T": self.decode_truncate_message,
        }
        decoded_payload = decode_functions[self.message_type]()
        return decoded_payload

    def decode_pgoutput(self):
        """Decode the entire pgoutput message."""
        decoded_message = {
            "type": self.message_type,
            "send_time": self.send_time,
            "data_start": self.data_start,
            "wal_end": self.wal_end,
            "data_size": self.data_size,
        }
        decoded_payload = self.decode_message_payload()
        decoded_message.update(decoded_payload)
        return decoded_message


if __name__ == "__main__":

    class DebugMessage:
        """Debug class for simulating pgoutput messages."""

        def __init__(self, message_payload):
            self.message_payload = message_payload

        @property
        def payload(self):
            return self.message_payload

        @property
        def data_size(self):
            return "data_size"

        @property
        def data_start(self):
            return "data_start"

        @property
        def send_time(self):
            return "send_time"

        @property
        def wal_end(self):
            return "wal_end"

    # Example pgoutput messages
    messages = [
        b"B\x00\x00\x00:\x91\x00\x8c\x10\x00\x02\xbe\x00\xff\x84\x9a\x8c\x00\r\x9dZ",
        b"R\x00\x00C\x9bpublic\x00dummy_table\x00f\x00\x04\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01name\x00\x00\x00\x04\x13\x00\x00\x006\x01value\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01birthdate\x00\x00\x00\x04:\xff\xff\xff\xff",
        b"D\x00\x00C\x9bO\x00\x04t\x00\x00\x00\x0248t\x00\x00\x00\x03Heyt\x00\x00\x00\x03100t\x00\x00\x00\n2024-05-10",
        b"C\x00\x00\x00\x00:\x91\x00\x8c\x10\x00\x00\x00:\x91\x00\x8c@\x00\x02\xbe\x00\xff\x84\x9a\x8c",
        b"B\x00\x00\x00:\x91\x01\xaf\xc8\x00\x02\xbe\x01\x01o\xca\xeb\x00\r\x9dd",
        b"R\x00\x00C\x9bpublic\x00dummy_table\x00f\x00\x03\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01name\x00\x00\x00\x04\x13\x00\x00\x006\x01value\x00\x00\x00\x00\x17\xff\xff\xff\xff",
        b"T\x00\x00\x00\x01\x00\x00\x00C\x9b",
        b"C\x00\x00\x00\x00:\x91\x01\xaf\xc8\x00\x00\x00:\x91\x01\xb0\x98\x00\x02\xbe\x01\x01o\xca\xeb",
        b"B\x00\x00\x00:\x91\x01\xb3`\x00\x02\xbe\x01\x02\x04<|\x00\r\x9df",
        b"R\x00\x00C\x9bpublic\x00dummy_table\x00f\x00\x03\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01name\x00\x00\x00\x04\x13\x00\x00\x006\x01value\x00\x00\x00\x00\x17\xff\xff\xff\xff",
        b"I\x00\x00C\x9bN\x00\x03t\x00\x00\x00\x0249t\x00\x00\x00\x05Alicet\x00\x00\x00\x0210",
        b"I\x00\x00C\x9bN\x00\x03t\x00\x00\x00\x0250t\x00\x00\x00\x03Bobt\x00\x00\x00\x0220",
        b"I\x00\x00C\x9bN\x00\x03t\x00\x00\x00\x0251t\x00\x00\x00\x07Charliet\x00\x00\x00\x0230",
        b"C\x00\x00\x00\x00:\x91\x01\xb3`\x00\x00\x00:\x91\x01\xb3\x90\x00\x02\xbe\x01\x02\x04<|",
        b"B\x00\x00\x00:\x92\x00\x01(\x00\x02\xbe\x01\x03\xa7|e\x00\r\x9dm",
        b"U\x00\x00C\x9bO\x00\x03t\x00\x00\x00\x0250t\x00\x00\x00\x03Bobt\x00\x00\x00\x0220N\x00\x03t\x00\x00\x00\x0250t\x00\x00\x00\x04testt\x00\x00\x00\x0220",
        b"C\x00\x00\x00\x00:\x92\x00\x01(\x00\x00\x00:\x92\x00\x01X\x00\x02\xbe\x01\x03\xa7|e",
        b"B\x00\x00\x00:\x92\x00\x01\xe8\x00\x02\xbe\x01\x03\xf3C'\x00\r\x9do",
        b"U\x00\x00C\x9bO\x00\x03t\x00\x00\x00\x0251t\x00\x00\x00\x07Charliet\x00\x00\x00\x0230N\x00\x03t\x00\x00\x00\x0251nt\x00\x00\x00\x0230",
        b"C\x00\x00\x00\x00:\x92\x00\x01\xe8\x00\x00\x00:\x92\x00\x02\x18\x00\x02\xbe\x01\x03\xf3C'",
        b"B\x00\x00\x00:\x92\x00\x06x\x00\x02\xbe\x01\x04v^p\x00\r\x9ds",
        b"R\x00\x00C\x9bpublic\x00dummy_table\x00f\x00\x04\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01name\x00\x00\x00\x04\x13\x00\x00\x006\x01value\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01birthdate\x00\x00\x00\x04:\xff\xff\xff\xff",
        b"I\x00\x00C\x9bN\x00\x04t\x00\x00\x00\x0252t\x00\x00\x00\x03Heyt\x00\x00\x00\x03100t\x00\x00\x00\n2024-05-10",
        b"C\x00\x00\x00\x00:\x92\x00\x06x\x00\x00\x00:\x92\x00\x06\xa8\x00\x02\xbe\x01\x04v^p",
        b"B\x00\x00\x00:\x92\x00i\xf0\x00\x02\xbe\x01\x05:\xfdK\x00\r\x9dv",
        b"D\x00\x00C\x9bO\x00\x04t\x00\x00\x00\x0252t\x00\x00\x00\x03Heyt\x00\x00\x00\x03100t\x00\x00\x00\n2024-05-10",
        b"C\x00\x00\x00\x00:\x92\x00i\xf0\x00\x00\x00:\x92\x00j \x00\x02\xbe\x01\x05:\xfdK",
    ]

    # Decode each message and print the decoded message
    for message in messages:
        _message = DebugMessage(message)
        decoder = MessageDecoder(pgoutput_message=_message, starting_position=0)
        decoded_message = decoder.decode_pgoutput()
        print(decoded_message)
