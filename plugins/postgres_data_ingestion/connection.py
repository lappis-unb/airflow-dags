import psycopg2
import psycopg2.extras


def get_connection(config):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        dbname=config["database"],
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )


def close_connection(cur, conn):
    cur.close()
    conn.close()
