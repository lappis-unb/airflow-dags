from dateutil.parser import parse as date_parse


def parse_column_values(values, columns_info):
    type_map = {
        16: bool,  # boolean
        17: bytes,  # bytea
        18: str,  # char
        19: str,  # name
        20: int,  # int8
        21: int,  # int2
        23: int,  # int4
        25: str,  # text
        700: float,  # float4
        701: float,  # float8
        1042: str,  # bpchar
        1043: str,  # varchar
        1082: date_parse,  # date
        1083: str,  # time
        1114: date_parse,  # timestamp
        1184: date_parse,  # timestamptz
        1700: float,  # numeric
        2950: str,  # uuid
        3802: str,  # jsonb
        114: str,  # json
        142: str,  # xml
        650: str,  # cidr
        869: str,  # inet
        829: str,  # macaddr
        600: str,  # point
        601: str,  # lseg
        602: str,  # path
        603: str,  # box
        604: str,  # polygon
        628: str,  # line
        718: str,  # circle
        790: float,  # money
        1560: bytes,  # bit
        1562: bytes,  # varbit
        1266: str,  # timetz
        1186: str,  # interval
        1790: str,  # refcursor
        2202: int,  # regprocedure
        2203: int,  # regoper
        2204: int,  # regoperator
        2205: int,  # regclass
        2206: int,  # regtype
        3926: str,  # int8range
        3904: str,  # int4range
        3906: str,  # numrange
        3908: str,  # tsrange
        3910: str,  # tstzrange
        3912: str,  # daterange
    }

    return {
        columns_info[k]["column_name"]: (type_map[columns_info[k]["data_type"]](v) if v is not None else None)
        for k, v in values.items()
    }


def generate_change_record(
    current_changes,
    message_type,
    transaction_timestamp,
    old_values,
    new_values,
    transaction_id,
    transaction_start_lsn,
):
    current_changes = current_changes or []
    current_changes.append(
        {
            "operation_type": message_type,
            "operation_timestamp": transaction_timestamp,
            "row_before_transaction": old_values,
            "row_after_transaction": new_values,
            "_transaction_id": transaction_id,
            "_transaction_start_lsn": transaction_start_lsn,
        }
    )
    return current_changes


def handle_begin_message(decoded_message):
    return (
        decoded_message["lsn"],
        decoded_message["transaction_id"],
        decoded_message["commit_timestamp"],
    )


def handle_relation_message(decoded_message, relation_map):
    relation_map[decoded_message["relation_id"]] = {
        "schema": decoded_message["namespace"],
        "table_name": decoded_message["relation_name"],
        "columns": decoded_message["columns"],
    }


def handle_commit_message(decoded_message, aux_table_changes, table_changes):
    transaction_end_lsn = decoded_message["transaction_lsn"]
    commit_lsn = decoded_message["commit_lsn"]

    for k, v in aux_table_changes.items():
        for change in v:
            if commit_lsn != change["_transaction_start_lsn"]:
                raise Exception("Commit message transaction start does not match message block start.")
            change["_transaction_end_lsn"] = transaction_end_lsn

        table_changes[k] = table_changes.get(k, []) + v

    return {}


def handle_data_message(decoded_message, relation_map, transaction_info, aux_table_changes, message_type):
    relation_id = decoded_message["relation_id"]
    relation_info = relation_map[relation_id]
    columns_info = relation_info["columns"]
    ftq = f"{relation_info['schema']}.{relation_info['table_name']}"

    old_values = None
    new_values = None

    if message_type == "I":
        new_values = parse_column_values(decoded_message["inserted_row"], columns_info)
    elif message_type == "U":
        old_values = parse_column_values(decoded_message["old_column_values"], columns_info)
        new_values = parse_column_values(decoded_message["new_column_values"], columns_info)
    elif message_type == "D":
        old_values = parse_column_values(decoded_message["deleted_column_values"], columns_info)

    aux_table_changes[ftq] = generate_change_record(
        current_changes=aux_table_changes.get(ftq),
        message_type=message_type,
        transaction_timestamp=transaction_info["timestamp"],
        old_values=old_values,
        new_values=new_values,
        transaction_id=transaction_info["id"],
        transaction_start_lsn=transaction_info["start_lsn"],
    )


def process_messages(decoded_messages):
    aux_table_changes = {}
    table_changes = {}
    relation_map = {}
    transaction_info = {}

    for decoded_message in decoded_messages:
        message_type = decoded_message["type"]

        if message_type == "B":
            transaction_info = {
                "start_lsn": decoded_message["lsn"],
                "id": decoded_message["transaction_id"],
                "timestamp": decoded_message["commit_timestamp"],
            }

        elif message_type == "R":
            handle_relation_message(decoded_message, relation_map)

        elif message_type == "C":
            aux_table_changes = handle_commit_message(decoded_message, aux_table_changes, table_changes)

        elif message_type in {"I", "U", "D"}:
            handle_data_message(
                decoded_message,
                relation_map,
                transaction_info,
                aux_table_changes,
                message_type,
            )

        elif message_type == "T":
            for relation_id in decoded_message["truncated_relations"]:
                relation_info = relation_map[relation_id]
                ftq = f"{relation_info['schema']}.{relation_info['table_name']}"

                aux_table_changes[ftq] = generate_change_record(
                    current_changes=aux_table_changes.get(ftq),
                    message_type=message_type,
                    transaction_timestamp=transaction_info["timestamp"],
                    old_values=None,
                    new_values=None,
                    transaction_id=transaction_info["id"],
                    transaction_start_lsn=transaction_info["start_lsn"],
                )
        else:
            raise Exception(f"Unrecognized message type: {message_type}")

    if message_type != "C":
        raise Exception(f"Message block finished with message of type {message_type} instead of C")

    return table_changes


if __name__ == "__main__":
    from datetime import datetime, timezone

    import pandas as pd

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 100)

    decoded_messages = [
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251540835344,
            "transaction_id": 892250,
            "commit_timestamp": datetime(2024, 6, 16, 13, 57, 29, 579148, tzinfo=timezone.utc),
        },
        {
            "type": "R",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "namespace": "public",
            "relation_name": "dummy_table",
            "identity_setting": 102,
            "columns": {
                "column_0": {
                    "column_name": "id",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_1": {
                    "column_name": "name",
                    "key": 1,
                    "data_type": 1043,
                    "data_type_modifier": 54,
                },
                "column_2": {
                    "column_name": "value",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_3": {
                    "column_name": "birthdate",
                    "key": 1,
                    "data_type": 1082,
                    "data_type_modifier": -1,
                },
            },
        },
        {
            "type": "D",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "replica_identity_changed": False,
            "deleted_column_values": {
                "column_0": "48",
                "column_1": "Hey",
                "column_2": "100",
                "column_3": "2024-05-10",
            },
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251540835344,
            "transaction_lsn": 251540835392,
            "commit_timestamp": datetime(2024, 6, 16, 13, 57, 29, 579148, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251540910024,
            "transaction_id": 892260,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 1, 769707, tzinfo=timezone.utc),
        },
        {
            "type": "R",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "namespace": "public",
            "relation_name": "dummy_table",
            "identity_setting": 102,
            "columns": {
                "column_0": {
                    "column_name": "id",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_1": {
                    "column_name": "name",
                    "key": 1,
                    "data_type": 1043,
                    "data_type_modifier": 54,
                },
                "column_2": {
                    "column_name": "value",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
            },
        },
        {
            "type": "T",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "number_of_relations": 1,
            "truncate_option": 0,
            "truncated_relations": [17307],
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251540910024,
            "transaction_lsn": 251540910232,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 1, 769707, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251540910944,
            "transaction_id": 892262,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 11, 498108, tzinfo=timezone.utc),
        },
        {
            "type": "R",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "namespace": "public",
            "relation_name": "dummy_table",
            "identity_setting": 102,
            "columns": {
                "column_0": {
                    "column_name": "id",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_1": {
                    "column_name": "name",
                    "key": 1,
                    "data_type": 1043,
                    "data_type_modifier": 54,
                },
                "column_2": {
                    "column_name": "value",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
            },
        },
        {
            "type": "I",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "inserted_row": {"column_0": "49", "column_1": "Alice", "column_2": "10"},
        },
        {
            "type": "I",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "inserted_row": {"column_0": "50", "column_1": "Bob", "column_2": "20"},
        },
        {
            "type": "I",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "inserted_row": {"column_0": "51", "column_1": "Charlie", "column_2": "30"},
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251540910944,
            "transaction_lsn": 251540910992,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 11, 498108, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251557577000,
            "transaction_id": 892269,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 38, 974053, tzinfo=timezone.utc),
        },
        {
            "type": "U",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "replica_identity_changed": False,
            "old_column_values": {"column_0": "50", "column_1": "Bob", "column_2": "20"},
            "new_column_values": {"column_0": "50", "column_1": "test", "column_2": "20"},
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251557577000,
            "transaction_lsn": 251557577048,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 38, 974053, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251557577192,
            "transaction_id": 892271,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 43, 940135, tzinfo=timezone.utc),
        },
        {
            "type": "U",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "replica_identity_changed": False,
            "old_column_values": {
                "column_0": "51",
                "column_1": "Charlie",
                "column_2": "30",
            },
            "new_column_values": {"column_0": "51", "column_1": None, "column_2": "30"},
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251557577192,
            "transaction_lsn": 251557577240,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 43, 940135, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251557578360,
            "transaction_id": 892275,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 52, 532336, tzinfo=timezone.utc),
        },
        {
            "type": "R",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "namespace": "public",
            "relation_name": "dummy_table",
            "identity_setting": 102,
            "columns": {
                "column_0": {
                    "column_name": "id",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_1": {
                    "column_name": "name",
                    "key": 1,
                    "data_type": 1043,
                    "data_type_modifier": 54,
                },
                "column_2": {
                    "column_name": "value",
                    "key": 1,
                    "data_type": 23,
                    "data_type_modifier": -1,
                },
                "column_3": {
                    "column_name": "birthdate",
                    "key": 1,
                    "data_type": 1082,
                    "data_type_modifier": -1,
                },
            },
        },
        {
            "type": "I",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "inserted_row": {
                "column_0": "52",
                "column_1": "Hey",
                "column_2": "100",
                "column_3": "2024-05-10",
            },
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251557578360,
            "transaction_lsn": 251557578408,
            "commit_timestamp": datetime(2024, 6, 16, 13, 58, 52, 532336, tzinfo=timezone.utc),
        },
        {
            "type": "B",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "lsn": 251557603824,
            "transaction_id": 892278,
            "commit_timestamp": datetime(2024, 6, 16, 13, 59, 5, 418059, tzinfo=timezone.utc),
        },
        {
            "type": "D",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "relation_id": 17307,
            "replica_identity_changed": False,
            "deleted_column_values": {
                "column_0": "52",
                "column_1": "Hey",
                "column_2": "100",
                "column_3": "2024-05-10",
            },
        },
        {
            "type": "C",
            "send_time": "send_time",
            "data_start": "data_start",
            "wal_end": "wal_end",
            "data_size": "data_size",
            "is_unused": 0,
            "commit_lsn": 251557603824,
            "transaction_lsn": 251557603872,
            "commit_timestamp": datetime(2024, 6, 16, 13, 59, 5, 418059, tzinfo=timezone.utc),
        },
    ]

    table_changes = process_messages(decoded_messages)

    for table, changes in table_changes.items():
        print(table)

        df = pd.DataFrame(changes)

        print(df)
