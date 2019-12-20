import os
from datetime import datetime
from psycopg2.sql import SQL, Identifier, Literal


tables = {
    "druid": {
        "description": {
            "id": "serial PRIMARY KEY",
            "xp": "integer",
            "story": "varchar",
        },
        "data": [(150, "cool story"), (10, "sad story")],
    },
    "archer": {
        "description": {
            "id": "serial PRIMARY KEY",
            "xp": "integer",
            "story": "varchar",
            "arch": "varchar",
        },
        "data": [(55, "nice fellow", "oak"), (2, "big brother", "fir")],
    },
}


def get_sql_table_description(column):
    name, data_type = column
    return f"{name} {data_type}"


def setup_db(conn):
    for table_name, fixtures in tables.items():
        columns = fixtures["description"]
        description = ", ".join(map(get_sql_table_description, columns.items()))

        create_table = "CREATE TABLE {} ({})".format(table_name, description)
        conn.execute(create_table)

        columns_except_pk = dict(columns)
        columns_except_pk.pop('id')

        data = fixtures["data"]
        for row in data:
            columns_names = SQL(", ").join(map(Identifier, columns_except_pk.keys()))
            columns_values = SQL(", ").join(map(Literal, row))

            insert_row = SQL("INSERT INTO {} ({}) VALUES ({})").format(
                Identifier(table_name), columns_names, columns_values
            )

            conn.execute(insert_row)


get_all_table_names = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
"""

get_db_meta = "SELECT current_database();"


def convert_values_to_string_and_store_in_tuple(row):
    values = row.values()
    return tuple(map(str, values))


def get_existing_tables(conn):
    conn.execute(get_all_table_names)
    tables_meta = conn.fetchall()
    return map(lambda t: t["table_name"], tables_meta)


def get_current_db_name(conn):
    conn.execute(get_db_meta)
    (db_meta,) = conn.fetchall()
    return db_meta["current_database"]


def get_table_data(conn, table_name):
    query = SQL("SELECT * from {}").format(Identifier(table_name))
    conn.execute(query)
    return conn.fetchall()


def prepare_csv_dump_data(table_data):
    rows = map(convert_values_to_string_and_store_in_tuple, table_data)
    csv_rows = map(lambda row: ",".join(row), rows)
    return "\n".join(csv_rows)


def dump_db(conn):
    table_names = get_existing_tables(conn)
    db_name = get_current_db_name(conn)

    current_datetime = datetime.now().isoformat(timespec="seconds")
    dump_dirname = f"dump_{db_name}_{current_datetime}"
    os.mkdir(dump_dirname)

    for table_name in table_names:
        table_data = get_table_data(conn, table_name)

        dump_file_name = f"{dump_dirname}/{table_name}.csv"
        with open(dump_file_name, "w") as file:
            data = prepare_csv_dump_data(table_data)
            file.write(data)
