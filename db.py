import os
from datetime import datetime
from psycopg2.sql import SQL, Identifier, Literal

# TODO:
# 1. Форматирование запросов
# 2. Исключения


tables = {
    'druid': {
        'description': {'id': 'serial PRIMARY KEY', 'xp': 'integer', 'story': 'varchar'},
        'data': [(150, "cool story"), (10, "sad story")]
    },
    'archer': {
        'description': {'id': 'serial PRIMARY KEY', 'xp': 'integer', 'story': 'varchar', 'arch': 'varchar'},
        'data': [(55, "nice fellow", "oak"), (2, "big brother", "fir")]
    },
}


def get_str_tuple_values(row):
    values = row.values()
    str_values = map(str, values)
    return tuple(str_values)


def get_sql_table_description(column):
    name, data_type = column
    return f'{name} {data_type}'


def get_values_to_insert(value):
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def setup_db(conn):
    for table_name, fixtures in tables.items():
        columns = fixtures['description']
        description = ', '.join(map(get_sql_table_description, columns.items()))

        create_table = "CREATE TABLE {} ({})".format(table_name, description)
        conn.execute(create_table)

        data = fixtures['data']
        for row in data:
            columns_except_pk = list(columns.keys())[1:]
            columns_names = SQL(', ').join(map(Identifier, columns_except_pk))
            columns_values = SQL(', ').join(map(Literal, row))

            insert_row = (
                SQL("INSERT INTO {} ({}) VALUES ({})")
                .format(
                    Identifier(table_name),
                    columns_names,
                    columns_values
                )
            )

            conn.execute(insert_row)


get_all_table_names = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
"""

get_db_meta = "SELECT current_database();"


def dump_db(conn):
    conn.execute(get_all_table_names)
    tables_meta = conn.fetchall()
    table_names = [*map(lambda t: t['table_name'], tables_meta)]

    conn.execute(get_db_meta)
    db_meta, = conn.fetchall()
    db_name = db_meta['current_database']
    current_datetime = datetime.now().isoformat(timespec='seconds')

    dump_dirname = f'{db_name}_dump_{current_datetime}'
    try:
        os.mkdir(dump_dirname)
    except Exception:
        print('Oops.')

    for table_name in table_names:
        get_table_data = SQL("SELECT * from {}").format(Identifier(table_name))
        conn.execute(get_table_data)
        table_data = conn.fetchall()

        with open(f"{dump_dirname}/{table_name}.csv", "w") as dump_file:
            rows = [*map(get_str_tuple_values, table_data)]
            csv_rows = map(lambda tup: ','.join(tup), rows)
            data_to_write = '\n'.join(csv_rows)
            dump_file.write(data_to_write)
