import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier


def get_str_tuple_values(row):
    values = row.values()
    str_values = map(str, values)
    return tuple(str_values)


# Fill data

rows = [
    (150, "cool story"),
    (10, "sad story")
]


insert_data = "INSERT INTO druids (xp, story) VALUES (%s, %s)"
create_druids_table = "CREATE TABLE druids (id serial PRIMARY KEY, xp integer, story varchar)"


conn = psycopg2.connect(database="postgres", user="postgres", host="localhost")
cursor = conn.cursor(cursor_factory=RealDictCursor)

cursor.execute(create_druids_table)
for row in rows:
    cursor.execute(insert_data, row)
conn.commit()


# Dump

get_all_tables = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
"""

cursor.execute(get_all_tables)
res = cursor.fetchall()
all_tables = [*map(lambda t: t['table_name'], res)]
print(all_tables)


dump = {}
for table_name in all_tables:
    cursor.execute(SQL("SELECT * from {}").format(Identifier(table_name)))
    table_data = cursor.fetchall()
    dump[table_name] = table_data
    print(cursor.description)
    colnames = [desc[0] for desc in cursor.description]

    with open(f"{table_name}.csv", "w") as dump_file:
        cols = ','.join(colnames)
        dump_file.write(f'{cols}\n')

        tuple_values = [*map(get_str_tuple_values, dump['druids'])]
        csv_values = map(lambda tup: ','.join(tup), tuple_values)
        data_to_write = '\n'.join(csv_values)
        dump_file.write(data_to_write)

cursor.close()
conn.close()
