import psycopg2


def connect():
    conn = psycopg2.connect(database='postgres', user='postgres', host='localhost')
    cursor = conn.cursor()
    cursor.execute('SELECT NOW()')
    records = cursor.fetchall()
    print(records)
    cursor.close()
    conn.close()


def dump():
    print('Dumped.')
