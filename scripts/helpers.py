import os


def execute_sql_from_file(conn, filename):
    # conn: a (psycopg2) connection object
    # filename: name of the file as it appears in sql_snippets
    cur = conn.cursor()

    dirname = os.path.dirname(__file__)
    cur.execute(open(os.path.join(dirname, 'sql_snippets', filename), 'r').read())