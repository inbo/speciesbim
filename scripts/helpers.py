import os

from jinjasql import JinjaSql

def execute_sql_from_jinja_string(conn, sql_string, context=None):
    # conn: a (psycopg2) connection object
    # sql_string: query template (Jinja-supported string)
    # context: the context (dict-like) that will be use with the template
    #
    # returns the cursor object
    #
    # examples:
    #
    # execute_sql_from_jinja_string(conn, "SELECT version();")
    # execute_sql_from_jinja_string(conn, "SELECT * FROM biodiv.address LIMIT {{limit}}", {'limit': 5})
    j = JinjaSql()

    if context is None:
        context = {}

    query, bind_params = j.prepare_query(sql_string, context)

    cur = conn.cursor()
    cur.execute(query, bind_params)

    return cur


def execute_sql_from_file(conn, filename, context=None):
    # conn: a (psycopg2) connection object
    # filename: name of the template (Jinja) file as it appears in sql_snippets
    # context: the context (dict-like) that will be passed to Jinja
    #
    # returns the cursor object
    dirname = os.path.dirname(__file__)
    return execute_sql_from_jinja_string(conn=conn,
                                         sql_string=open(os.path.join(dirname, 'sql_snippets', filename), 'r').read(),
                                         context=context)
