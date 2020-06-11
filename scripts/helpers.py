import os

from jinja2 import Environment
from jinjasql import JinjaSql


def surround_by_quote(a_list):
    return ['"%s"' % an_element for an_element in a_list]

def execute_sql_from_jinja_string(conn, sql_string, context=None):
    # conn: a (psycopg2) connection object
    # sql_string: query template (Jinja-supported string)
    # context: the context (dict-like) that will be use with the template
    #
    # an extra Jinja filter (surround_by_quote) is available and can be useful to double-quote field names
    #
    # returns the cursor object
    #
    # examples:
    #
    # execute_sql_from_jinja_string(conn, "SELECT version();")
    # execute_sql_from_jinja_string(conn, "SELECT * FROM biodiv.address LIMIT {{limit}}", {'limit': 5})
    e = Environment()
    e.filters["surround_by_quote"] = surround_by_quote
    j = JinjaSql(env=e)

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
