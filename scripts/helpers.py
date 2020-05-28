import os

from jinja2 import Template


def execute_sql_from_file(conn, filename, context=None):
    # conn: a (psycopg2) connection object
    # filename: name of the template (Jinja) file as it appears in sql_snippets
    # context: the context (dict-like) that will be passed to Jinja
    if context is None:
        context = {}

    cur = conn.cursor()

    dirname = os.path.dirname(__file__)

    sql_template = Template(open(os.path.join(dirname, 'sql_snippets', filename), 'r').read())
    cur.execute(sql_template.render(context))