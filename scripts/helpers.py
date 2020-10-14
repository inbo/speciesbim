import configparser
import logging
import os

import psycopg2
import psycopg2.extras
from jinja2 import Environment
from jinjasql import JinjaSql
from pygbif import species

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

CONFIG_FILE_PATH = './config.ini'


def setup_log_file(relative_path):
    logging.basicConfig(filename=os.path.join(__location__, relative_path),
                        level=logging.INFO,
                        filemode='w',
                        format='%(asctime)s | %(message)s')

def get_config():
    """ Read config.ini (in the same directory than this script) and returns a configparser """
    config_parser = configparser.RawConfigParser()

    try:
        with open(os.path.join(__location__, CONFIG_FILE_PATH)) as f:
            config_parser.read_file(f)
    except IOError:
        raise Exception(f"Configuration file ({CONFIG_FILE_PATH}) not found")

    return config_parser


def get_database_connection():
    """ Read config.ini (in the same directory than this script) and returns a (psycopg2) connection object"""
    config_parser = get_config()

    conn =  psycopg2.connect(dbname=config_parser.get('database', 'dbname'),
                            user=config_parser.get('database', 'user'),
                            password=config_parser.get('database', 'password'),
                            host=config_parser.get('database', 'host'),
                            port=int(config_parser.get('database', 'port')),
                            options=f"-c search_path={config_parser.get('database', 'schema')}")

    conn.autocommit = True
    return conn


def surround_by_quote(a_list):
    return ['"%s"' % an_element for an_element in a_list]


def execute_sql_from_jinja_string(conn, sql_string, context=None, dict_cursor=False):
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

    if dict_cursor:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    else:
        cur = conn.cursor()

    cur.execute(query, bind_params)

    return cur


def execute_sql_from_file(conn, filename, context=None, dict_cursor=False):
    # conn: a (psycopg2) connection object
    # filename: name of the template (Jinja) file as it appears in sql_snippets
    # context: the context (dict-like) that will be passed to Jinja
    #
    # returns the cursor object
    dirname = os.path.dirname(__file__)
    return execute_sql_from_jinja_string(conn=conn,
                                         sql_string=open(os.path.join(dirname, 'sql_snippets', filename), 'r').read(),
                                         context=context,
                                         dict_cursor=dict_cursor)

def paginated_name_usage(*args, **kwargs):
    """Small helper to handle the pagination in pygbif and make sure we get all results in one shot"""
    PER_PAGE = 100

    results = []
    offset = 0

    while True:
        resp = species.name_usage(*args, **kwargs, limit=PER_PAGE, offset=offset)
        results = results + resp['results']
        if resp['endOfRecords']:
            break
        else:
            offset = offset + PER_PAGE

    return results

def print_indent(msg, depth=0, indent=4):
    print("{}{}".format(" " * (indent * depth), msg))

def insert_or_get_scientificnameid(conn, scientific_name, authorship):
    """ Insert or select a name in scientificname table based on its scientific name and authorship

        If the scientific name - authorship combination already exists in the scientificname table, select it.
        Otherwise, insert it in a new row.

        In both cases, returns the row id """
    # insert in scientificname
    sc_name_template = """WITH ins AS (
                                INSERT INTO scientificname ("scientificName", "authorship")
                                VALUES ({{ scientific_name }}, {{ authorship}})         -- input value
                                ON CONFLICT DO NOTHING
                                RETURNING scientificname.id
                                )
                            SELECT id FROM ins
                            UNION  ALL
                            SELECT "id" FROM scientificname          -- 2nd SELECT never executed if INSERT successful
                            {% if authorship is defined %}
                                WHERE "scientificName" = {{ scientific_name }} AND "authorship" = {{ authorship }} -- input value a 2nd time
                            {% else %}
                                WHERE "scientificName" = {{ scientific_name }} AND "authorship" is NULL -- input value a 2nd time
                            {% endif %}
                            LIMIT  1;"""
    cur = execute_sql_from_jinja_string(conn,
                                        sql_string=sc_name_template,
                                        context={'scientific_name': scientific_name,
                                                 'authorship': authorship},
                                        dict_cursor=True)
    return cur.fetchone()['id']

