# Main script to transform the BIM database to the new version
# See: https://github.com/inbo/speciesbim/issues/3

# Before running this script, make sure you have a config.ini file in the current directory
# It should contain DB connection information (set up an external tunnel if necessary)
# You can start by copying config.ini.example to config.ini and change its content.
import os

import psycopg2
import configparser
import gbif_match

from helpers import execute_sql_from_file

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

CONFIG_FILE_PATH = './config.ini'
LOG_FILE_PATH = "./logs/transform_db_log.csv"

# TODO: make sure the script outputs an error if the config file is not found
config_parser = configparser.RawConfigParser()

try:
    with open(os.path.join(__location__, CONFIG_FILE_PATH)) as f:
        config_parser.read_file(f)
except IOError:
    raise Exception(f"Configuration file ({CONFIG_FILE_PATH}) not found")

conn = psycopg2.connect(dbname=config_parser.get('database', 'dbname'),
                        user=config_parser.get('database', 'user'),
                        password=config_parser.get('database', 'password'),
                        host=config_parser.get('database', 'host'),
                        port=int(config_parser.get('database', 'port')),
                        options=f"-c search_path={config_parser.get('database', 'schema')}")

with open(os.path.join(__location__, LOG_FILE_PATH), 'w') as log_file:
    with conn:
        message = "Step 1: Drop our new tables if they already exists (idempotent script)"
        print(message)
        log_file.write(message + '\n')
        execute_sql_from_file(conn, 'drop_new_tables_if_exists.sql')

        message = "Step 2: create the new tables"
        print(message)
        log_file.write(message + '\n')
        execute_sql_from_file(conn, 'create_new_tables.sql')

        message = "Step 3: populate the scientifcname tables based on the actual content"
        print(message)
        log_file.write(message + '\n')
        execute_sql_from_file(conn, 'populate_scientificname.sql',
                              {'limit': config_parser.get('transform_db', 'scientificnames-limit')})

        message = "Step 4: populate taxonomy table with matches to GBIF Backbone and update scientificname table"
        print(message)
        log_file.write(message + '\n')
        gbif_match.gbif_match(conn, config_parser=config_parser, log_file=log_file, unmatched_only=False)
