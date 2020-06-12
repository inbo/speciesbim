# Main script to transform the BIM database to the new version
# See: https://github.com/inbo/speciesbim/issues/3

# Before running this script, make sure you have a config.ini file in the current directory
# It should contain DB connection information (set up an external tunnel if necessary)
# You can start by copying config.ini.example to config.ini and change its content.

import psycopg2
import configparser
import gbif_match

from helpers import execute_sql_from_file

# TODO: make sure the script outputs an error if the config file is not found
configParser = configparser.RawConfigParser()
configParser.read(r'config.ini')

conn = psycopg2.connect(dbname=configParser.get('database', 'dbname'),
                        user=configParser.get('database', 'user'),
                        password=configParser.get('database', 'password'),
                        host=configParser.get('database', 'host'),
                        port=int(configParser.get('database', 'port')),
                        options=f"-c search_path={configParser.get('database', 'schema')}")

log_filename = "./logs/transform_db_log.csv"
log_file = open(log_filename, 'w')

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
                          {'limit': configParser.get('transform_db', 'scientificnames-limit')})

    message = "Step 4: populate taxonomy table with matches to GBIF Backbone and update scientificname table"
    print(message)
    log_file.write(message + '\n')
    gbif_match.gbif_match(conn, configParser=configParser, log_file = log_file, unmatched_only=False)

log_file.close()
