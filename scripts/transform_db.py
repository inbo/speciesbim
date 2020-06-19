# Main script to transform the BIM database to the new version
# See: https://github.com/inbo/speciesbim/issues/3

# Before running this script, make sure you have a config.ini file in the current directory
# It should contain DB connection information (set up an external tunnel if necessary)
# You can start by copying config.ini.example to config.ini and change its content.
import os
import gbif_match

from helpers import execute_sql_from_file, get_database_connection, get_config

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
LOG_FILE_PATH = "./logs/transform_db_log.csv"


with open(os.path.join(__location__, LOG_FILE_PATH), 'w') as log_file:
    conn = get_database_connection()
    config = get_config()
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
                              {'limit': config.get('transform_db', 'scientificnames-limit')})

        message = "Step 4: populate taxonomy table with matches to GBIF Backbone and update scientificname table"
        print(message)
        log_file.write(message + '\n')
        gbif_match.gbif_match(conn, config_parser=config, log_file=log_file, unmatched_only=False)
