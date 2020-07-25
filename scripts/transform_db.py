# Main script to transform the BIM database to the new version
# See: https://github.com/inbo/speciesbim/issues/3

# Before running this script, make sure you have a config.ini file in the current directory
# It should contain DB connection information (set up an external tunnel if necessary)
# You can start by copying config.ini.example to config.ini and change its content.
import os
import gbif_match
import vernacular_names
import exotic_status
import logging

from helpers import execute_sql_from_file, get_database_connection, get_config, setup_log_file

LOG_FILE_PATH = "./logs/transform_db_log.csv"

setup_log_file(LOG_FILE_PATH)
conn = get_database_connection()
config = get_config()

with conn:
    message = "Step 1: Drop our new tables if they already exists (idempotent script)"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'drop_new_tables_if_exists.sql')

    message = "Step 2: create the new tables"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'create_new_tables.sql')

    message = "Step 3: populate the scientifcname tables based on the actual content"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'populate_scientificname.sql',
                          {'limit': config.get('transform_db', 'scientificnames-limit')})

    message = "Step 4: populate taxonomy table with matches to GBIF Backbone and related backbone tree " +\
              "and update scientificname table"
    print(message)
    logging.info(message)
    gbif_match.gbif_match(conn, config_parser=config, unmatched_only=False)

    message = "Step 5: populate vernacular names from GBIF for each entry in the taxonomy table"
    print(message)
    logging.info(message)
    vernacular_names.populate_vernacular_names(conn, config_parser=config, empty_only=False)

    message = "Step 6: populate field exotic_be (values: True of False) from GRIIS checklist for each entry in taxonomy table."
    print(message)
    logging.info(message)
    # datasetKey of Global Register of Introduced and Invasive Species - Belgium
    griis_be = "6d9e952f-948c-4483-9807-575348147c7e"
    exotic_status.populate_is_exotic_be_field(conn, config_parser=config, exotic_status_source = griis_be)
