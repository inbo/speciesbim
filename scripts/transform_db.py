# Main script to transform the BIM database to the new version
# See: https://github.com/inbo/speciesbim/issues/3

# Before running this script, make sure you have a config.ini file in the current directory
# You can start by copying config.ini.example to config.ini and change its content.
import os
import logging

import deduplicate_taxon
import match_annexscientificname_to_scientificname
import gbif_match
import vernacular_names
import exotic_status
import populate_annex_scientificname


from helpers import execute_sql_from_file, get_database_connection, get_config, setup_log_file

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

LOG_FILE_PATH = "./logs/transform_db.log"
ANNEX_FILE_PATH = os.path.join(__location__, "../data/raw/official_annexes.csv")
ANNEX_FILE_PATH_DEMO = os.path.join(__location__, "../data/raw/official_annexes_demo.csv")

# GBIF datasetKey of checklist: Global Register of Introduced and Invasive Species - Belgium
GRIIS_DATASET_UUID = "6d9e952f-948c-4483-9807-575348147c7e"

setup_log_file(LOG_FILE_PATH)
conn = get_database_connection()
config = get_config()
# activate/deactivate demo mode
demo = config.getboolean('demo_mode', 'demo')

with conn:
    message = "Prepare: Solve duplicates in taxon table"
    print(message)
    logging.info(message)
    deduplicate_taxon.deduplicate_taxon(conn, config_parser=config)

    message = "Step 0: Drop our new tables if they already exists (idempotent script)"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'drop_new_tables_if_exists.sql')

    message = "Step 1: create the new tables"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'create_new_tables.sql')

    message = "Step 2: populate the scientificname table based on the actual content"
    print(message)
    logging.info(message)
    execute_sql_from_file(conn, 'populate_scientificname.sql',
                          {'limit': config.get('transform_db', 'scientificnames-limit')})

    message = "Step 3: populate annexscientificname table based on official annexes"
    print(message)
    logging.info(message)
    if not demo:
        populate_annex_scientificname.populate_annex_scientificname(conn, config_parser=config,
                                                                    annex_file=ANNEX_FILE_PATH)
    else:
        populate_annex_scientificname.populate_annex_scientificname(conn, config_parser=config,
                                                                    annex_file=ANNEX_FILE_PATH_DEMO)

    message = "Step 4: populate taxonomy table with matches to GBIF Backbone and related backbone tree " +\
              "and update scientificname table"
    print(message)
    logging.info(message)
    gbif_match.gbif_match(conn, config_parser=config, unmatched_only=False)

    message = "Step 5: populate vernacular names from GBIF for each entry in the taxonomy table"
    print(message)
    logging.info(message)
    # list of 2-letters language codes (ISO 639-1)
    languages = ['fr', 'nl', 'en']
    vernacular_names.populate_vernacular_names(conn, config_parser=config, empty_only=False, filter_lang=languages)

    message = "Step 6: populate field exotic_be (values: True of False) from GRIIS checklist for each entry in " \
              "taxonomy table "
    print(message)
    logging.info(message)
    exotic_status.populate_is_exotic_be_field(conn, config_parser=config, exotic_status_source=GRIIS_DATASET_UUID)
