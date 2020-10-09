# This script deduplicate existing entries in the existing taxon table by:
#
# - Load its configuration ("row X should be replaced by row Y") from an external file
# - Based on that configuration, updates the FK that point to those records in multiple tables => each relationship to X is replaced by a FK to Y
# - Finally, deletes the X entries in taxon

# This gives us cleaner data to import from, and avoid errors later down the road (since our new "scientificname"
# table rejects duplicate, thus breaking the script)
#
# This script updates the existing data, so while it can be run multiple times, it will only have effects the first time.
#
# After succesful execution, the following query should return no results (no more duplicates on scientificanme + authorship in the taxon table):
# WITH cte_duplicates AS (
#     SELECT acceptedname, scientificnameauthorship, COUNT(*) FROM biodiv.taxon
#     GROUP BY acceptedname, scientificnameauthorship
#     HAVING COUNT(*) > 1)
#
# SELECT t.* FROM
# biodiv.taxon t, cte_duplicates
# WHERE t.acceptedname = cte_duplicates.acceptedname AND
#       t.scientificnameauthorship = cte_duplicates.scientificnameauthorship ORDER by t.acceptedname
#
# key: old taxon_id (to be deleted)
# value: new taxon_id (to replace the other one)
import os

from helpers import get_database_connection, execute_sql_from_jinja_string, get_config

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def deduplicate_taxon(conn, config_parser):
    import json

    with open(os.path.join(__location__, config_parser.get('deduplicate_taxon', 'config-filename')), 'r') as fp:
        taxon_to_replace = json.load(fp)

        with conn:
            for old_id, new_id in taxon_to_replace.items():
                print(f"Will replace taxon {old_id} by {new_id}")
                q = "UPDATE biodiv.commontaxa SET nptaxonid = {{ new_id }} WHERE nptaxonid = {{ old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

                q = "UPDATE biodiv.media SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

                q = "UPDATE biodiv.identifiablespecies SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

                q = "UPDATE biodiv.occurence SET identifiablespeciesid = {{ new_id }} WHERE identifiablespeciesid = {{ old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

                q = "UPDATE biodiv.speciesannex SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

                q = "DELETE FROM biodiv.taxon WHERE id = {{old_id }};"
                execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})
            print('DONE')


if __name__ == "__main__":
    connection = get_database_connection()
    conf = get_config()
    deduplicate_taxon(connection, config_parser=conf)
