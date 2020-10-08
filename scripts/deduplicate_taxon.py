# This script deduplicate existing entries in the existing taxon table by:
#
# - Load its configuration ("row X should be replaced by row Y") from an external file
# - Based on that configuration, updates the FK that point to those records in multiple tables (commontaxa, media, identifiablespecies, occurrence) => each relationship to X is replaced by a FK to Y
# - Finally, deletes the X entries in taxon

# This gives us cleaner data to import from, and avoid errors later down the road (since our new "scientificname"
# table rejects duplicate, thus breaking the script)
#
# This script updates the existing data, so while it can be run multiple times, it will only have effects the first time.
#
# After succesful execution, the following query should return no results (no more duplicates on scientificanme + authorship in the taxon table):
# WITH cte_duplicates AS (
#     SELECT acceptedname, scientificnameauthorship, COUNT(*) FROM biodiv.taxon
#     --WHERE
#     --id IN (SELECT nptaxonid FROM biodiv.commontaxa) OR
#     --id IN (SELECT taxonid FROM biodiv.media) OR
#     --id IN (SELECT taxonid FROM biodiv.identifiablespecies WHERE
#     --id IN (SELECT identifiablespeciesid FROM biodiv.occurence))
#     GROUP BY acceptedname, scientificnameauthorship
#     HAVING COUNT(*) > 1)
#
# SELECT t.* FROM
# biodiv.taxon t, cte_duplicates
# WHERE t.acceptedname = cte_duplicates.acceptedname AND
#       t.scientificnameauthorship = cte_duplicates.scientificnameauthorship ORDER by t.acceptedname
#
# replacement data (taxon IDs):
# 51923 -> 20618
# 51280 -> 16405
# 51274 -> 51268
# 51819 -> 51486
# 51681 -> 3521

# key: old taxon_id (to be deleted)
# value: new taxon_id (to replace the other one)
from helpers import get_database_connection, execute_sql_from_jinja_string

TAXON_TO_REPLACE = {
    51923: 20618,
    51280: 16405,
    51274: 51268,
    51819: 51486,
    51681: 3521,
    51689: 3510,
    51699: 3512,
    51683: 50153,
    51682: 3513,
    51697: 18127,
    50185: 50184,
    50188: 20672,
    51602: 2524,
    51039: 34306,
    51502: 51429,
    50252: 21247,
    51616: 20801,
    50980: 3484,
    51269: 51263,
    51684: 3435,
    51685: 3436,
    51808: 51799,
    51325: 50385,
    51920: 51459,
    51553: 51334,
    51651: 20922,
    51624: 20822,
    51854: 50577,
    51485: 51832,
    51813: 20727,
    50737: 22507,
    50771: 24425,
    51259: 41015,
    51794: 51520,
    51158: 51111,
    51795: 21035,
    51831: 51828,
    32519: 32518,
    32523: 32522,
    51796: 20856,
    51440: 21519,
    51469: 5762,
    51850: 20518,
    51285: 51284,
    51789: 21531,
    51907: 3358,

}


def deduplicate_taxon(conn):
    with conn:
        for old_id, new_id in TAXON_TO_REPLACE.items():
            print(f"Will replace taxon {old_id} by {new_id}")
            q = "UPDATE biodiv.commontaxa SET nptaxonid = {{ new_id }} WHERE nptaxonid = {{ old_id }};"
            execute_sql_from_jinja_string(conn, q, context= {'new_id': new_id, 'old_id': old_id})

            q = "UPDATE biodiv.media SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
            execute_sql_from_jinja_string(conn, q, context= {'new_id': new_id, 'old_id': old_id})

            q = "UPDATE biodiv.identifiablespecies SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
            execute_sql_from_jinja_string(conn, q, context= {'new_id': new_id, 'old_id': old_id})

            q = "UPDATE biodiv.occurence SET identifiablespeciesid = {{ new_id }} WHERE identifiablespeciesid = {{ old_id }};"
            execute_sql_from_jinja_string(conn, q, context= {'new_id': new_id, 'old_id': old_id})

            # TODO: inform Damiano that we also need to update speciesannex, is it an issue?
            q = "UPDATE biodiv.speciesannex SET taxonid = {{ new_id }} WHERE taxonid = {{ old_id }};"
            execute_sql_from_jinja_string(conn, q, context={'new_id': new_id, 'old_id': old_id})

            q = "DELETE FROM biodiv.taxon WHERE id = {{old_id }};"
            execute_sql_from_jinja_string(conn, q, context= {'new_id': new_id, 'old_id': old_id})
        print ('DONE')


if __name__ == "__main__":
    connection = get_database_connection()
    deduplicate_taxon(connection)
