import logging

import pygbif
import time
import datetime
from helpers import execute_sql_from_file, get_database_connection, get_config, setup_log_file, \
    execute_sql_from_jinja_string, print_indent


def _insert_or_get_rank(conn, rank_name):
    """ Insert or select a rank

    If rank_name already exists in the rank table, select it.
    Otherwise, insert it in a new row.

    In both cases, returns the row id """

    template = """WITH ins AS (
        INSERT INTO rank(name)
        VALUES ({{ rank_name }})         -- input value
        ON CONFLICT(name) DO NOTHING
        RETURNING rank.id
        )
    SELECT id FROM ins
    UNION  ALL
    SELECT id FROM rank          -- 2nd SELECT never executed if INSERT successful
    WHERE name = {{ rank_name }}  -- input value a 2nd time
    LIMIT  1;"""

    cur = execute_sql_from_jinja_string(conn, sql_string=template, context={'rank_name': rank_name}, dict_cursor=True)
    return cur.fetchone()['id']


def _update_match_info(conn, match_info, scientificname_row_id):
    # update scientificname with info about match and taxonomyId
    match_info = {k: v for k, v in match_info.items() if v is not None}
    template = """ UPDATE scientificname SET """ \
               + ", ".join([f'"{i}"' + ' = ' + '{{ ' + str(i) + ' }}' for i in match_info.keys()]) \
               + """ WHERE "id" = {{ id }} """
    data = match_info.copy()
    data['id'] = scientificname_row_id
    execute_sql_from_jinja_string(conn, sql_string=template, context=data)


def _update_taxonomy_if_needed(conn, taxon_in_taxonomy, taxon, depth=0):
    # Params: depth is the recursion level (used for log indentation)

    # GBIF knows about this taxon, and so we are. Do we need to update or do we already have the latest data
    gbifId = taxon['gbifId']

    taxonomyId = taxon_in_taxonomy.get('id')
    taxonomy_fields_to_compare = {k: taxon_in_taxonomy[k] for k in taxon}
    taxonomy_fields_to_change = taxonomy_fields_to_compare.copy()
    if taxon == taxonomy_fields_to_compare:
        print_indent(f"Taxon {taxon['scientificName']} already present in taxonomy (id = {taxonomyId}).", depth)
    else:
        # unchanged fields
        keys_same_values = dict(taxonomy_fields_to_compare.items() & taxon.items()).keys()
        # remove unchanged fields
        for key in keys_same_values: del taxonomy_fields_to_change[key]
        for key in keys_same_values: del taxon[key]
        print(f"Fields - values to change:")
        [print(key, value) for key, value in taxonomy_fields_to_change.items()]
        print(f"New fields - values:")
        [print(key, value) for key, value in taxon.items()]
        context_to_query = taxon
        context_to_query['gbifId'] = gbifId
        template = """ UPDATE taxonomy SET """ \
                   + ", ".join([f'"{i}"' + ' = ' + '{{ ' + str(i) + ' }}' for i in taxon.keys()]) \
                   + """ WHERE "gbifId" = {{ gbifId }} """
        execute_sql_from_jinja_string(conn, sql_string=template, context=context_to_query)
        return taxonomyId


def _insert_new_entry_taxonomy(conn, taxon):
    gbifId = taxon['gbifId']

    # insert taxon in taxonomy table
    execute_sql_from_jinja_string(
        conn,
        """INSERT INTO taxonomy ({{ col_names | surround_by_quote | join(', ') | sqlsafe }}) VALUES {{ values | inclause }}""",
        {'col_names': tuple(taxon.keys()),
         'values': tuple(taxon.values())}
    )
    # get id (PK) in taxonomy
    cur = execute_sql_from_jinja_string(conn,
                                        """SELECT id FROM taxonomy WHERE "gbifId" = {{ gbifId }}""",
                                        {'gbifId': gbifId})
    taxonomyId = cur.fetchall()
    assert taxonomyId is not None, f"Taxon with gbifId {gbifId} not inserted into the taxonomy table."
    assert len(taxonomyId) <= 1, \
        f"Too many taxa returned for gbifId = {gbifId}. Duplicates in taxonomy table."

    _insert_new_entry_taxonomy.counter += 1
    return taxonomyId[0][0]

_insert_new_entry_taxonomy.counter = 0


def _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id):
    # Search the taxonomy table by gbif_id
    # Returns a dict such as: {'id': 1, 'gbifId': 5, 'scientificName': 'Fungi', 'rankId': 1, 'acceptedId': None, 'parentId': None}
    # If nothing is found, returns all None: {'id': None, 'gbifId': None, ...}
    template = """SELECT * FROM taxonomy WHERE "gbifId" = {{ gbifId }} """
    taxon_cur = execute_sql_from_jinja_string(conn, sql_string=template, context={'gbifId': gbif_id})
    taxon_values = taxon_cur.fetchall()
    cols_taxonomy = list(map(lambda x: x[0], taxon_cur.description))

    assert len(taxon_values) <= 1, f"Multiple taxa with gbifId = {gbif_id} in taxonomy."
    if len(taxon_values) == 1:
        taxon = dict(zip(cols_taxonomy, taxon_values[0]))
    else:
        taxon = dict.fromkeys(cols_taxonomy)
    return taxon


def _add_taxon_tree(conn, gbif_key, depth=0):
    # Params: depth is the recursion level (used for log indentation)

    # get info from GBIF Backbone
    name_usage_info = pygbif.name_usage(key=gbif_key)
    gbifId = name_usage_info.get('key')
    assert gbifId == gbif_key, f"Inconsistency in GBIF database. Got {gbif_key} from name_usage({gbifId})."
    scientificName = name_usage_info.get('scientificName')

    gbif_parentKey = name_usage_info.get("parentKey")
    parent_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_parentKey)

    # get accepted GBIF Key synonyms are pointing to (None for accepted taxa)
    gbif_acceptedKey = name_usage_info.get('acceptedKey')
    accepted_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_acceptedKey)
    print_indent(f"Recursively adding the taxon with GBIF key {gbif_key} ({scientificName}) to the taxonomy table", depth=depth)

    taxon = {
        'gbifId': gbifId,
        'scientificName': scientificName,
        'rankId': _insert_or_get_rank(conn=conn, rank_name=name_usage_info.get('rank')),
        'parentId': parent_in_taxonomy.get('id'),
        'acceptedId': accepted_in_taxonomy.get('id')
    }
    # find and add taxon recursively to taxonomy table
    taxon_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_key)

    if taxon_in_taxonomy.get('gbifId') is None:  # Taxon is not yet in our taxonomy table
        if gbif_parentKey is None:
            print_indent("According to GBIF, this is a root taxon (no more parents to insert)", depth=depth)
        else:
            print_indent("According to GBIF, this is *not* a root taxon, we'll insert parents first", depth=depth)
            _add_taxon_tree(conn, gbif_key=gbif_parentKey, depth=depth+1)
            # get the updated parentId
            parent_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_parentKey)
            taxon['parentId'] = parent_in_taxonomy.get('id')
        if gbif_acceptedKey is None:
            print_indent("According to GBIF, this is *not* a synonym (no accepted taxon to insert)", depth=depth)
        else:
            print_indent("According to GBIF, this is a synonym. We'll insert accepted taxon first", depth=depth)
            _add_taxon_tree(conn, gbif_key=gbif_acceptedKey, depth=depth + 1)
            # get the updated acceptedId
            accepted_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_acceptedKey)
            taxon['acceptedId'] = accepted_in_taxonomy.get('id')
        newly_inserted_id = _insert_new_entry_taxonomy(conn, taxon=taxon)
        if (taxon['acceptedId'] is None):
            msg = f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {newly_inserted_id}, parentId = {taxon['parentId']})."
        else:
            msg = f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {newly_inserted_id}, parentId = {taxon['parentId']}, acceptedId = {taxon['acceptedId']})."
        print_indent(msg, depth=depth)
    else:  # The taxon already appears in the taxonomy table
        print_indent("This taxon already appears in the taxonomy table", depth=depth)
        # get the updated parentId
        parent_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_parentKey)
        taxon['parentId'] = parent_in_taxonomy.get('id')
        #  get the updated acceptedId
        accepted_in_taxonomy = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbif_acceptedKey)
        taxon['acceptedId'] = accepted_in_taxonomy.get('id')
        _update_taxonomy_if_needed(conn, taxon_in_taxonomy=taxon_in_taxonomy, taxon=taxon, depth=depth)


def gbif_match(conn, config_parser, unmatched_only=True):
    limit = config_parser.get('gbif_match', 'scientificnames-limit')
    demo = config_parser.getboolean('demo_mode', 'demo')
    # get data from the scientificname table
    scientificname_cur = execute_sql_from_file(conn,
                                    'get_names_scientificname.sql',
                                    {'limit': limit,
                                     'demo': demo,
                                     'unmatched_only': unmatched_only},
                                    dict_cursor=True)
    total_sn_count = scientificname_cur.rowcount
    print(f"Number of taxa in scientificname table: {total_sn_count}.")
    log = f"Match names (scientificName + authorship) to GBIF Backbone. "
    print(log)
    logging.info(log)

    start = time.time()
    match_count = 0

    last_matched = datetime.datetime.now()
    print(f"Timestamp used for this (whole) match process: {last_matched}")

    # match names to GBIF Backbone
    for row in scientificname_cur:
        row_id = row['id']
        # get name to check
        name = row['scientificName']
        if row['authorship'] is not None:
            name += " " + row['authorship']
        print(f'Try matching the "{name}" name...')

        # initialize match information
        match_info = {
            'taxonomyId': None,
            'lastMatched': last_matched,
            'matchType': None,
            'matchConfidence': None
        }

        # match name
        gbif_taxon_info = pygbif.name_backbone(name=name, strict=True)

        match_info['matchType'] = gbif_taxon_info.get('matchType')
        match_info['matchConfidence'] = gbif_taxon_info.get('confidence')

        if gbif_taxon_info['matchType'] != 'NONE':
            match_count += 1

            gbifId = gbif_taxon_info.get('usageKey')
            _add_taxon_tree(conn, gbifId)
            taxon = _get_taxon_from_taxonomy_by_gbifId(conn, gbif_id=gbifId)
            match_info['taxonomyId'] = taxon['id']

        else:
            log = f"No match found for {name} (id: {row_id})."
            print(log)
            logging.warning(log)

        print(f"Add match information (and taxonomiyId, if a match was found) to scientificname for {name} (id: {row_id}).")
        _update_match_info(conn, match_info, row_id)
        if (row_id % 10 == 9) and (row_id < total_sn_count - 1): # Get time info after multiple of 10 taxa
            elapsed_time = time.time() - start
            # notice expected time as calculated below is highly overestimated at the beginning as all trees up to
            # kingdoms have to be built at the beginning
            expected_time = elapsed_time / (row_id + 1) * (total_sn_count - row_id - 1)
            print(f"{row_id + 1}/{total_sn_count} taxa handled in {round(elapsed_time, 2)}s. Expected time to go: {expected_time}s.")


    # Logging and statistics
    end = time.time()
    n_matched_taxa_perc = match_count / total_sn_count * 100
    n_matched_taxa = f"Number of matched names: {match_count}/{total_sn_count} ({n_matched_taxa_perc:.2f}%)."
    print(n_matched_taxa)
    logging.info(n_matched_taxa)
    print(f"Total number of insertions in the taxonomy table: {_insert_new_entry_taxonomy.counter}")
    elapsed_time = f"Match to GBIF Backbone performed in {round(end - start)}s."
    print(elapsed_time)
    logging.info(elapsed_time)


if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/match_names_to_gbif_backbone.log")

    gbif_match(conn=connection, config_parser=config, unmatched_only=False)
