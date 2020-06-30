import logging

import pygbif
import time
import datetime
from helpers import execute_sql_from_file, get_database_connection, get_config, setup_log_file
from helpers import execute_sql_from_jinja_string


def _update_match_info(conn, match_info, scientificname_row_id):
    # update scientificname with info about match and taxonomyId
    match_info = {k: v for k, v in match_info.items() if v is not None}
    template = """ UPDATE scientificname SET """ \
               + ", ".join([f'"{i}"' + ' = ' + '{{ ' + str(i) + ' }}' for i in match_info.keys()]) \
               + """ WHERE "id" = {{ id }} """
    data = match_info.copy()
    data['id'] = scientificname_row_id
    execute_sql_from_jinja_string(conn, sql_string=template, context=data)


def _update_taxonomy_if_needed(conn, taxon_in_taxonomy, taxon):
    # GBIF knows about this taxon, and so we are. Do we need to update or do we already have the latest data
    gbifId = taxon['gbifId']

    taxonomyId = taxon_in_taxonomy.get('id')
    taxonomy_fields_to_compare = {k: taxon_in_taxonomy[k] for k in taxon}
    taxonomy_fields_to_change = taxonomy_fields_to_compare.copy()
    if taxon == taxonomy_fields_to_compare:
        print(f"Taxon {taxon['scientificName']} already present in taxonomy (id = {taxonomyId}).")
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

    print(f"Taxon {taxon['scientificName']} currently not present in the taxonomy table.")
    # insert taxon in taxonomy table

    # insert taxon in taxonomy
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
    taxonomyId = taxonomyId[0][0]
    print(f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {taxonomyId}).")
    return taxonomyId


# To remove (or at least improve with dict_cursor) later during refactoring
# def _get_taxonomy_as_dict(conn):
#     taxonomy_cur = execute_sql_from_file(conn, 'get_taxa_taxonomy.sql')
#     taxonomy = taxonomy_cur.fetchall()
#     cols_taxonomy = list(map(lambda x: x[0], taxonomy_cur.description))
#     taxonomy_dict = dict()
#     if taxonomy is not None:
#         for row in taxonomy:
#             # use gbifID as key of taxonomy_dict
#             taxonomy_dict[row[1]] = dict(zip(cols_taxonomy, row))
#
#     return taxonomy_dict


def gbif_match(conn, config_parser, unmatched_only=True):
    # get data from the scientificname table
    if not unmatched_only:
        scientificname_cur = execute_sql_from_file(conn,
                                    'get_names_scientificname.sql',
                                    {'limit': config_parser.get('gbif_match', 'scientificnames-limit')},
                                    dict_cursor=True)
    else:
        # unmatched names only
        scientificname_cur = execute_sql_from_file(conn, 'get_names_scientificname_unmatched_only.sql',
                                    {'limit': config_parser.get('gbif_match', 'scientificnames-limit')},
                                    dict_cursor=True)

    # get taxonomy table and store it as a dictionary
    # taxonomy_dict = _get_taxonomy_as_dict(conn)

    total_sn_count = scientificname_cur.rowcount
    print(f"Number of taxa in scientificname table: {total_sn_count}.")
    log = f"Match names (scientificName + authorship) to GBIF Backbone. "
    print(log)
    logging.info(log)

    start = time.time()
    match_count = 0

    last_matched = datetime.datetime.now()
    print(last_matched)

    # match names to GBIF Backbone
    for row in scientificname_cur:
        row_id = row['id']
        # get name to check
        name = row['scientificName']
        if row['authorship'] is not None:
            name += " " + row['authorship']
        print(f'Try matching {name}.')
        # match name
        gbif_taxon_info = pygbif.name_backbone(name=name, strict=True)

        match_info = {
            'taxonomyId': None,
            'lastMatched': last_matched,
            'matchType': None,
            'matchConfidence': None
        }

        if gbif_taxon_info['matchType'] != 'NONE':
            match_count += 1

            gbifId = gbif_taxon_info.get('usageKey')
            scientificName = gbif_taxon_info.get('scientificName')
            kingdom = gbif_taxon_info.get('kingdom')

            match_info['matchType'] = gbif_taxon_info.get('matchType')
            match_info['matchConfidence'] = gbif_taxon_info.get('confidence')

            taxon = {'gbifId': gbifId, 'scientificName': scientificName, 'kingdom': kingdom}

            if gbifId not in taxonomy_dict:
                # GBIF knows about this taxon, and we don't
                match_info['taxonomyId'] = _insert_new_entry_taxonomy(conn, taxon)
            else:
                # We already know about it, something to update
                match_info['taxonomyId'] = _update_taxonomy_if_needed(conn, taxonomy_dict, taxon)

        else:
            log = f"No match found for {name} (id: {row_id})."
            print(log)
            logging.warning(log)

        print(f"Add match information and taxonomiyId, if present, to scientificname for {name} (id: {row_id}).")
        _update_match_info(conn, match_info, row_id)

    # Logging and statistics
    end = time.time()
    n_matched_taxa_perc = match_count / total_sn_count * 100
    n_matched_taxa = f"Number of matched names: {match_count}/{total_sn_count} ({n_matched_taxa_perc:.2f}%)."
    print(n_matched_taxa)
    logging.info(n_matched_taxa)
    elapsed_time = f"Match to GBIF Backbone performed in {round(end - start)}s."
    print(elapsed_time)
    logging.info(elapsed_time)


if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/match_names_to_gbif_backbone_log.csv")

    gbif_match(conn=connection, config_parser=config, unmatched_only=True)
