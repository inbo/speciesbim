import logging

import pygbif
import time
import datetime
from helpers import execute_sql_from_file, get_database_connection, get_config, setup_log_file
from helpers import execute_sql_from_jinja_string


def _update_match_info(conn, match_info, taxonomyId, name):
    # update scientificname with info about match and taxonomyId
    try:
        print(f"Add match information and taxonomiyId, if present, to scientificname for {name} (id: {id}).")
        match_info['taxonomyId'] = taxonomyId
        match_info = {k: v for k, v in match_info.items() if v is not None}
        template = """ UPDATE scientificname SET """ \
                   + ", ".join([f'"{i}"' + ' = ' + '{{ ' + str(i) + ' }}' for i in match_info.keys()]) \
                   + """ WHERE "id" = {{ id }} """
        data = match_info.copy()
        data['id'] = id
        execute_sql_from_jinja_string(conn, sql_string=template, context=data)
    except Exception as e:
        # TODO: Show Damiano: I actually had an error there
        print(e)  # TODO: clarify: what kind of exception should we be waiting for?


def _update_taxonomy_if_needed(conn, taxonomy_dict, gbifId, taxon):
    # GBIF knows about this taxon, and so we are. Do we need to update or do we already ahve the latest data
    taxonomyId = taxonomy_dict[gbifId]['id']
    taxonomy_dict_to_compare = {k: taxonomy_dict[gbifId][k] for k in taxon}
    taxonomy_dict_to_change = taxonomy_dict_to_compare.copy()
    if taxon == taxonomy_dict_to_compare:
        print(f"Taxon {taxon['scientificName']} already present in taxonomy (id = {taxonomyId}).")
    else:
        # unchanged fields
        keys_same_values = dict(taxonomy_dict_to_compare.items() & taxon.items()).keys()
        # remove unchanged fields
        for key in keys_same_values: del taxonomy_dict_to_change[key]
        for key in keys_same_values: del taxon[key]
        print(f"Fields - values to change:")
        [print(key, value) for key, value in taxonomy_dict_to_change.items()]
        print(f"New fields - values:")
        [print(key, value) for key, value in taxon.items()]
        try:
            template = """ UPDATE taxonomy SET """ \
                       + ", ".join([f'"{i}"' + ' = ' + '{{ ' + str(i) + ' }}' for i in taxon.keys()]) \
                       + """ WHERE "gbifId" = {{ gbifId }} """
            execute_sql_from_jinja_string(conn, sql_string=template, context=taxon)
            return taxonomyId
        except Exception as e:
            print(e)  # TODO: clarify: what kind of exception should we be waiting for?


def _insert_new_entry_taxonomy(conn, taxon, gbifId): # TODO: is gbifID necessary here? I think it also appears in taxon, no?
    print(f"Taxon {taxon['scientificName']} currently not present in the taxonomy table.")
    # insert taxon in taxonomy table
    try:
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
        taxonomyId = cur.fetchall()  # TODO: show Damiano: variable reuse, for different purposes...
        assert taxonomyId is not None, f"Taxon with gbifId {gbifId} not inserted into the taxonomy table."
        assert len(taxonomyId) <= 1, \
            f"Too many taxa returned for gbifId = {gbifId}. Duplicates in taxonomy table."
        taxonomyId = taxonomyId[0][0]
        print(f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {taxonomyId}).")
        return taxonomyId
    except Exception as e:
        print(e)  # TODO: clarify: what kind of exception should we be waiting for?


# To remove (or at least improve with dict_cursor) later during refactoring
def _get_taxonomy_as_dict(conn):
    taxonomy_cur = execute_sql_from_file(conn, 'get_taxa_taxonomy.sql')
    taxonomy = taxonomy_cur.fetchall()
    cols_taxonomy = list(map(lambda x: x[0], taxonomy_cur.description))
    taxonomy_dict = dict()
    if taxonomy is not None:
        for row in taxonomy:
            # use gbifID as key of taxonomy_dict
            taxonomy_dict[row[1]] = dict(zip(cols_taxonomy, row))

    return taxonomy_dict


def gbif_match(conn, config_parser, unmatched_only=True):
    # get data from the scientificname table
    if not unmatched_only:
        scientificname_cur = execute_sql_from_file(conn,
                                    'get_names_scientificname.sql',
                                    {'limit': config_parser.get('gbif_match', 'scientificnames-limit')},
                                    dict_cursor=True)  # TODO: show new option to Damiano
    else:
        # unmatched names only
        scientificname_cur = execute_sql_from_file(conn, 'get_names_scientificname_unmatched_only.sql',
                                    {'limit': config_parser.get('gbif_match', 'scientificnames-limit')},
                                    dict_cursor=True)

    # get taxonomy table and store it as a dictionary
    taxonomy_dict = _get_taxonomy_as_dict(conn)

    n_taxa = scientificname_cur.rowcount
    print(f"Number of taxa in scientificname table: {n_taxa}.")
    log = f"Match names (scientificName + authorship) to GBIF Backbone. "
    print(log)
    logging.info(log)

    start = time.time()
    i = 0  # TODO: better name for this?

    last_matched = datetime.datetime.now()
    print(last_matched)

    # match names to GBIF Backbone
    for row in scientificname_cur:
        id = row['id']
        # get name to check
        name = row['scientificName']
        if row['authorship'] is not None:
            name += " " + row['authorship']
        print(f'Try matching {name}.')
        # match name
        gbif_taxon_info = pygbif.name_backbone(name=name, strict=True)

        # initialize fields
        gbifId = None
        scientificName = None
        kingdom = None
        taxonomyId = None
        matchType = None
        matchConfidence = None
        try:
            gbifId = gbif_taxon_info['usageKey']
            scientificName = gbif_taxon_info['scientificName']
            kingdom = gbif_taxon_info['kingdom']
            matchType = gbif_taxon_info['matchType']
            matchConfidence = gbif_taxon_info['confidence']
            i += 1
        except KeyError:  # TODO: is it the best way to test the match is successful?
            log = f"No match found for {name} (id: {id})."
            print(log)
            logging.warning(log)

        taxon = {'gbifId': gbifId, 'scientificName': scientificName, 'kingdom': kingdom}

        if gbifId is not None and gbifId not in taxonomy_dict:
            # GBIF knows about this taxon, and we don't
            taxonomyId = _insert_new_entry_taxonomy(conn, taxon, gbifId)
        elif gbifId is not None:
            taxonomyId = _update_taxonomy_if_needed(conn, taxonomy_dict, gbifId, taxon)

        match_info = {'taxonomyId': taxonomyId,
                      'lastMatched': last_matched,
                      'matchType': matchType,
                      'matchConfidence': matchConfidence}

        _update_match_info(conn, match_info, taxonomyId, name)

    # Logging and statistics
    end = time.time()
    n_matched_taxa_perc = i / n_taxa * 100
    n_matched_taxa = f"Number of matched names: {i}/{n_taxa} ({n_matched_taxa_perc:.2f}%)."
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
