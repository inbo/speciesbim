import pygbif
import psycopg2
import time
import datetime
import configparser
from helpers import execute_sql_from_file
from helpers import execute_sql_from_jinja_string

def gbif_match(conn, configParser, log_file, unmatched_only = False):
    with conn:
        # get scientificname table and store it as a dictionary
        if not unmatched_only:
            cur = execute_sql_from_file(conn, 'get_names_scientificname.sql',
                                                     {'limit': configParser.get('gbif_match', 'scientificnames-limit')})
        else:
            # unmatched names only
            cur = execute_sql_from_file(conn, 'get_names_scientificname_unmatched_only.sql',
                                        {'limit': configParser.get('gbif_match', 'scientificnames-limit')})
        cols_scientificname = list(map(lambda x: x[0], cur.description))
        scientificname = cur.fetchall()
        scientificname_dict = dict()
        if scientificname is not None:
            for row in scientificname:
                scientificname_dict[row[0]] = dict(zip(cols_scientificname, row))

        # get taxonomy table and store it as a dictionary
        cur = execute_sql_from_file(conn, 'get_taxa_taxonomy.sql')
        taxonomy = cur.fetchall()
        cols_taxonomy = list(map(lambda x: x[0], cur.description))
        taxonomy_dict = dict()
        if taxonomy is not None:
            for row in taxonomy:
                # use gbifID as key of taxonomy_dict
                taxonomy_dict[row[1]] = dict(zip(cols_taxonomy, row))

        n_taxa = len(scientificname)
        print(f"Number of taxa in scientificname table: {n_taxa}.")
        log = f"Match names (scientificName + authorship) to GBIF Backbone. "
        print(log)
        log_file.write(log + '\n')
        
        start = time.time()
        i = 0

        lastMatched = datetime.datetime.now()
        print(lastMatched)

        # match names to GBIF Backbone

        for id, row in scientificname_dict.items():
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
            except:
                log = f"No match found for {name} (id: {id})."
                print(log)
                log_file.write(log + '\n')

            taxon = {'gbifId': gbifId, 'scientificName': scientificName, 'kingdom': kingdom}
            match_info = {'taxonomyId': taxonomyId,
                          'lastMatched': lastMatched,
                          'matchType': matchType,
                          'matchConfidence': matchConfidence}
            if gbifId is not None and gbifId not in taxonomy_dict:
                print(f"Taxon {taxon['scientificName']} not in taxonomy.")
                # insert taxon in taxonomy table
                try:
                    # insert taxon in taxonomy
                    cols_string = '\"'+'\", \"'.join(taxon.keys())+'\"'
                    values_string = '\'' +'\', \''.join(map(lambda x: str(x).replace("'", "''"), taxon.values())) +'\''
                    execute_sql_from_jinja_string(conn,
                                                  "INSERT INTO taxonomy ({{cols}}) VALUES ({{values}}) ",
                                                  {'cols': cols_string,'values': values_string})
                    conn.commit()
                    # get id (PK) in taxonomy
                    cur = execute_sql_from_jinja_string(conn,
                                                        "SELECT \"id\" FROM taxonomy WHERE \"gbifId\" = {{gbifId}}",
                                                        {'gbifId': gbifId})
                    taxonomyId = cur.fetchall()
                    assert taxonomyId is not None, f"Taxon with gbifId {gbifId} not inserted into the taxonomy table."
                    assert len(taxonomyId) <= 1, \
                        f"Too many taxa returned for gbifId = {gbifId}. Duplicates in taxonomy table."
                    taxonomyId = taxonomyId[0][0]
                    print(f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {taxonomyId}).")
                except Exception as e:
                    print(e)
            elif gbifId is not None:
                taxonomyId = taxonomy_dict[gbifId]['id']
                taxonomy_dict_to_compare = {k: taxonomy_dict[gbifId][k] for k in taxon}
                taxonomy_dict_to_change = taxonomy_dict_to_compare.copy()
                if taxon == taxonomy_dict_to_compare:
                    print(f"Taxon {taxon['scientificName']} already present in taxonomy (id = {taxonomyId}).")
                else:
                    #unchanged fields
                    keys_same_values = dict(taxonomy_dict_to_compare.items() & taxon.items()).keys()
                    # remove unchanged fields
                    for key in keys_same_values: del taxonomy_dict_to_change[key]
                    for key in keys_same_values: del taxon[key]
                    print(f"Fields - values to change:")
                    [print(key, value) for key, value in taxonomy_dict_to_change.items()]
                    print(f"New fields - values:")
                    [print(key, value) for key, value in taxon.items()]
                    try:
                        cols_values_to_update = " , ".join(["\"" + str(i) + "\"" + " = " + "'" +
                                                            str(j).replace("'", "''") + "'"
                                                            for (i,j) in taxon.items()])
                        execute_sql_from_jinja_string(conn,
                                                      "UPDATE taxonomy SET {{cols_values_to_update}} "
                                                      "WHERE \"gbifId\" = {{gbifId}}",
                                                      {'cols_values_to_update': cols_values_to_update,
                                                       'gbifId': gbifId})
                    except Exception as e:
                        print(e)
            conn.commit()

            # update scientificname with info about match and taxonomyId
            try:
                print(f"Add taxonomiyId (FK) if present and match information to scientificname for {name} (id: {id}).")
                match_info['taxonomyId'] = taxonomyId
                match_info = {k: v for k, v in match_info.items() if v is not None}
                cols_values_to_update = " , ".join(["\"" + str(i) + "\"" + " = " + "'" + str(j) + "'"
                                                    for (i, j) in match_info.items()])
                execute_sql_from_jinja_string(conn,
                                              "UPDATE scientificname SET {{cols}} WHERE \"id\" = {{id}}",
                                              {'cols': cols_values_to_update, 'id': id})
                conn.commit()
            except Exception as e:
                print(e)

        end = time.time()
        n_matched_taxa_perc = i / n_taxa * 100
        n_matched_taxa = f"Number of matched names: {i}/{n_taxa} ({n_matched_taxa_perc:.2f}%)."
        print(n_matched_taxa)
        log_file.write(n_matched_taxa + '\n')
        elapsed_time = f"Match to GBIF Backbone performed in {round(end - start)}s."
        print(elapsed_time)
        log_file.write(elapsed_time + '\n')

        # try to improve number of matches by using rank
        # get unmatched_taxa and store it as a dictionary (lastMatched filter to limit to unmatched taxa of first round)
        lastMatched_sql = "'" + str(lastMatched) + "'"
        cur = execute_sql_from_jinja_string(conn, "SELECT * FROM scientificname WHERE \"taxonomyId\" IS NULL AND "
                                                  "\"lastMatched\" = {{lastMatched}}",
                                            {'lastMatched': lastMatched_sql})
        cols_scientificname = list(map(lambda x: x[0], cur.description))
        scientificname = cur.fetchall()
        scientificname_dict = dict()
        if scientificname is not None:
            for row in scientificname:
                scientificname_dict[row[0]] = dict(zip(cols_scientificname, row))
        n_taxa = len(scientificname)
        print(f"Number of unmatched taxa by name in scientificname table: {n_taxa}.")
        log = f"Use rank to improve match of names (scientificname + authorship) to GBIF Backbone. "
        print(log)
        log_file.write('\n' + log + '\n')

        # get updated taxonomy table and store it as a dictionary
        cur = execute_sql_from_file(conn, 'get_taxa_taxonomy.sql')
        taxonomy = cur.fetchall()
        cols_taxonomy = list(map(lambda x: x[0], cur.description))
        taxonomy_dict = dict()
        if taxonomy is not None:
            for row in taxonomy:
                # use gbifID as key of taxonomy_dict
                taxonomy_dict[row[1]] = dict(zip(cols_taxonomy, row))

        start = time.time()
        i = 0

        for id, row in scientificname_dict.items():
            # get name to check
            name = row['scientificName']
            if row['authorship'] is not None:
                name += " " + row['authorship']
            # get rank from taxon table
            deprecatedTaxonId = row['deprecatedTaxonId']
            cur = execute_sql_from_jinja_string(conn, "SELECT taxonranken FROM biodiv.taxon WHERE \"id\" = {{id}}",
                                                {'id': deprecatedTaxonId})
            rank = cur.fetchall()
            if rank is not None:
                assert len(rank) <= 1,\
                    f"Too many taxa returned from biodiv.taxon (id = {deprecatedTaxonId}) " \
                        f"for taxon with scientific name: {name} (id = {id})."
                rank = rank[0][0]
                # correct ranks, otherwise GBIF API crashes
                if rank == "subforma":
                    rank = "subform"
                elif rank == "forma":
                    rank = "form"
                if rank != "informal group" and rank != "division":
                    print(f'Try matching {name} with rank {rank}.')
                    # match name
                    gbif_taxon_info = pygbif.name_backbone(name=name, rank=rank, strict=True)
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
                    except:
                        log = f"No match found for {name} with rank {rank} (id: {id})."
                        print(log)
                        log_file.write(log + '\n')
                    taxon = {'gbifId': gbifId, 'scientificName': scientificName, 'kingdom': kingdom}
                    match_info = {'taxonomyId': taxonomyId,
                                  'lastMatched': lastMatched,
                                  'matchType': matchType,
                                  'matchConfidence': matchConfidence}
                    if gbifId is not None and gbifId not in taxonomy_dict:
                        print(f"Taxon {taxon['scientificName']} with rank {rank} not in taxonomy.")
                        # insert taxon in taxonomy table
                        try:
                            # insert taxon in taxonomy
                            cols_string = '\"' + '\", \"'.join(taxon.keys()) + '\"'
                            values_string = '\'' + '\', \''.join(map(lambda x: str(x).replace("'", "''"),
                                                                     taxon.values())) + '\''
                            execute_sql_from_jinja_string(conn,
                                                          "INSERT INTO taxonomy ({{cols}}) VALUES ({{values}}) ",
                                                          {'cols': cols_string, 'values': values_string})
                            conn.commit()
                            # get id (PK) in taxonomy
                            cur = execute_sql_from_jinja_string(conn,
                                                                "SELECT id FROM taxonomy WHERE \"gbifId\" = {{gbifId}}",
                                                                {'gbifId': gbifId})
                            taxonomyId = cur.fetchall()
                            assert taxonomyId is not None,\
                                f"Inserting taxon {taxon['scientificName']} into taxonomy table failed."
                            assert len(taxonomyId) <= 1,\
                                f"Many taxa returned for gbifId = {gbifId}. Duplicates detected in taxonomy table."
                            taxonomyId = taxonomyId[0][0]
                            print(f"Taxon {taxon['scientificName']} inserted in taxonomy (id = {taxonomyId}).")
                        except Exception as e:
                            print(e)

                        # update scientificname with info about match and taxonomyId
                        try:
                            print(
                                f"Add taxonomiyId (FK) and match information to scientificname for {name} "
                                f"with rank {rank} (id: {id}).")
                            match_info['taxonomyId'] = taxonomyId
                            match_info = {k: v for k, v in match_info.items() if v is not None}
                            cols_values_to_update = " , ".join(["\"" + str(i) + "\"" + " = " + "'" + str(j) + "'"
                                                                for (i, j) in match_info.items()])
                            cur = execute_sql_from_jinja_string(conn, "UPDATE scientificname SET {{cols}} "
                                                                      "WHERE \"id\" = {{id}}",
                                                                {'cols': cols_values_to_update, 'id': id})
                            conn.commit()
                        except Exception as e:
                            print(e)
                else:
                    print(rank + " is an invalid rank. No second attempt to match to GBIF Backbone performed.")
            else:
                print("No rank available for {name}. No second attempt to match to GBIF Backbone performed.")

        end = time.time()
        n_matched_taxa_perc = i / n_taxa * 100
        n_matched_taxa = f"Number of matched names: {i}/{n_taxa} ({n_matched_taxa_perc:.2f}%)."
        print(n_matched_taxa)
        log_file.write(n_matched_taxa + '\n')
        elapsed_time = f"Match to GBIF Backbone with rank performed in {round(end - start)}s."
        print(elapsed_time)
        log_file.write(elapsed_time + '\n')

if __name__ == "__main__":

    configParser = configparser.RawConfigParser()
    configParser.read(r'config.ini')
    conn = psycopg2.connect(dbname=configParser.get('database', 'dbname'),
                            user=configParser.get('database', 'user'),
                            password=configParser.get('database', 'password'),
                            host=configParser.get('database', 'host'),
                            port=int(configParser.get('database', 'port')),
                            options=f"-c search_path={configParser.get('database', 'schema')}")
    log_filename = "./logs/match_names_to_gbif_backbone_log.csv"
    log_file = open(log_filename, 'w')

    gbif_match(conn = conn, configParser = configParser, log_file= log_file, unmatched_only=True)