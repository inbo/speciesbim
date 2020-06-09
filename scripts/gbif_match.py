import pygbif
import psycopg2
import time
import datetime
import configparser
from helpers import execute_sql_from_file
from helpers import execute_sql_from_jinja_string

def gbif_match(conn, configParser, log_filename = "./logs/match_names_to_gbif_backbone_log.csv"):
    with conn:
        # get scientificname table and store it as a dictionary
        cur = execute_sql_from_file(conn, 'get_names_scientificname.sql',
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

        # correct ranks, otherwise GBIF API crashes
        # dict_rank = {"subforma": "subform",
        #              "forma": "form"}

        # scientificname = scientificname.replace(dict(taxonranken=dict_rank))

        n_taxa = len(scientificname)
        print(f"Number of taxa in scientificname table: {n_taxa}.")
        not_found_taxa_log = open(log_filename, 'w')
        log = f"Match names (scientificName + authorship) to GBIF Backbone. "
        print(log)
        not_found_taxa_log.write(log + '\n')

        not_found_taxa_log.close()
        not_found_taxa_log = open(log_filename, 'a')

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
                not_found_taxa_log.write(log + '\n')

            # add values to dfs
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
                    values_string = '\'' +'\', \''.join(map(lambda x: str(x), taxon.values())) +'\''
                    cur = execute_sql_from_jinja_string(conn, "INSERT INTO taxonomy ({{cols}}) VALUES ({{values}}) ",
                                                        {'cols': cols_string,'values': values_string})
                    conn.commit()
                    # get id (PK) in taxonomy
                    cur = execute_sql_from_jinja_string(conn, "SELECT id FROM taxonomy WHERE \"gbifId\" = {{gbifId}}",
                                                        {'gbifId': gbifId})
                    taxonomyId = cur.fetchone()[0] # Change it to fecthall() and assert that legnth = 1
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
                        cols_values_to_update = " , ".join(["\"" + str(i) + "\"" + " = " + "'" + str(j) + "'"
                                                            for (i,j) in taxon.items()])
                        cur = execute_sql_from_jinja_string(conn,
                                                            "UPDATE taxonomy SET {{cols_values_to_update}} "
                                                            "WHERE \"gbifId\" = {{gbifId}}",
                                                            {'cols_values_to_update': cols_values_to_update, 'gbifId': gbifId})
                        cols_values_to_update = " , ".join()
                    except Exception as e:
                        print(e)
            conn.commit()

            # update scientificname with info about match and taxonomyId
            try:
                print(f"Add taxonomiyId (FK) and match information to scientificname for {name}  (id: {id}).")
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

            # info_to_add_scientificname = {'id': id, 'lastMatched': lastMatched, 'matchType': matchType,
            #                              'matchConfidence': matchConfidence}
            # taxonomy = taxonomy.append(taxon_to_add_taxonomy, ignore_index=True)
            # scientificname_info_match = scientificname_info_match.append(info_to_add_scientificname, ignore_index=True)
        end = time.time()
        n_matched_taxa = f"Number of matched names: {i}/{n_taxa} {i / n_taxa * 100}%."
        print(n_matched_taxa)
        not_found_taxa_log.write(n_matched_taxa + '\n')
        elapsed_time = f"Match to GBIF Backbone performed in {round(end - start)}s."
        print(elapsed_time)
        not_found_taxa_log.write(elapsed_time + '\n')
        not_found_taxa_log.close()

    # TODO: try to improve matches by using rank

if __name__ == "__main__":
    configParser = configparser.RawConfigParser()
    configParser.read(r'config.ini')

    conn = psycopg2.connect(dbname=configParser.get('database', 'dbname'),
                            user=configParser.get('database', 'user'),
                            password=configParser.get('database', 'password'),
                            host=configParser.get('database', 'host'),
                            port=int(configParser.get('database', 'port')),
                            options=f"-c search_path={configParser.get('database', 'schema')}")
    gbif_match(conn = conn, configParser = configParser)