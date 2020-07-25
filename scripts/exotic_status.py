import logging
import time
from helpers import execute_sql_from_jinja_string, get_database_connection, get_config, \
    setup_log_file, paginated_name_usage, print_indent

def _get_alien_taxa(datasetKey):
    """ Retrieve all taxa in GBIF checklist containing the exotic species in BE.
    The function returns these taxa as a list of nubKey values (= GBIF taxon kayes from GBIF Backbone)"""

    alien_taxa_in_be = paginated_name_usage(datasetKey=datasetKey)
    alien_taxa_list = []
    for taxon in alien_taxa_in_be:
        nubKey = taxon.get('nubKey')
        origin = taxon.get('origin')
        if (nubKey is not None and origin == "SOURCE"):
            alien_taxa_list += [nubKey]
    return alien_taxa_list

def _find_exotic_taxa(exotic_taxon, taxa, exotic_taxa_list, depth=0):
    """ Function to search an exotic taxon in taxa and, recursively, all its children.

    Params: depth is the recursion level (used for log indentation)

    Returns a list of ids of exotic taxa in taxonomy table
    """

    if (exotic_taxon in taxa):
        print_indent(
            f"Taxon {taxa[exotic_taxon]['scientificName']} (gbifId: {exotic_taxon}) is exotic in Belgium.",
            depth=depth)
        id = taxa[exotic_taxon]['id']
        exotic_taxa_list += [id]
        for t in taxa.values():
            if (t['parentId'] == id):
                exotic_taxa_list = _find_exotic_taxa(exotic_taxon=t['gbifId'],
                                                     taxa=taxa,
                                                     exotic_taxa_list=exotic_taxa_list,
                                                     depth=depth+1)
    return exotic_taxa_list

def populate_is_exotic_be_field(conn, config_parser, exotic_status_source):

    msg = f"We'll now retrieve the GBIF checklist containing the exotic taxa in Belgium, datasetKey: {exotic_status_source}."
    print(msg)
    logging.info(msg)

    start_time = time.time()
    # get alien taxa from GRIIS Belgium checklist
    alien_taxa = _get_alien_taxa(datasetKey=exotic_status_source)
    end_time = time.time()

    msg = f"Retrieved {len(alien_taxa)} exotic taxa in {round(end_time-start_time)}s."
    print(msg)
    logging.info(msg)

    taxon_cur = execute_sql_from_jinja_string(conn, "SELECT * FROM taxonomy", dict_cursor=True)

    total_taxa_count = taxon_cur.rowcount
    msg = f"We'll now update exotic_be field for {total_taxa_count} taxa of the taxonomy table."
    print(msg)
    logging.info(msg)

    start_time = time.time()

    taxa_to_check = dict()
    for taxon in taxon_cur:
        id = taxon['id']
        parentId = taxon['parentId']
        scientificName = taxon['scientificName']
        gbifId = taxon['gbifId']
        taxa_to_check[gbifId] = {'id': id, 'gbifId': gbifId, 'scientificName': scientificName, 'parentId': parentId}

    exotic_taxa_ids= []

    for exotic_taxon in alien_taxa:
        exotic_taxa_ids = _find_exotic_taxa(exotic_taxon=exotic_taxon,
                                                 taxa=taxa_to_check,
                                                 exotic_taxa_list= exotic_taxa_ids,
                                                 depth=0)

    msg = f"{len(exotic_taxa_ids)} exotic taxa found in taxonomy."
    print(msg)
    logging.info(msg)

    # set exotic_be = True for exotic taxa and False for the others
    template = """ UPDATE taxonomy SET "exotic_be" = """ \
               + """ CASE WHEN "id" IN {{ ids | inclause }} THEN true""" \
               + """ ELSE false END"""
    update_exotic_be_cur = execute_sql_from_jinja_string(conn, sql_string=template, context={'ids': exotic_taxa_ids})

    end_time = time.time()

    msg = f"Field exotic_be updated for {update_exotic_be_cur.rowcount} taxa in taxonomy in {round(end_time - start_time)}s."
    print(msg)
    logging.info(msg)

if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/populate_exotic_status_field.csv")
    # datasetKey of Global Register of Introduced and Invasive Species - Belgium
    griis_be = "6d9e952f-948c-4483-9807-575348147c7e"

    populate_is_exotic_be_field(conn=connection, config_parser=config, exotic_status_source = griis_be)
