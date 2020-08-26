from helpers import get_database_connection, get_config, setup_log_file, execute_sql_from_jinja_string
from csv import reader
import time
import logging

def _get_annex(path):
    """ Read taxa from file with list of taxa (names) contained in official annexes

    Return a dictionary of names and their corrected versions (which are equal to the names if no correction is needed)
    """
    with open(path) as csvfile:
        annex_data = reader(csvfile)
        scientificnames_annex = dict()
        fields = next(annex_data)
        print("Columns in "+ path + ": " + ", ".join(fields))
        for (i, row) in enumerate(annex_data):
            id = i+1
            scientific_name_original = row[1]
            scientific_name_corrected = row[2]
            annex_id = row[0]
            remarks = row[4]
            scientificnames_annex[id] = {'id': id,
                                        'scientificNameOriginal': scientific_name_original,
                                        'scientificName': scientific_name_corrected,
                                        'annexCode': annex_id,
                                        'remarks': remarks}
    return scientificnames_annex

def populate_scientificname_annex(conn, config_parser, annex_file):
    """ Populate the table scientificnameannex

    If taxa-limit in configuraton file is not a empty string but a number n, then the first n taxa are imported into
    the table

    """
    n_taxa_max = config_parser.get('scientificname_annex', 'taxa-limit')
    if len(n_taxa_max) > 0:
        n_taxa_max = int(n_taxa_max)
    else:
        n_taxa_max = None
    start = time.time()
    annex_names = _get_annex(path=annex_file)
    counter_insertions = 0
    for value in annex_names.values():
        values = value.values()
        fields = value.keys()
        if (n_taxa_max is None or counter_insertions < n_taxa_max):
            template = """INSERT INTO scientificnameannex ({{ col_names | surround_by_quote | join(', ') | sqlsafe }}) VALUES {{ values | inclause }}"""
            execute_sql_from_jinja_string(
                conn,
                template,
                context={'col_names': tuple(fields), 'values': tuple(values)}
            )
            counter_insertions += 1
        else:
            break
    # Logging and statistics
    end = time.time()
    print(f"Total number of taxa inserted in scientificnameannex: {counter_insertions}")
    elapsed_time = f"Table scientificnameannex populated in {round(end - start)}s."
    print(elapsed_time)
    logging.info(elapsed_time)


if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/populate_scientificnameannex_log.csv")
    annex_file_path = "../data/raw/official_annexes.csv"
    populate_scientificname_annex(conn=connection, config_parser=config, annex_file=annex_file_path)
