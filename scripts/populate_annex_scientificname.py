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
        annex_scientificnames = dict()
        fields = next(annex_data)
        print("Columns in " + path + ": " + ", ".join(fields))
        for (i, row) in enumerate(annex_data):
            id = i + 1
            scientific_name_original = row[1]
            scientific_name_corrected = row[2]
            annex_id = row[0]
            remarks = row[4]
            annex_scientificnames[id] = {'id': id,
                                         'scientificNameOriginal': scientific_name_original,
                                         'scientificName': scientific_name_corrected,
                                         'annexCode': annex_id,
                                         'remarks': remarks}
    return annex_scientificnames


def populate_annex_scientificname(conn, config_parser, annex_file):
    """ Populate the table annexscientificname

    If taxa-limit in configuraton file is not a empty string but a number n, then the first n taxa are imported into
    the table

    """
    annex_names = _get_annex(path=annex_file)
    message_n_names_in_annex_file = "Number of taxa listed in official annexes and ordinances: " + \
                                    str(len(annex_names))
    print(message_n_names_in_annex_file)
    logging.info(message_n_names_in_annex_file)
    n_taxa_max = config_parser.get('annex_scientificname', 'taxa-limit')
    if len(n_taxa_max) > 0:
        n_taxa_max = int(n_taxa_max)
    else:
        n_taxa_max = len(annex_names)
    start = time.time()
    counter_insertions = 0
    for value in annex_names.values():
        values = value.values()
        fields = value.keys()
        if counter_insertions < n_taxa_max:
            template = """INSERT INTO annexscientificname ({{ col_names | surround_by_quote | join(', ') | sqlsafe 
            }}) VALUES {{ values | inclause }} """
            execute_sql_from_jinja_string(
                conn,
                template,
                context={'col_names': tuple(fields), 'values': tuple(values)}
            )
            counter_insertions += 1
            # running infos on screen (no logging)
            if counter_insertions % 20 == 0:
                elapsed_time = time.time() - start
                expected_time = elapsed_time / counter_insertions * (n_taxa_max - counter_insertions)
                info_message = "\r" + \
                               f"{counter_insertions}/{n_taxa_max} taxa inserted in annexscientificname in" + \
                               f" {round(elapsed_time, 2)}s." + \
                               f" Expected time to go: {round(expected_time, 2)}s."
                print(info_message, end="", flush=True)

        else:
            break
    # Logging and statistics
    end = time.time()
    n_taxa_inserted = f"\nTotal number of taxa inserted in annexscientificname: {counter_insertions}"
    print(n_taxa_inserted)
    logging.info(n_taxa_inserted)
    elapsed_time = f"Table annexscientificname populated in {round(end - start)}s."
    print(elapsed_time)
    logging.info(elapsed_time)


if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/populate_annexscientificname_log.csv")
    annex_file_path = "../data/raw/official_annexes.csv"
    # for demo
    annex_file_path_demo = "../data/raw/official_annexes_demo.csv"
    demo = config.getboolean('demo_mode', 'demo')

    if not demo:
        populate_annex_scientificname(conn=connection, config_parser=config, annex_file=annex_file_path)
    else:
        populate_annex_scientificname(conn=connection, config_parser=config, annex_file=annex_file_path_demo)
