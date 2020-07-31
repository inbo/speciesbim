import logging
import time
from pycountry import languages as pylang

from helpers import execute_sql_from_jinja_string, get_database_connection, setup_log_file, get_config, \
    paginated_name_usage

def _iso639_1_to_2_dict(lang):
    """
    Takes some 2 letter-code languages (['fr', 'nl']).
    Returns a dictionary with the corresponding 3-letter codes ('fra', 'nld') and bibliogaphic synonyms ('fre', 'dut')
    as keys, the corresponding 2-leter-code languages as values.
    Example:
        {'fra': 'fr',
         'fre': 'fr',
         'nld': 'nl',
         'dut': 'nl'}
        """
    languages_info = [pylang.get(alpha_2=l) for l in lang]
    # attributes to search for in a language object
    attributes = ['alpha_3', 'bibliographic']
    # create a dictionary with 3-letters as keys and 2-letter format as values
    # something like {'fra': 'fr', 'fre': 'fr', 'nld': 'nl', 'dut': 'nl', 'eng': 'en'}
    gbif_languages = [(getattr(language_info, i), getattr(language_info, "alpha_2")) for language_info in languages_info
                      for i in attributes if
                      i in dir(language_info)]
    gbif_languages = dict(gbif_languages)
    return gbif_languages

def _get_vernacular_names_gbif(gbif_taxon_id, languages3=None):
    # languages3 is a list of 3-letter language codes (ISO 639-1 Code) (default: no filtering)
    # GBIF uses ISO 639-2 codes (3-letters format)
    # !! Synonyms !!: GBIF uses 'fra' for french and 'nld' for dutch
    # example: ['fra', 'nld']

    # returns a list of dict such as:
    #
    # [{'taxonKey': 5, 'vernacularName': 'champignons', 'language': 'fra',
    #   'source': 'Integrated Taxonomic Information System (ITIS)', 'sourceTaxonKey': 102179465},
    #  {'taxonKey': 5, 'vernacularName': 'schimmels', 'language': 'nld', 'country': 'BE',
    #   'source': 'Belgian Species List', 'sourceTaxonKey': 100489794}]

    names_data = paginated_name_usage(key=gbif_taxon_id, data="vernacularNames")

    if languages3 is not None:
        names_data = [nd for nd in names_data if nd['language'] in languages3]

    return names_data


def populate_vernacular_names(conn, config_parser, empty_only, filter_lang=None):
    # If empty only, only process the taxa currently without vernacular names
    # Otherwise, process all entries in the taxonomy table
    # filter_lang is a list of language codes (ISO 639-1 Code) (default: no filtering)
    if empty_only:
        taxa_selection_sql = """SELECT *
                                FROM taxonomy
                                WHERE NOT EXISTS (SELECT vernacularname."taxonomyId" FROM vernacularname WHERE taxonomy.id = vernacularname."taxonomyId") {% if limit %} LIMIT {{ limit }} {% endif %}"""
    else:
        taxa_selection_sql = """SELECT * FROM taxonomy {% if limit %} LIMIT {{ limit }} {% endif %}"""

    limit = config_parser.get('vernacular_names', 'taxa-limit')
    cur = execute_sql_from_jinja_string(conn, sql_string=taxa_selection_sql, context={'limit': limit}, dict_cursor=True)

    msg = f"We'll now load vernacular names for {cur.rowcount} entries in the taxonomy table. Languages: "
    if filter_lang is not None:
        msg += ", ".join(filter_lang)
    print(msg)
    logging.info(msg)

    # Create dictionary mapping 3-letter codes (as stored in GBIF) and 2-letter codes
    if filter_lang is not None:
        filter_lang_dict = _iso639_1_to_2_dict(filter_lang)
    else:
        filter_lang_dict = None

    # Get list of 3-letters format languages
    languages3 = None
    if filter_lang_dict is not None:
        languages3 = list(filter_lang_dict.keys())

    total_vernacularnames_counter = 0
    total_taxa_counter = 0
    start_time = time.time()

    for taxon in cur:
        taxonomy_id = taxon['id']
        gbif_taxon_id = taxon['gbifId']

        total_taxa_counter += 1

        vns = _get_vernacular_names_gbif(gbif_taxon_id, languages3=languages3)
        for vernacular_name in vns:
            name = vernacular_name.get('vernacularName')
            lang_code = filter_lang_dict[vernacular_name.get('language')]
            source = vernacular_name.get('source')

            msg = f"Now saving '{name}'({lang_code}) for taxon with ID: {taxonomy_id} (source: {source})"
            print(msg)
            logging.info(msg)

            insert_template = """INSERT INTO vernacularname("taxonomyId", "language", "name", "source") VALUES ({{ taxonomy_id}}, {{ lang_code }}, {{ name }}, {{ source }})"""
            execute_sql_from_jinja_string(conn, sql_string=insert_template, context={'taxonomy_id': taxonomy_id,
                                                                                     'lang_code': lang_code,
                                                                                     'name': name,
                                                                                     'source': source})
            total_vernacularnames_counter += 1

    end_time = time.time()

    msg = f"Done loading {total_vernacularnames_counter} (for {total_taxa_counter} taxa) vernacular names in {round(end_time - start_time)}s."
    print(msg)
    logging.info(msg)


if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/vernacular_names.csv")

    populate_vernacular_names(connection, config_parser=config, empty_only=False)
