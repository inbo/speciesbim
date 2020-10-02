from helpers import get_database_connection, get_config, setup_log_file, execute_sql_from_jinja_string, execute_sql_from_file
import time
import logging

def _update_scientificname_id(conn, scientificname_id, row_id):
    """ Add scientificnameId in annexscientificname if the name is found in scientificname table"""
    template = """ UPDATE annexscientificname SET "scientificnameId" = {{ scientificname_id }} """\
               """ WHERE "id" = {{ id }} """
    execute_sql_from_jinja_string(conn,
                                  template,
                                  {'scientificname_id': scientificname_id,
                                   'id': row_id})

def _insert_or_get_scientificname(conn, scientific_name):
    """ Insert or select a name in scientificname table based on its scientific name

        If the scientific name already exists in the scientificname table, select it.
        Otherwise, insert it in a new row.

        In both cases, returns the row id """

    sc_name_template = """WITH ins AS (
        INSERT INTO scientificname ("scientificName")
        VALUES ({{ scientific_name }})         -- input value
        ON CONFLICT ("scientificName") DO NOTHING
        RETURNING scientificname.id
        )
    SELECT id FROM ins
    UNION  ALL
    SELECT "id" FROM scientificname          -- 2nd SELECT never executed if INSERT successful
    WHERE "scientificName" = {{ scientific_name }}  -- input value a 2nd time
    LIMIT  1;"""
    cur = execute_sql_from_jinja_string(conn,
                                        sql_string=sc_name_template,
                                        context={'scientific_name': scientific_name},
                                        dict_cursor=True)
    return cur.fetchone()['id']

def match_annexscientificname_to_scientificname(conn, config_parser, unmatched_only=True):
    """ Match names in annexscientificname table to names in scientificname table
    Names not found in table scientificname are added """
    limit = config_parser.get('annex_scientificname_to_scientificname', 'scientificnames-limit')
    demo = config_parser.getboolean('demo_mode', 'demo')
    # get data from the scientificname table
    scientificname_cur = execute_sql_from_file(conn,
                                    'get_names_scientificname.sql')
    total_sn_count = scientificname_cur.rowcount
    n_taxa_message = f"Number of taxa in scientificname table: {total_sn_count}"
    print(n_taxa_message)
    annexscientificname_cur = execute_sql_from_file(conn,
                                                    'get_names_annexscientificname.sql',
                                                    {'unmatched_only': unmatched_only,
                                                     'limit': limit},
                                                    dict_cursor=True)
    total_sn_annex_count = annexscientificname_cur.rowcount
    n_taxa_message = f"Number of taxa with a not empty scientificName in annexscientificname table: {total_sn_annex_count}"
    print(n_taxa_message)
    log = f"Match names in annexscientificname to names in scientificname"
    print(log)
    logging.info(log)

    start = time.time()

    # match or add names to scientificname table
    for row in annexscientificname_cur:
        row_id = row['id']
        # get name to check
        name = row['scientificName']
        print(f'Try matching the "{name}" name and add to scientificname if not found')
        scientificname_id = _insert_or_get_scientificname(conn=conn, scientific_name=name)
        print(f"Update scientificnameId ({ scientificname_id }) for {name} (id: {row_id}).")
        _update_scientificname_id(conn, scientificname_id=scientificname_id, row_id=row_id)

    # Get number of names in scientificname table
    n_rows_scientificname_cur = execute_sql_from_jinja_string(conn,
                                                              'SELECT COUNT(*) FROM scientificname',
                                                              dict_cursor=True)
    n_rows_scientificname = n_rows_scientificname_cur.fetchone()[0]
    n_names_added_from_annex = n_rows_scientificname - total_sn_count
    # Logging and statistics
    end = time.time()
    summary_match = f"Number of names in annexscientificname table inserted in scientificname table: " \
            f" { n_names_added_from_annex }/ { total_sn_annex_count }"
    print(summary_match)
    logging.info(summary_match)
    elapsed_time = f"Matching of annexscientificname to scientificname table performed in {round(end - start)}s."
    print(elapsed_time)
    logging.info(elapsed_time)


# def _match_name_annex_to_scientificname(conn, name_annex):
#     """ Match a scientific name in annexscientificname table to scientificname
#
#     Names not found in table scientificname are added
#     """
#
#     template = """SELECT * FROM scientificname WHERE "scientificName" = {{ name_annex }} """
#     scientific_cur = execute_sql_from_jinja_string(conn,sql_string=template, context={'name_annex': name_annex})
#     scientificname_values = scientific_cur.fetchall()
#     cols_scientificname = list(map(lambda x: x[0], scientific_cur.scientificname))
#
if __name__ == "__main__":
    connection = get_database_connection()
    config = get_config()
    setup_log_file("./logs/match_annexscientificname_to_scientificname.log")

    match_annexscientificname_to_scientificname(conn=connection, config_parser=config, unmatched_only=True)