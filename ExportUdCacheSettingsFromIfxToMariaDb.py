import sys
import IfxPyDbi as ifxDb
import mysql.connector as mariaDb
from datetime import date

fetch_master_brands_stmt = "SELECT dadp_brand ,  \
                       dadp_brand_name ,  \
                       ud_parentbrand , \
                       ud_parentbrand_name  \
                       FROM dev_stat03:adsstats.ud_master_brand;"
#Prepare query
fetch_ud_cache_settings_stmt = "select a.chain as chain, \
         a.config_level as config_level , \
         a.affiliates as affiliates , \
         a.properties as properties ,\
         a.cache_keys as cache_keys , \
         a.noncache_keys as noncache_keys, \
         a.cache_neg_rates as cache_neg_rates , \
         a.force_cache_only as force_cache_only ,\
         a.no_override_sourceonly as no_override_sourceonly , \
         b.min_lead_days as min_lead_days ,\
         b.max_lead_days as max_lead_days , \
         b.cache_stale_time_value as cache_stale_time_value , \
         b.cache_stale_time_units as cache_stale_time_units \
         from ud_cache_chain_config a, ud_cache_stale_time_config b \
         where a.chain = b.chain \
         and a.config_level = b.config_level \
         and a.affiliates = b.affiliates \
         and a.properties = b.properties"

insert_ud_cache_settings_stmt = "INSERT INTO ud_cache_settings(exec_wk ,\
          chain,\
          config_level, \
          affiliates, \
          properties, \
          cache_keys, \
          noncache_keys,\
          cache_neg_rates,\
          force_cache_only,\
          no_override_sourceonly,\
          min_lead_days,\
          max_lead_days,\
          cache_stale_time_value ,\
          cache_stale_time_units,\
          dadp_brand_name,\
          ud_parentbrand,\
          ud_parentbrand_name)\
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

def connect_to_dadt_informixdb():
    global dadtIfxDbConn, dadtIfxDbCur
    print(
        'Going to create Database connection with dadt Informix with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
    # Informix Database Connection String
    ConStr = "SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;"
    # Establish Database connection with Informix Database.
    try:
        print('Establishing connection with dadt Informix Database..')
        dadtIfxDbConn = ifxDb.connect(ConStr, "", "")
        print('Successfully Connected with dadt Informix Database !!')
    except Exception as e:
        print(e)
        print('Connection failed with dadt Informix Database!!')
        print('Exiting..')
        sys.exit()
    # Create Cursor
    print('Creating cursor for dadt Informix Database..')
    dadtIfxDbCur = dadtIfxDbConn.cursor()

def cleanup_dadt_informix_db_connections():
    print('Cleaning Informixdb connection..')
    dadtIfxDbCur.close()
    dadtIfxDbConn.close()

def connect_to_stats03_informixdb():
    global stats03IfxDbConn, stats03IfxDbCur
    print(
        'Going to create Database connection with Stats03 Informix with Connection "SERVER=stats03;DB_LOCALE=en_US.57372;DATABASE=stats03;HOST=dhscbincdbs01al.asp.dhisco.com;SERVICE=40000;UID=adsstats;PWD=RVXUF7J?8-zV;')
    # Informix Database Connection String
    ConStr = "SERVER=stats03;DB_LOCALE=en_US.57372;DATABASE=stats03;HOST=dhscbincdbs01al.asp.dhisco.com;SERVICE=40000;UID=adsstats;PWD=RVXUF7J?8-zV;"
    # Establish Database connection with Informix Database.
    try:
        print('Establishing connection with Stats03 Informix Database..')
        stats03IfxDbConn = ifxDb.connect(ConStr, "", "")
        print('Successfully Connected with Stats03 Informix Database !!')
    except Exception as e:
        print(e)
        print('Connection failed with Stats03 Informix Database!!')
        print('Exiting..')
        sys.exit()
    # Create Cursor
    print('Creating cursor for Stats03 Informix Database..')
    stats03IfxDbCur = stats03IfxDbConn.cursor()

def cleanup_stats03_informixdb_connections():
    print('Cleaning Informixdb connection..')
    stats03IfxDbCur.close()
    stats03IfxDbConn.close()

def connect_to_mariadb():
    global mariadb_connection, mariaCur
    # Establish Database connection with Informix Database.
    try:
        print(
            'Establishing connection with Mariadb with connection parameters user=root, password=tiger, database=test..')
        mariadb_connection = mariaDb.connect(user='root', password='tiger', database='test')
        print('Successfully Connected with Mariadb !!')
    except Exception as e:
        print(e)
        print('Connection failed with Mariadb!!')
        cleanup_dadt_informix_db_connections()
        print('Exiting..')
        sys.exit()
    # Create Cursor
    print('Creating cursor for UD Informix Database..')
    mariaCur = mariadb_connection.cursor()

def cleanup_mariadb_connections():
    print('Cleaning Maridb connection..')
    mariaCur.close()
    mariadb_connection.close()

def cleanup_connections():
    cleanup_dadt_informix_db_connections()
    cleanup_stats03_informixdb_connections
    cleanup_mariadb_connections()


def fetch_ud_cache_settings():
    print('Going to execute main Select statement with UD Informix Database.')
    dadtIfxDbCur.execute(fetch_ud_cache_settings_stmt)
    rows = dadtIfxDbCur.fetchall()
    print('Fetch Successful!!')
    return rows


def fetch_master_brands():
    print('Going to execute fetch_master_brands statement with stats03 Informix Database.')
    stats03IfxDbCur.execute(fetch_master_brands_stmt)
    rows = stats03IfxDbCur.fetchall()
    print('Master Brands Fetch Successful!!')
    masterBrands = {}
    for row in rows:
        masterBrands.setdefault(row[0], row)
    return masterBrands

def join_ud_cache_settings_and_master_brand(settings, master_brands):
    today = (date.today().__str__(),)
    none = (None , None , None)
    merged = []
    for setting in settings:
        master_brand = none
        if(master_brands.__contains__(setting[0])):
           master_brand = master_brands.get(setting[0])[1:]
        merged.append(today+setting+master_brand)
    return merged

def insert_into_mariadb(settings):
    try:
        print('Inserting data into Mariadb..')
        mariaCur.executemany(insert_ud_cache_settings_stmt, settings)
        mariadb_connection.commit()
        print('Data inserted Successfully in Mariadb!!')
    except Exception as e:
        print('Exception while inserting into mariadb')
        print(e)
        print('Rolling back transaction!!')
        mariadb_connection.rollback()


def main_procedure():
    connect_to_dadt_informixdb()
    connect_to_mariadb()
    connect_to_stats03_informixdb()
    try:
        cache_settings = fetch_ud_cache_settings()
        brands = fetch_master_brands()
        joined = join_ud_cache_settings_and_master_brand(cache_settings, brands)
        insert_into_mariadb(joined)
    except Exception as e:
        print(e)
        print('Fetch failed!!')
    finally:
        cleanup_connections()

main_procedure()

