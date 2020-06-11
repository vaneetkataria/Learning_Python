import IfxPyDbi as ifxDb
import mysql.connector as mariaDb
from datetime import date


print('Going to create Database connection with UD Informix with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
# Informix Database Connection String
ConStr = "SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;"

# Establish Database connection with Informix Database.
try:
    print('Establishing connection with UD Informix Database..')
    ifxConn = ifxDb.connect(ConStr, "", "")
    print('Successfully Connected with UD Informix Database !!')
except Exception as e:
    print(e)
    print('Connection failed with UD Informix Database!!')
    print('Exiting..')
    exit()

# Create Cursor
print('Creating cursor for UD Informix Database..')
ifxCur = ifxConn.cursor()

def cleanup_informix():
    print('Cleaning Informixdb connection..')
    ifxCur.close()
    ifxConn.close()

# Establish Database connection with Informix Database.
try:
    print('Establishing connection with Mariadb with connection parameters user=root, password=tiger, database=test..')
    mariadb_connection = mariaDb.connect(user='root', password='tiger', database='test')
    print('Successfully Connected with Mariadb !!')
except Exception as e:
    print(e)
    print('Connection failed with Mariadb!!')
    cleanup_informix()
    print('Exiting..')
    exit()

# Create Cursor
print('Creating cursor for UD Informix Database..')
mariaCur = mariadb_connection.cursor()

#Prepare query
query = "select a.chain as chain, \
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


def cleanup_mariadb():
    print('Cleaning Maridb connection..')
    mariaCur.close()
    mariadb_connection.close()

def cleanup():
    cleanup_informix()
    cleanup_mariadb()

try:
    print('Going to execute main Select statement with UD Informix Database.')
    ifxCur.execute(query)
    rows = ifxCur.fetchall()
    print('Fetch Successful!!')
    settings = []
    stmt = "INSERT INTO ud_cache_settings(exec_wk ,\
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
          cache_stale_time_value,\
          cache_stale_time_units) \
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for row in rows:
        settings.append(row[:0] + (date.today().__str__(),) + row[0:])
    try:
        print('Inserting data into Mariadb..')
        mariaCur.executemany(stmt, settings)
        mariadb_connection.commit()
        print('Data inserted Successfully in Mariadb!!')
    except Exception as e:
        print('Exception while inserting into mariadb')
        print(e)
        print('Rolling back transaction!!')
        mariadb_connection.rollback()
except Exception as e:
    print(e)
    print('Fetch failed!!')
finally:
    cleanup()

