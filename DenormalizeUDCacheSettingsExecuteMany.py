import IfxPyDbi as dbapi2
from datetime import date

print('Going to create Informix Database connection with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
# Informix Database Connection String
ConStr = "SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;"

# Establish Database connection with Informix Database.
try:
    print('Going to Connect to Informix DB.')
    conn = dbapi2.connect(ConStr, "", "")
    print('Connected Successfylly!!')
except Exception as e:
    print(e)
    print('Connection failed!!')
    conn.close()

#Create Cursor
print('Creating cursor.')
cur = conn.cursor()

# Prepare Query type 1
#query = "select a.*, b.min_lead_days, b.max_lead_days, b.cache_stale_time_value, b.cache_stale_time_units \
#from ud_cache_chain_config a, ud_cache_stale_time_config b \
#where a.chain = b.chain \
#and a.config_level = b.config_level \
#and a.affiliates = b.affiliates \
#and a.properties = b.properties \
#and a.chain in ('MC','BG','BR','CY','DP','ET','FN','RC','RZ','TO','XV','VC','AK','EB','AR','GE','OX','P2','DE','FP','TX','SI','WI','WH','MD','AL','EL','LC','XR','GX')"

#Prepare Query type 2
#query = "select a.*, b.min_lead_days, b.max_lead_days, b.cache_stale_time_value, b.cache_stale_time_units \
#from ud_cache_chain_config a, ud_cache_stale_time_config b \
#where a.chain = b.chain \
#and a.config_level = b.config_level \
#and a.affiliates = b.affiliates \
#and a.properties = b.properties  \
#and a.affiliates like '%00%'"

#Prepare Query type 3
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

try:
 print('Going to execute main Select statement.')
 cur.execute(query)
 rows = cur.fetchall()
 print ('Fetch Successful!!')
 today=date.today()
 mdyyyy=today.month.__str__()+"/"+today.day.__str__()+"/"+today.year.__str__()
 settings = []
 stmt = "INSERT INTO dadt:web.ud_cache_settings(exec_wk ,\
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
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
 for row in rows:
     settings.append(row[:0]+(mdyyyy,)+row[0:])
 try:
     cur.executemany(stmt , settings)
     conn.commit()
 except Exception as e:
     print(e)
     cur.close()
     conn.close()

except Exception as e:
 print(e)
 print('Fetch failed!!')
finally:
 cur.close()
 conn.close()
