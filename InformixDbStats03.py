import IfxPyDbi as dbapi2
import smtplib
import csv

print('Going to create Informix Database connection with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
# Informix Database Connection String
ConStr = "SERVER=stats03;DB_LOCALE=en_US.57372;DATABASE=stats03;HOST=dhscbincdbs01al.asp.dhisco.com;SERVICE=40000;UID=adsstats;PWD=RVXUF7J?8-zV;"

# Establish Database connection with Informix Database.
try:
    print('Going to Connect to Informix DB.')
    conn = dbapi2.connect(ConStr, "", "")
    print('Connected Successfully!!')
except Exception as e:
    print(e)
    print('Connection failed!!')

print('Creating cursor.')
cur = conn.cursor()

# Cleanup Existing data in newtable
try:
 print('Going to execute delete newtable statement.')
 stmt = cur.execute('SELECT dadp_brand , dadp_brand_name , ud_parentbrand , ud_parentbrand_name FROM dev_stat03:adsstats.ud_master_brand;')
 print ('delete from newtable Successful!!')
except:
 print ('delete from newtable failed!!')



