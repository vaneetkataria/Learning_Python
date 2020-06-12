import IfxPyDbi as ifxDb
import mysql.connector as mariadb
from datetime import date

print('Going to create Informix Database connection with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
# Informix Database Connection String
ConStr = "SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;"

# Establish Database connection with Informix Database.
try:
    print('Establishing conncetion with Informix DB.')
    dadtIfxDbConn = ifxDb.connect(ConStr, "", "")
    print('Connection Successfully established with InformixDb!!')
except Exception as e:
    print(e)
    print('INformix Db Connection failed!!')

print('Creating cursor.')
dadtIfxDbCur = dadtIfxDbConn.cursor()

# Establish Database connection with Mariadb Database.
try:
    print('Establishing conncetion with Maria DB.')
    mariadb_connection = mariadb.connect(user='root', password='tiger', database='test')
    print('Connection Successfully established with MariaDb!!')
except Exception as e:
    print(e)
    print('Mariadb Connection failed!!')

print('Creating cursor.')
mariadbCur = mariadb_connection.cursor()


# Cleanup Existing data in informix newtable
try:
 print('Going to execute delete newtable statement in informix db. ')
 stmt = dadtIfxDbCur.execute('delete from newtable;')
 print ('delete from newtable Successful!!')
except:
 print ('delete from newtable failed!!')

print('Going to exceute inserts in informixdb.')
dadtIfxDbCur.execute("insert into newtable values( 1, 'Sunday', 101, 201 )")
dadtIfxDbCur.execute("insert into newtable values( 2, 'Monday', 102, 202 )")
dadtIfxDbCur.execute("insert into newtable values( 3, 'Tuesday', 103, 203 )")
dadtIfxDbCur.execute("insert into newtable values( 4, 'Wednesday', 104, 204 )")
dadtIfxDbCur.execute("insert into newtable values( 5, 'Thursday', 105, 2005 )")
dadtIfxDbCur.execute("insert into newtable values( 6, 'Friday', 106, 206 )")
dadtIfxDbCur.execute("insert into newtable values( 7, 'Saturday', 107, 207 )")
dadtIfxDbConn.commit ()

# Cleanup Existing data in newtable
try:
 print('Going to execute delete newtable1 statement in mariadb.')
 stmt = mariadbCur.execute('delete from newtable_1;')
 print ('delete from newtable_1 Successful!!')
except:
 print ('delete from newtable_1 failed!!')

print('Executing select from informixdb.')
dadtIfxDbCur.execute("SELECT c1, c2, c3 , c4  FROM newtable")
rows = dadtIfxDbCur.fetchall()

try:
 for row in rows:
     c1 , c2 , c3 , c4 = row
     mariadbCur.execute("INSERT INTO newtable_1(c0,\
     c1,\
     c2,\
     c3,\
     c4) \
     VALUES ('%s',\
     %s,\
     '%s',\
     %s,\
     %s)"
     % ('final2',\
         c1,\
         c2,\
         c3, \
         c4))
 mariadb_connection.commit()
except Exception as e:
  print(e)
finally:
 dadtIfxDbCur.close()
 dadtIfxDbConn.close()
 mariadbCur.close()
 mariadb_connection.close()


