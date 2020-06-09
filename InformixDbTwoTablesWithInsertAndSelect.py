import IfxPyDbi as dbapi2

print('Going to create Informix Database connection with Connection String SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;')
# Informix Database Connection String
ConStr = "SERVER=udqa;DB_LOCALE=en_US.57372;DATABASE=dadt;HOST=dhscalqaldbm01a.asp.dhisco.com;SERVICE=40013;UID=web;PWD=5kywalk3r;"

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
 stmt = cur.execute('delete from newtable;')
 print ('delete from newtable Successful!!')
except:
 print ('delete from newtable failed!!')


print('Going to exceute inserts.')
cur.execute("insert into newtable values( 1, 'Sunday', 101, 201 )")
cur.execute("insert into newtable values( 2, 'Monday', 102, 202 )")
cur.execute("insert into newtable values( 3, 'Tuesday', 103, 203 )")
cur.execute("insert into newtable values( 4, 'Wednesday', 104, 204 )")
cur.execute("insert into newtable values( 5, 'Thursday', 105, 2005 )")
cur.execute("insert into newtable values( 6, 'Friday', 106, 206 )")
cur.execute("insert into newtable values( 7, 'Saturday', 107, 207 )")
conn.commit ()

# Cleanup Existing data in newtable
try:
 print('Going to execute delete newtable1 statement.')
 stmt = cur.execute('delete from newtable1;')
 print ('delete from newtable1 Successful!!')
except:
 print ('delete from newtable1 failed!!')

print('Executing select.')
try:
 from datetime import date
 wk = date.today().__str__()
 cur.execute("INSERT into newtable1 (c0 ,c1 , c2 ,c3 , c4) SELECT  'f',c1, c2, c3 , c4  FROM newtable")
except Exception as e:
  print(e)
conn.commit()
