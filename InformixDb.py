import IfxPyDbi as dbapi2
import smtplib
import csv

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

print('Creating CSV file with data.')
print('Executing select.')
cur.execute("SELECT c1 as ID, c2 as Day, c3 , c4  FROM newtable")
rows = cur.fetchall()
column_names = [i[0] for i in cur.description]
fp = open('informixdb.csv', 'w')
myFile = csv.writer(fp)
myFile.writerow(column_names)
myFile.writerows(rows)
fp.close()

# import the corresponding modules
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

port = 2525
smtp_server = "smtp.ad.dhisco.com"
login = "1a2b3c4d5e6f7g" # paste your login generated by Mailtrap
password = "1a2b3c4d5e6f7g" # paste your password generated by Mailtrap

subject = "An example of boarding pass"
sender_email = "vaneet.kataria@rategain.com"
receiver_email = "vaneetkataria54@gmail.com"

message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = subject

# Add body to email
body = "Please Find Attached the UD Cache Settings Report."
message.attach(MIMEText(body, "plain"))

filename = "informixdb.csv"
# Open PDF file in binary mode

# We assume that the file is in the directory where you run your Python script from
with open(filename, "rb") as attachment:
    # The content type "application/octet-stream" means that a MIME attachment is a binary file
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())

# Encode to base64
encoders.encode_base64(part)

# Add header
part.add_header(
    "Content-Disposition",
    f"attachment; filename= {filename}",
)

# Add attachment to your message and convert it to string
message.attach(part)
text = message.as_string()

# send your email
print('Going to send email.')
with smtplib.SMTP(smtp_server, 25) as server:
    server.sendmail(
        sender_email, receiver_email, text
    )
print('Sent!!')

