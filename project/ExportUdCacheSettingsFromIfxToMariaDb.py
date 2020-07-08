import sys
import IfxPyDbi as ifxDb
import mysql.connector as mariaDb
from datetime import date
import json
import smtplib
from email.mime.multipart import MIMEMultipart

def load_application_proprties():
    global properties
    if(len(sys.argv) < 2):
        print('Environment must be specified . ReRun the script with a single command line argument as dev/qa/uat/prod etc.')
        sys.exit()
    print('Active Environment is: '+ sys.argv[1])
    with open('application.properties_'+sys.argv[1]+'.json', "r") as read_file:
        properties = json.load(read_file)
    print('Environment Specific Properties:')
    print(properties)

def connect_to_dadt_informixdb():
    global dadtIfxDbConn, dadtIfxDbCur
    # Establish Database connection with Informix Database.
    try:
        print('Establishing connection with dadt Informix Database..')
        dadtIfxDbConn = ifxDb.connect(properties.get('db.dadt.connection.string'), "", "")
        print('Successfully Connected with dadt Informix Database !!')
    except Exception as e:
        print(e)
        print('Connection failed with dadt Informix Database!!')
        print('Exiting..')
        email_execution_status_as_failure()
        sys.exit()
    # Create Cursor
    print('Creating cursor for dadt Informix Database..')
    dadtIfxDbCur = dadtIfxDbConn.cursor()

def release_dadt_informix_db_connections():
    print('Releasing Informixdb connection..')
    dadtIfxDbCur.close()
    dadtIfxDbConn.close()

def connect_to_stats03_informixdb():
    global stats03IfxDbConn, stats03IfxDbCur
    try:
        print('Establishing connection with Stats03 Informix Database..')
        stats03IfxDbConn = ifxDb.connect(properties.get('db.stats03.connection.string'), "", "")
        print('Successfully Connected with Stats03 Informix Database !!')
    except Exception as e:
        print(e)
        print('Connection failed with Stats03 Informix Database!!')
        release_dadt_informix_db_connections()
        print('Exiting..')
        email_execution_status_as_failure()
        sys.exit()
    # Create Cursor
    print('Creating cursor for Stats03 Informix Database..')
    stats03IfxDbCur = stats03IfxDbConn.cursor()

def release_stats03_informixdb_connections():
    print('Releasing Informixdb connection..')
    stats03IfxDbCur.close()
    stats03IfxDbConn.close()

def connect_to_reports_mariadb():
    global mariadb_connection, mariaCur
    try:
     mariadb_connection = mariaDb.connect(user=properties.get('db.reports.connection.param.user'),
                                             password=properties.get('db.reports.connection.param.password'),
                                             database=properties.get('db.reports.connection.param.database') ,
                                             host=properties.get('db.reports.connection.param.host') ,
                                             port=properties.get('db.reports.connection.param.port'))
     print('Successfully Connected with Mariadb !!')
    except Exception as e:
        print(e)
        print('Connection failed with Mariadb!!')
        release_dadt_informix_db_connections()
        release_stats03_informixdb_connections()
        print('Exiting..')
        email_execution_status_as_failure()
        sys.exit()
    # Create Cursor
    print('Creating cursor for UD Informix Database..')
    mariaCur = mariadb_connection.cursor()

def release_reports_mariadb_connections():
    print('Releasing Maridb connection..')
    mariaCur.close()
    mariadb_connection.close()

def release_connections():
    release_dadt_informix_db_connections()
    release_stats03_informixdb_connections()
    release_reports_mariadb_connections()


def fetch_ud_cache_settings():
    print('Going to execute main Select statement with UD Informix Database.')
    dadtIfxDbCur.execute(properties.get('db.dadt.sql.stmt.fetch_ud_cache_settings'))
    rows = dadtIfxDbCur.fetchall()
    print('Fetch Successful!!')
    return rows

def fetch_master_brands():
    print('Going to execute fetch_master_brands statement with stats03 Informix Database.')
    stats03IfxDbCur.execute(properties.get('db.stats03.sql.stmt.fetch_master_brands'))
    rows = stats03IfxDbCur.fetchall()
    print('Master Brands Fetch Successful!!')
    masterBrands = {}
    for row in rows:
        masterBrands.setdefault(row[0], row)
    return masterBrands

def join_ud_cache_settings_and_master_brands(settings, master_brands):
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
        mariaCur.executemany(properties.get('db.reports.sql.stmt.insert_ud_cache_settings'), settings)
        mariadb_connection.commit()
        print('Data inserted Successfully in Mariadb!!')
        email_execution_status_as_success()
    except Exception as e:
        print('Exception while inserting into mariadb')
        print(e)
        print('Rolling back transaction!!')
        mariadb_connection.rollback()
        email_execution_status_as_failure()


def email_execution_status_as_success():
    email_execution_status(properties.get('email.subject.success'))

def email_execution_status_as_failure():
    email_execution_status(properties.get('email.subject.failure'))


def email_execution_status(subject):
    try:
        smtp_server = properties.get('smtp.server.host')
        port = properties.get('smtp.server.port')
        sender_email = properties.get('email.sender')
        receiver_email_ids = properties.get('email.receiver').split(',')

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = (', ').join(receiver_email_ids)
        message["Subject"] = subject
        text = message.as_string()

        print('Going to send email.')
        with smtplib.SMTP(smtp_server, port) as server:
            server.sendmail(sender_email, receiver_email_ids, text)
        print('Email Sent Successfully!!')
    except Exception as e:
        print(e)
        print('Failed to send Email!!')


def main_procedure():
    load_application_proprties()
    connect_to_dadt_informixdb()
    connect_to_stats03_informixdb()
    connect_to_reports_mariadb()
    try:
        cache_settings = fetch_ud_cache_settings()
        brands = fetch_master_brands()
        insert_into_mariadb(join_ud_cache_settings_and_master_brands(cache_settings, brands))
    except Exception as e:
        print(e)
        print('Fetch failed!!')
        email_execution_status_as_failure()
    finally:
        release_connections()

main_procedure()

