# Flask app for romi

# Import all libraries
# import main  # chatbot service/functions (robo.py)

import logging
from flask import Flask, request, redirect, Request
import warnings

import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.fernet import Fernet
import json
import sqlite3
from sqlite3 import Error
import datetime
import csv
import json
import os
import pandas as pd
import requests
import settings

BASE_DIR= os.getenv("BASE_DIR")
BASE_BECON_URL= os.getenv("BASE_BECON_URL")
BECON_URL= BASE_BECON_URL + '/api/monitorsave/'
DJ_USERNAME= os.getenv("DJ_USERNAME")
DJ_PASSWORD= os.getenv("DJ_PASSWORD")
ENCRYPTION_KEY= os.getenv("ENCRYPTION_KEY")

warnings.filterwarnings('ignore')  # Ignore warnings

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


# Set up Flask
app = Flask(__name__)
def beconRecord(site):
    data = {'status':1, 'sitecode':site.strip()}
    resp = requests.post(BECON_URL, json=data, auth=(DJ_USERNAME, DJ_PASSWORD))
    if resp.status_code   == 200 or resp.status_code   == 201 :

        print('becon saved successfully',resp)
    else:
        print('becon did not complete or failed',resp)


def saveData(data,sitename,district):
    district = district.strip()
    sitename = sitename.strip()
    reports = json.loads(data.decode())
    for report in reports:
        logging.info('Saving report: %s', report)

        dirpath = os.path.join(BASE_DIR, district, report['report_source'])
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        
        data_filename = f"{sitename} - {report['report_name']}.json" if 'report_name' in report else sitename

        with open(os.path.join(dirpath, data_filename), 'w') as outfile:
            print(json.dumps(report), file=outfile)

def writeCSV(chafile,siteid):
    print('checksiteid:',siteid)
    df=pd.read_csv(chafile)

    for row in df.itertuples():
        print('checkcheck:',row.orgUnit)
        print('original', siteid)
        if row.orgUnit.strip() == siteid.strip():
            print('entered..........................')
            print('hit:',row)
            df.at[row.Index,'Trigger']= 0
            break
    

    #print(df)
    df.to_csv ('SiteCodes2.csv', index = None, header=True)


   


def chacsv(filename):
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            #print(f'\t{row["Trigger"]} works in the {row["SiteName"]} department, and was born in {row["orgUnit"]}.')
            if int(row["Trigger"]) == 1:
                print('send sms')
                row["Trigger"] = 2;
            wr.writerow(row)

            line_count += 1
        #print(f'Processed {line_count} lines.')

def chacsvb(filename,site):
    state = {}
    check = 0

    with open(filename, mode='r') as csv_file:
        print('in here: ',site)
        csv_reader = csv.DictReader(csv_file)
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                #print(f'Column names are {", ".join(row)}')
                line_count += 1
            #print(f'\t{row["Trigger"]} works in the {row["SiteName"]} department, and was born in {row["orgUnit"]}.')
            #print('ROW::',row["orgUnit"] + ' ::'+row["Trigger"])
            if int(row["Trigger"]) == 1 and row["orgUnit"].strip() == site.strip() :
                state = json.dumps({'response':'Yes','quota':row["Quota"],'year':row["Year"]})
                row["Trigger"] = 2;
                check = 1
                print('in the line of duty:',state)
            #wr.writerow(row)
            
            if check == 1:
                break

            line_count += 1
    return state


def decrypt(data,key):
    f = Fernet(key)
    decrypted = f.decrypt(data)
    return decrypted

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
def updatedata(data,conn):

    sql = '''INSERT OR REPLACE INTO sites(name,status_id,updated_date)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, data)
    return cur.lastrowid

@app.route("/")
@app.route("/egpaf")
def root():
    return "Healthy"

@app.route("/update_sites",methods=['GET', 'POST'])
def sites():
    site = request.form['site']
    status = request.form['status']
    database = r"pythonsqlite.db"
    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS sites (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    name text NOT NULL,
                                    status_id integer NOT NULL,
                                    updated_date text NOT NULL
                                );"""
 
    # create a database connection
    conn = create_connection(database)
 
    # create tables
    print("monitor")
    if conn is not None:
        # create tasks table
        data = (site,status,str(datetime.datetime.now()))
        create_table(conn, sql_create_tasks_table)
        print(updatedata(data,conn))
    else:
        print("Error! cannot create the database connection.")

    return "Database Created"

@app.route("/get_site_status",methods=['GET', 'POST'])
def getSiteSatus():
    site = request.form['site']
    database = r"pythonsqlite.db"
    rows = []
    conn = create_connection(database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute("SELECT * FROM sites WHERE name=?", (site,))
     
        rows = cur.fetchall()
     
        for row in rows:
            print(row)

    return str(rows[0]["status_id"])

@app.route("/trigger",methods=['GET', 'POST'])
def trigger():
    print('entered')
    chafile = 'SiteCodes2.csv'
    chacsv(chafile)
    return str('executed')

@app.route("/trigger_per_site",methods=['GET', 'POST'])
def triggerb():
    site = request.args.get('site')
    #print('here is the site: ', site)
    chafile = 'SiteCodes2.csv'

    beconRecord(site)
    state = chacsvb(chafile,site)
    print('here is the state:',state)
    return state

@app.route("/egpaf/sms", methods=['GET', 'POST'])
@app.route("/sms", methods=['GET', 'POST'])
def chat_reply():
    print("request headers:", request.form['Body'])

    message_body = request.form['Body']
    sitecode = request.form['sitecode']
    sitename = request.form['sitename']
    district = request.form['district']
    key =  bytes(ENCRYPTION_KEY, encoding='utf-8')
    string_encrypted = str.encode(message_body)
    print(decrypt(string_encrypted,key))
    saveData(decrypt(string_encrypted,key),sitename,district)
    writeCSV('SiteCodes2.csv',sitecode)

    #message_feedback = 'well executed'
    return str(message_body)


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 2:
        app.run(host='0.0.0.0', port=sys.argv[1], debug=True)
    else:
        app.run(host='0.0.0.0', debug=True)
