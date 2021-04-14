#!/usr/bin/env python
# coding: utf-8


import json
import urllib.request
import zipfile
from io import StringIO
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import sys
from slacker import Slacker

slack = Slacker('xoxb-1483962724294-1731766923767-x1YWHXy7Dejdb38yjB5bKTGO')

#Downloading and unziping the file to dataframe
url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'
filehandle, _ = urllib.request.urlretrieve(url)
zip_file_object = zipfile.ZipFile(filehandle, 'r')
first_file = zip_file_object.namelist()[0]
file = zip_file_object.open(first_file)
content = file.read()
s=str(content,'utf-8')
data = StringIO(s) 
df=pd.read_csv(data)

#unpivoting the main table for manipulation
df1=df.set_index('Date').stack().reset_index()
df1=df1.rename(columns={'Date':'date','level_1': 'currency', 0:'exchange_rate'})
df1['date']=df1['date'].apply(pd.to_datetime)
df1['week_nr']=df1['date'].dt.strftime('%Y%U')
df2=df1.groupby(by=["currency","week_nr"]).mean().reset_index()
df2.columns=['currency', 'week_nr', 'weekly_avg']
df3=df1.merge(df2, left_on=['currency','week_nr'], right_on=['currency', 'week_nr'])

#getting the currency name from the currency table in Manager database
with open (r'C:\Users\Simonas\Documents\links.txt', 'r') as f:
    c=json.loads(f.read())
    
hostname = c['parameters'][1]['hostname']
username =c['parameters'][1]['username']
password = c['parameters'][1]['password']
database = c['parameters'][1]['database']

#Connecting to the syno_manager database
conn=mysql.connector.connect( host=hostname, user=username, passwd=password, db=database )

# Simple routine to run a query on a database and print the results:
cur = conn.cursor()
cur.execute("""select * from currency c """)
currency=cur.fetchall()
df_cur=pd.DataFrame(currency)
df_cur=df_cur[[1,4]]
df3=df3.merge(df_cur, left_on="currency", right_on=1)
df3=df3.rename(columns={4:'currency_name'})
df3[['date', 'currency', 'exchange_rate', 'week_nr', 'weekly_avg','currency_name']]


#Getting DB credentials
with open (r'C:\Users\Simonas\Documents\links.txt', 'r') as f:
    c=json.loads(f.read())
    
msql_connector= c['logins'][3]['currencies']
sqlEngine = create_engine(msql_connector, pool_recycle=3600) # creating a connection to the database

dbConnection = sqlEngine.connect()
try:
    frame  = df.to_sql('full_table', dbConnection, if_exists='replace', index=False);
    print("Full table created successfully.")
    frame = df3.to_sql('weekly_avg', dbConnection, if_exists='replace', index=False);
    print("Weekly average table created successfully.")
except ValueError as vx: # error handling
    slack.chat.post_message('#data-set-update-log', vx);
    print(vx)
except Exception as ex:
    slack.chat.post_message('#data-set-update-log', ex);
    print(ex)
else:
    slack.chat.post_message('#data-set-update-log', "Currencies data set updated sucessfully");
    print("Full table created successfully.")   
finally:
    dbConnection.close()

