import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=["Name","MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_csv="Largest_banks_data.csv"
sql_connection = sqlite3.connect(db_name)
csv_path='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'



#task2
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :  # Condition (a) and (b)
                bank_name=col[1].find_all('a')[1]['title']
                data_dict = {table_attribs[0]: bank_name,
                             table_attribs[1]: col[2].contents[0][:-1]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df



def transform(df,csv_path):
    exchange_rate=pd.read_csv(csv_path)
    exchange_rate=exchange_rate.set_index('Currency').to_dict()['Rate']
    exchange_rate = {currency: float(rate) for currency, rate in exchange_rate.items()}
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_csv):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
#task1

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')



log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
df1=transform(df,csv_path)


load_to_csv(df1,output_csv)
load_to_db(df1,sql_connection,table_name)
query_statement1=f"SELECT * FROM Largest_banks"
query_statement2=f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement3=f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement1,sql_connection)
run_query(query_statement2,sql_connection)

run_query(query_statement3,sql_connection)

log_progress('Data extraction complete. Initiating Transformation process')
