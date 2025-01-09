from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Name": col[1].get_text(strip=True),
                        "MC_USD_Billion": col[2].get_text(strip=True).replace(',', '')}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            
    return df

def transform(df, csv_path):
    read_csv = pd.read_csv(csv_path)
    df['MC_USD_Billion'] = df.iloc[:,1].astype(float)
    exchange_rate_dict = read_csv.set_index('currency')['rate'].to_dict()
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_IDR_Billion'] = [np.round(x*exchange_rate_dict['IDR'],2) for x in df['MC_USD_Billion']]
	
    return df

def load_to_csv(df, output_path):
	df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
	df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
	print(query_statement)
	query_output = pd.read_sql(query_statement, sql_connection)
	print(query_output)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
output_path = 'Largest_banks_data.csv'

# Mengatur Pandas untuk menampilkan semua kolom dan mencegah pemotongan frame
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)  # Atur jumlah baris yang ditampilkan
pd.set_option('display.max_colwidth', None)  # Atur lebar maksimum kolom

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# query_statement = f"SELECT * from {table_name}"
# run_query(query_statement, sql_connection)

# Execute 3 function calls using the queries as mentioned below.
query_statement_1 = f"SELECT * FROM {table_name}"
log_progress('Print the contents of the entire table')
run_query(query_statement_1, sql_connection)

query_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
log_progress('Print the average market capitalization of all the banks in Billion USD')
run_query(query_statement_2, sql_connection)

query_statement_3 = f"SELECT Name FROM {table_name} LIMIT 5"
log_progress('Print only the names of the top 5 banks')
run_query(query_statement_3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()