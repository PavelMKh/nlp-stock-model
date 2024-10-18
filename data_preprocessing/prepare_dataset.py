import pandas as pd
import psycopg2
import re
from datetime import datetime

query_path = 'data_preprocessing/final_dataset_query.sql'

PATTERNS = {
    'MDA': r'ITEM.{,20}MANAGEMENT.{,10}DISCUSSION.{,10}ANALYSIS.{,10}OF.{,10}FINANCIAL(.*?)ITEM.{0,3}\d.{0,4}QUANTITATIVE'
}

def main():
    user, password, host, port, database = get_connection_parameters()
    df = load_data_from_db(user, password, host, port, database, query_path)

    # remove all consecutive spaces and line breaks
    df['full_content'] = df['full_content'].replace(r'\n', '', regex=True).replace(r'\s+', ' ', regex=True).str.lower()

    # extract item features from report 
    df = extract_features(df, PATTERNS)
        
    # save dataset
    df.to_csv(f'data/data{datetime.now():%d-%m-%Y}.csv')



def get_connection_parameters():
    print("Input the data for connection to database:")
    user = input("username >> ")
    password = input("password >> ")
    host = input("host >> ")
    port = input("port >> ")
    database = input("database >> ")
    return user, password, host, port, database

def load_data_from_db(user, password, host, port, database, file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()

    conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
    
    try:
        df = pd.read_sql_query(sql_query, conn)
    finally:
        conn.close()
    
    return df

def extract_matching_text(text, pattern):
    matches = re.findall(pattern, text, re.IGNORECASE)
    if matches:
        return ' '.join(matches)
    return None 

def extract_features(df, patterns):
    for k, v in patterns:
        df[k] = df['full_content'].apply(lambda x: extract_matching_text(x, v))
    return df

if __name__ == "__main__":
    main()