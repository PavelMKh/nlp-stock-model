import pandas as pd
import psycopg2
import re

"""
The code for text preprocessing, including creation and calculation of features, 
is moved to this script in order to improve performance
and not to carry out time-consuming pre-processing before each EDA
"""

query_path = 'data_preprocessing/final_dataset_query.sql'


N_CORES = -1 # -1: all avaliable cores


def main():
    title = "DATASET FOR MODEL LOAD"
    width = 55
    print("=" * width)
    print(title.center(width))
    print("=" * width)
    print("This script allows you to download a dataset ready for training")
    print("=" * width)
    export_method_prompt = (
            "\nSelect one of the download options: \n"
            "1 - Load full report texts \n"
            "2 - Load report with MDA item only \n"
            "3 - Quit the programm \n"
        )
    
    user, password, host, port, database = get_connection_parameters()

    while True:
        print(export_method_prompt)
        opt = int(input(">> "))
        if opt == 1:
            load_full_content(user, password, host, port, database)
        elif opt == 2:
            load_MDA(user, password, host, port, database)
        elif opt == 3:
            print("Thanks for using our app!")
            break
        else:
            print("This method doesn't exist")


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


def load_full_content(user, password, host, port, database):
    df = load_data_from_db(user, password, host, port, database, query_path)
    df['full_content'] = df['full_content'].apply(remove_line_break)
    df = df.dropna(subset=['close_day_before'])
    df = generate_targets(df)
    
    df[df['report_type'] == '10-Q'].to_csv(f'data/full_dataset_10Q.csv')
    df[df['report_type'] == '10-K'].to_csv(f'data/full_dataset_10K.csv')
    print("Dataset has been sucessfully saved")

def load_MDA(user, password, host, port, database):
    df = load_data_from_db(user, password, host, port, database, query_path)
    df['full_content'] = df['full_content'].apply(remove_line_break)
    df = df.dropna(subset=['close_day_before'])
    df = generate_targets(df)
    
    pattern_mda = re.compile(r'ITEM.{,20}MANAGEMENT.{,10}DISCUSSION.{,10}ANALYSIS.{,10}OF.{,10}FINANCIAL(.*?)ITEM.{0,3}\d.{0,4}QUANTITATIVE', re.IGNORECASE)
    df['MDA'] = df['full_content'].apply(lambda x: extract_matching_text(x, pattern_mda))
    df = df.drop('full_content', axis=1)

    df[df['report_type'] == '10-Q'].to_csv(f'data/MDA_dataset_10Q.csv')
    df[df['report_type'] == '10-K'].to_csv(f'data/MDA_dataset_10K.csv')
    print("Dataset has been sucessfully saved")

def remove_line_break(text):
    cleaned_text = re.sub(r'\n', ' ', text).strip()
    return cleaned_text

def extract_matching_text(text, pattern):
    """
    function for extracting text items from report
    """
    matches = pattern.findall(text)
    if matches:
        return ' '.join(matches)
    return None 

def calculate_target(row, period):
    price_diff = row[f'close_day_after_{period}'] / row['close_day_before']
    return 1 if price_diff > 1 else 0

def calculate_target_half_percent(row, period):
    price_diff = row[f'close_day_after_{period}'] / row['close_day_before'] - 1
    return 1 if price_diff > 0.005 else (-1 if price_diff < -0.005 else 0)

def generate_targets(df):
    for _, row in df.iterrows():
        df.at[_, 'target_1'] = calculate_target(row, 1)
        df.at[_, 'target_3'] = calculate_target(row, 3)
        df.at[_, 'target_10'] = calculate_target(row, 10)
        df.at[_, 'target_30'] = calculate_target(row, 30)
        df.at[_, 'target_1_p'] = calculate_target_half_percent(row, 1)
        df.at[_, 'target_3_p'] = calculate_target_half_percent(row, 3)
        df.at[_, 'target_10_p'] = calculate_target_half_percent(row, 10)
        df.at[_, 'target_30_p'] = calculate_target_half_percent(row, 30)
    return df

if __name__ == "__main__":
    main()