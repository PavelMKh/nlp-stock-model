import pandas as pd
import psycopg2
from joblib import Parallel, delayed
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from joblib import Parallel, delayed
from nltk.stem import WordNetLemmatizer

"""
The code for text preprocessing, including creation and calculation of features, 
is moved to this script in order to improve performance
and not to carry out time-consuming pre-processing before each EDA
"""

query_path = 'data_preprocessing/final_dataset_query.sql'

N_CORES = -1 # -1: all avaliable cores

pattern = re.compile('[^a-zA-Z0-9-]+')


def main():
    # nltk.download('stopworsds')
    user, password, host, port, database = get_connection_parameters()
    df = load_data_from_db(user, password, host, port, database, query_path)

    stop_words = set(stopwords.words('english'))
    pattern = re.compile('[^a-zA-Z0-9-]+')
    lemmatizer = WordNetLemmatizer()

    df = df.dropna(subset=['close_day_before'])

    df['full_content'] = Parallel(n_jobs=N_CORES)(delayed(clean_text)(text, pattern, stop_words) for text in df['full_content'])
    df['full_content'] = Parallel(n_jobs=N_CORES)(delayed(split_words)(text) for text in df['full_content'])
    df['full_content'] = Parallel(n_jobs=N_CORES)(delayed(lemmatize_words)(words, lemmatizer) for words in df['full_content'])
    df['full_content_length'] = Parallel(n_jobs=N_CORES)(delayed(len)(x) for x in df['full_content'])

    # # Extracting Management discussion and analysis and length calculation

    # pattern_mda = re.compile(r'ITEM.{,20}MANAGEMENT.{,10}DISCUSSION.{,10}ANALYSIS.{,10}OF.{,10}FINANCIAL(.*?)ITEM.{0,3}\d.{0,4}QUANTITATIVE', re.IGNORECASE)
    # df['MDA'] = Parallel(n_jobs=N_CORES)(delayed(extract_matching_text)(x, pattern_mda) for x in df['full_content'])
    # df['MDA_lenght'] = df['MDA'].apply(lambda x: None if pd.isna(x) else len(list(x.split())))


    # # Extracting Quantitative and Qualitative Disclosures About Market Risk and length calculation

    # pattern_market = re.compile(r'ITEM.{,20}QUANTITATIVE.{,10}AND.{,10}QUALITATIVE.{,10}DISCLOSURES.{,10}ABOUT(.*?)ITEM.{0,3}\d.{0,4}CONTROLS', re.IGNORECASE)
    # df['MARKET_RISK'] = Parallel(n_jobs=N_CORES)(delayed(extract_matching_text)(x, pattern_market) for x in df['full_content'])
    # df['MARKET_RISK_length'] = df['MARKET_RISK'].apply(lambda x: None if pd.isna(x) else len(list(x.split())))


    # # Extracting Risk Factors and length calculation

    # pattern_risks = re.compile(r'ITEM.{,20}RISK.{,10}FACTORS.{,10}(.*?)ITEM.{0,3}\d.{0,4}UNREGISTERED', re.IGNORECASE)
    # df['RISK_FACTORS'] = Parallel(n_jobs=N_CORES)(delayed(extract_matching_text)(x, pattern_risks) for x in df['full_content'])
    # df['RISK_FACTORS_length'] = df['RISK_FACTORS'].apply(lambda x: None if pd.isna(x) else len(list(x.split())))

    # Target generation

    df = generate_targets(df)

    # save dataset
    df.to_csv(f'data/final_data.csv')

    print("Dataset has been sucessfully saved")


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

def clean_text(text, pattern, stop_words):
    # cleaned_text = re.sub(r'\s+', ' ', re.sub(r'\n', ' ', text)).strip()
    text = re.sub(r'\n', ' ', text).strip()
    text = text.lower()
    text = pattern.sub(' ', text)
    text = " ".join([word for word in text.split() if word not in stop_words])
    return text

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
    # return 1 if price_diff > 1 else (-1 if price_diff < 1 else 0)

def lemmatize_words(word_list, lemmatizer):
    return [lemmatizer.lemmatize(word) for word in word_list]

def calculate_target_half_percent(row, period):
    price_diff = row[f'close_day_after_{period}'] / row['close_day_before'] - 1
    return 1 if price_diff > 0.005 else (-1 if price_diff < -0.005 else 0)

def generate_targets(df):
    df['target_1'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target)(row, 1) for _, row in df.iterrows())
    df['target_3'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target)(row, 3) for _, row in df.iterrows())
    df['target_10'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target)(row, 10) for _, row in df.iterrows())
    df['target_30'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target)(row, 30) for _, row in df.iterrows())
    df['target_1_p'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target_half_percent)(row, 1) for _, row in df.iterrows())
    df['target_3_p'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target_half_percent)(row, 3) for _, row in df.iterrows())
    df['target_10_p'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target_half_percent)(row, 10) for _, row in df.iterrows())
    df['target_30_p'] = Parallel(n_jobs=N_CORES)(delayed(calculate_target_half_percent)(row, 30) for _, row in df.iterrows())
    return df


def count_words(text):
    return len(list(text.split()))

def split_words(text):
    return text.split()

if __name__ == "__main__":
    main()