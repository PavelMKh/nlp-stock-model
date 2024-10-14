import os
import pandas as pd
import psycopg2
from datetime import datetime
from psycopg2 import sql, Error, extras
import logging

ROOT_DIRECTORY = '/Users/pavel/Documents/SEC_DATA/'

LIST_OF_TICKERS_1 = ['AAPL', 'META', 'NVDA', 'AMD', 'GOOG', 'XOM', 'MCD', 'KO', 'PFE', 'PG']

LIST_OF_TICKERS_2 = ['MSFT', 'AMZN', 'TSLA', 'GOOGL', 'JPM', 'JNJ', 'V', 'UNH', 'HD', 'DIS', 'NFLX', 'VZ', 'INTC', 'CMCSA', 'PEP', 'T', 'MRK', 'CSCO']

LIST_OF_TICKERS_3 = ['DIS', 'TSLA', 'NKE', 'AMZN', 'VZ', 'JNJ', 'HD', 'PFE', 'UNH', 'T', 'CRM', 'NFLX', 'KO', 'INTC', 
                     'CSCO', 'XOM', 'BA', 'MRK', 'LMT', 'AMD', 'GILD', 'SBUX', 'ADBE', 'CAT', 'QCOM']

LIST_OF_TICKERS_4 = ['AMGN', 'IBM', 'HON', 'PYPL','LRCX','ADP', 'TMO','MDLZ', 'CVS','SPGI', 'FISV','ZTS',
                     'CARR','BKNG','SYK','CCL', 'DHR','MDT', 'SYY','AON', 'TROW','RMD', 'DOV','ECL', 'FDX']

LIST_OF_TICKERS_5 = ['EOG', 'CIM', 'PXD', 'NBL', 'DVN', 'MPC', 'CHK', 'CDEV', 'RRC', 'NOG']

def main():
    logging.basicConfig(level=logging.INFO)
    title = "COMPANY 10-Q, 10-K REPORTS LOADER"
    width = 55
    print("=" * width)
    print(title.center(width))
    print("=" * width)
    print("This script allows you to load data from reports in TXT format,")
    print("either in CSV format or by saving it to a PostgreSQL database.")
    print("=" * width)
    export_method_prompt = (
            "\nEnter the export method for reports: \n"
            "1 - to a .csv file \n"
            "2 - to a PostgreSQL database \n"
            "3 - quit the programm \n"
        )
    print(export_method_prompt)
    while True:
        export_method = int(input("Enter your command >> "))
        if export_method == 1:
            reports_to_csv(LIST_OF_TICKERS_3)
        elif export_method == 2:
            reports_to_db(LIST_OF_TICKERS_3)
        elif export_method == 3:
            print("Thanks for using our app!")
            break
        else:
            print("This method doesn't exist")

def reports_to_csv(tickers):
   user, password, host, port, database = get_connection_parameters()
   cik_dict = get_cik_dict(user, password, host, port, database, tickers)
   result = get_reports_df(cik_dict, ROOT_DIRECTORY)
   result.to_csv(f'reports_{datetime.now():%d-%m-%Y}.csv', index=False)

def reports_to_db(tickers):
    user, password, host, port, database = get_connection_parameters()
    cik_dict = get_cik_dict(user, password, host, port, database, tickers)
    result = get_reports_df(cik_dict, ROOT_DIRECTORY)
    create_table(user, password, host, port, database)
    save_to_db(user, password, host, port, database, result)

def get_reports_df(cik_dict, root_directory):
    result = pd.DataFrame()
    
    for cik, ticker in cik_dict.items():
        extracted_data = []
        
        for dirpath, dirnames, filenames in os.walk(root_directory):
            for filename in filenames:
                if cik in filename and filename.endswith('.txt'):  # Use CIK to match filenames
                    file_path = os.path.join(dirpath, filename)
                    
                    with open(file_path, 'r') as file:
                        content = file.read()
                        file_data = {
                            'ticker': ticker, 
                            'report_type': None,
                            'publication_date': None,
                            'report_date': None,
                            'full_content': content
                        }

                        for line in content.splitlines():
                            if "CONFORMED SUBMISSION TYPE:" in line:
                                file_data['report_type'] = line.split(':')[1].strip()
                            elif "FILED AS OF DATE:" in line:
                                date_str = line.split(':')[1].strip()
                                # Convert to datetime format
                                file_data['publication_date'] = pd.to_datetime(date_str, format='%Y%m%d', errors='coerce')
                            elif "CONFORMED PERIOD OF REPORT:" in line:
                                date_str = line.split(':')[1].strip()
                                # Convert to datetime format
                                file_data['report_date'] = pd.to_datetime(date_str, format='%Y%m%d', errors='coerce')
                        
                        extracted_data.append(file_data)

        if extracted_data:
            df = pd.DataFrame(extracted_data)
            result = pd.concat([result, df], ignore_index=True)
    
    return result              
   

def get_cik_dict(user, password, host, port, database, tickers):
    cursor = None
    conn = None
    cik_query = """SELECT CIK, ticker
                   FROM companies
                   WHERE ticker IN %s"""
    tickers_tuple = tuple(tickers) if isinstance(tickers, (list, tuple)) else (tickers,)
    
    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = conn.cursor()
        cursor.execute(cik_query, (tickers_tuple,))
        result = cursor.fetchall()
        if not result:
           logging.warning("No results found for the given tickers.")
           return {}
        # Create a dictionary from the fetched results
        cik_dict = {cik: ticker for cik, ticker in result}
        return cik_dict
    
    except (Exception, Error) as error:
        logging.error(f"Error while working with PostgreSQL: {error}")
        if conn:
            conn.rollback()
            logging.info("Transaction rolled back due to an error.")
    
    finally:
        if cursor is not None:  
            cursor.close()
        if conn is not None:
            conn.close()

def get_connection_parameters():
    print("Input the data for connection to database:")
    user = input("username >> ")
    password = input("password >> ")
    host = input("host >> ")
    port = input("port >> ")
    database = input("database >> ")
    return user, password, host, port, database

def create_table(user, password, host, port, database):
    cursor = None
    conn = None
    sql_query = """create table if not exists reports (
                    id bigint generated by default as identity primary key,
                    ticker varchar(20) not null,
                    report_type varchar(10) not null,
                    publication_date date not null,
                    report_date date not null,
                    full_content text,
                    constraint unique_report unique (ticker, report_type, publication_date, report_date)
                    )"""
    try:
      conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
      cursor = conn.cursor()
      cursor.execute(sql_query)
      conn.commit()
    except (Exception, Error) as error:
      logging.error(f"Error while working woth PostgreSQL: {error}")
      if conn:
        conn.rollback()
        logging.info("Transaction rolled back due to an error.")
    finally:
      if cursor:
        cursor.close()
      if conn:
        conn.close()

def save_to_db(user, password, host, port, database, df):
    cursor = None
    conn = None
    sql_query = """insert into reports
                  (ticker, report_type, publication_date, report_date, full_content)
                values %s
                on conflict (ticker, report_type, publication_date, report_date) do nothing"""
    try:
      conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
      cursor = conn.cursor()
      extras.execute_values(cursor, sql_query, df[['ticker', 'report_type', 'publication_date', 
                                'report_date', 'full_content']].values)
      conn.commit()
      logging.info('Reports have saved to database')
    except (Exception, Error) as error:
      logging.error(f"Error while working with PostgreSQL: {error}")
      if conn:
        conn.rollback()
        logging.info("Transaction rolled back due to an error.")
    finally:
      if cursor:
        cursor.close()
      if conn:
        conn.close()


if __name__ == "__main__":
    main()