import getpass
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql, Error, extras

LIST_OF_TICKERS_1 = ['AAPL','META', 'NVDA', 'AMD', 'GOOG', 'XOM', 'MCD', 'KO', 'PFE', 'PG']

LIST_OF_TICKERS_2 = ['MSFT', 'AMZN', 'TSLA', 'GOOGL', 'JPM', 'JNJ', 'V', 'CMCSA', 'PEP', 'T', 'CSCO', 'DIS', 'NKE', 'VZ', 'HD', 'UNH', 'CRM', 'NFLX', 'INTC', 
                     'BA', 'MRK', 'LMT', 'GILD', 'SBUX']

LIST_OF_TICKERS_3 = ['AMGN', 'IBM', 'HON', 'PYPL', 'LRCX', 'MDLZ', 'BKNG', 'FDX', 'EOG', 'CIM', 'DVN', 'MPC', 'CHK', 
                     'RRC', 'NOG', 'ADBE', 'CAT', 'QCOM']

LIST_OF_TICKERS_5 = ["VRTX", "ETN", "BSX", "MDT", "ADI", "ANET", "PANW", "ADP", "KLAC", "BUD", "DE", "MELI", 
                      "SHOP", "FI", "BMY", "SO", "SHW", "DUK", "MAR", "DHI", "MRVL", "APD"]

LIST_OF_TICKERS_6 = ["CL", "WM", "SNPS", "SCCO", "ZTS", "APH", "DELL", "CTAS", "PH", "CMG", "ITW", "TGT", "MSI", "MCK", 
                     "ECL", "CSX", "CRWD", "ORLY", "CARR", "FCX", "SLB", "EPD", "CEG"]

LIST_OF_TICKERS_7 = ["NEM", "GM", "F", "NSC", "HLT", "AZO", "DASH", "AEP", "SRE", "LEN", "PCAR", "GEV", "CHTR", "WCN", "GWW", 
                      "D", "KMI", "PAYX", "STZ", "ODFL", "KHC", "FERG"]

CREATE_TABLE_SQL = """create table if not exists companies (
                    ticker VARCHAR(30) PRIMARY KEY NOT NULL,
                    AssetType VARCHAR(50) NOT NULL,
                    Name VARCHAR(50) NOT NULL,
                    Description TEXT NOT NULL,
                    CIK VARCHAR(20) NOT NULL,
                    Exchange VARCHAR(50) NOT NULL,
                    Currency VARCHAR(10) NOT NULL,
                    Country VARCHAR(50) NOT NULL,
                    Sector VARCHAR(200) NOT NULL,
                    Industry VARCHAR(200) NOT NULL,
                    Address VARCHAR(200) NOT NULL,
                    FiscalYearEnd VARCHAR(20) NOT NULL
                    )"""

INSERT_SQL = """insert into companies
                  (ticker, AssetType, Name, Description, CIK, Exchange, Currency, Country, Sector, Industry, Address, FiscalYearEnd)
                values %s
                on conflict (ticker) do nothing"""

def main():
    title = "COMPANY OVERVIEW LOADER"
    width = 55
    print("=" * width)
    print(title.center(width))
    print("=" * width)
    print("This script allows you to fetch company overview via API,")
    print("either in CSV format or by saving it to a database.")
    print("=" * width)
    api_key = getpass.getpass("Enter your API key >> ")
    export_method_prompt = (
            "\nEnter the export method for companies overview: \n"
            "1 - to a .csv file \n"
            "2 - to a PostgreSQL database \n"
            "3 - quit the programm \n"
        )
    print(export_method_prompt)
    while True:
        export_method = int(input("Enter your command >> "))
        if export_method == 1:
            companies_to_csv(api_key, LIST_OF_TICKERS_8)
        elif export_method == 2:
            companies_to_db(api_key, LIST_OF_TICKERS_8)
        elif export_method == 3:
            print("Thanks for using our app!")
            break
        else:
            print("This method doesn't exist")


def companies_to_csv(api_key, tickers):
    result = get_companies_df(api_key, tickers)
    result.to_csv(f'companies_{datetime.now():%d-%m-%Y}.csv', index=False)
    print(f"Companies data have saved in the file companies_{datetime.now():%d-%m-%Y}.csv")

def companies_to_db(api_key, tickers):
    user, password, host, port, database = get_connection_parameters()
    create_table(user, password, host, port, database, CREATE_TABLE_SQL)
    for ticker in tickers:
       try:
        result = get_companies_df(api_key, ticker)
        save_to_database(user, password, host, port, database, result, INSERT_SQL)
       except Exception as e:
        print(f"Error with ticker processing {ticker}: {e}")

def get_companies_df(api_key, ticker):
    result = pd.DataFrame()
    json_data = request_company(api_key, ticker)
    df  = pd.DataFrame.from_dict(json_data, orient='index').T
    df.rename(columns={
        'Symbol':'ticker'}, inplace=True)
    df['CIK'] = df['CIK'].astype(str).str.zfill(10)
    df.reset_index(inplace=True)
    df_res = df[['ticker', 'AssetType', 'Name', 'Description',
                   'CIK', 'Exchange', 'Currency', 'Country',
                   'Sector', 'Industry', 'Address', 'FiscalYearEnd']]
    result = pd.concat([result, df_res], ignore_index=True)
    return result

def request_company(api_key, ticker):
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    return json_data

def create_table(user, password, host, port, database, sql_query):
    cursor = None
    conn = None
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
      print(f"Error while working woth PostgreSQL: {error}")
      if conn:
        conn.rollback()
        print("Transaction rolled back due to an error.")
    finally:
      if cursor is not None:  
        cursor.close()
      if conn is not None:
        conn.close()

def save_to_database(user, password, host, port, database, df, sql):
    cursor = None
    conn = None
    try:
      conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
      cursor = conn.cursor()
      extras.execute_values(cursor, sql, df[['ticker', 'AssetType', 'Name', 'Description',
                                'CIK', 'Exchange', 'Currency', 'Country',
                                'Sector', 'Industry', 'Address', 'FiscalYearEnd']].values)
      conn.commit()
      print("Company overview successfully saved to database")
    except Exception as error:
      print(f"Error while saving data to database: {error}")
      if conn:
        conn.rollback()
        print("Transaction rolled back due to an error")
    finally:
      if cursor:
        cursor.close()
      if conn:
        conn.close()

def get_connection_parameters():
    print("Input the data for connection to database:")
    user = input("username >> ")
    password = input("password >> ")
    host = input("host >> ")
    port = input("port >> ")
    database = input("database >> ")
    return user, password, host, port, database


if __name__ == "__main__":
    main()