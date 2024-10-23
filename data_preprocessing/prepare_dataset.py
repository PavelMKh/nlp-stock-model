import pandas as pd
import psycopg2

query_path = 'data_preprocessing/final_dataset_query.sql'


def main():
    user, password, host, port, database = get_connection_parameters()
    df = load_data_from_db(user, password, host, port, database, query_path)

    # save dataset
    df.to_csv(f'data/final_data.csv')


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


if __name__ == "__main__":
    main()