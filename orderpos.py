import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

def extract_data(directory_path, file_name):
    local_file_path = f"{directory_path}\\{file_name}"
    data = pd.read_excel(local_file_path)
    return data

def load_data_to_postgres(dataframe, table_name, database_url):
    try:
        engine = create_engine(database_url, poolclass=QueuePool)
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Data has been loaded into the table {table_name} in PostgreSQL.')
    except SQLAlchemyError as e:
        print(f'Error: {e}')

directory_path = "C:\\Users\\user\\OneDrive\\Bureau\\webshop-data"
input_file_name = "orderposition.xlsx"

df = extract_data(directory_path, input_file_name)


df = df.drop(columns=['updated'])


df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)


null_values = df.isnull().sum()


df['created'] = pd.to_datetime(df['created'])


df['date'] = df['created'].dt.strftime('%Y-%m-%d')
df['time'] = df['created'].dt.strftime('%H:%M:%S.%f%z')

df = df.drop(columns=['created'])


df.to_excel(directory_path + "\\neworderpos.xlsx", index=False)


postgres_url = 'postgresql://postgres:farah@127.0.0.1:5432/ETL'
table_name = 'orderposition'


load_data_to_postgres(df, table_name, postgres_url)


