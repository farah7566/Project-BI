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
input_file_name = "Product.xlsx"
excel_file_path = f"{directory_path}\\{input_file_name}"

df = pd.read_excel(excel_file_path)

df = df.drop(columns=['updated', 'currentlyactiv'])


df['year'] = pd.to_datetime(df['year'])


df['date'] = df['year'].dt.strftime('%Y-%m-%d')
df['time'] = df['year'].dt.strftime('%H:%M:%S.%f%z')


df = df.drop(columns=['year'])
#null id
null_ids = df[df['id'].isnull()]
if not null_ids.empty:
    print("Null values found in 'id' column:\n", null_ids)
else:
    print("No null values found in 'id' column.")

print("\nCleaned DataFrame:")
print(df)

output_excel_path = r'C:\Users\user\OneDrive\Bureau\webshop-data\ps.xlsx'
df.to_excel(output_excel_path, index=False)


postgres_url = 'postgresql://postgres:farah@127.0.0.1:5432/ETL'
table_name = 'product'

load_data_to_postgres(df, table_name, postgres_url)
