import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

def remove_rows_with_blank_zipcode(dataframe):
    dataframe = dataframe[dataframe['zip'].astype(str).str.strip() != '']
    return dataframe

def load_data_to_postgres(dataframe, table_name, database_url):
    try:
        engine = create_engine(database_url, poolclass=QueuePool)
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Data has been loaded into the table {table_name} in PostgreSQL.')
    except SQLAlchemyError as e:
        print(f'Error: {e}')

directory_path = "C:\\Users\\user\\OneDrive\\Bureau\\webshop-data"
input_file_name = "address.xlsx"

# Read data from Excel
df = pd.read_excel(f"{directory_path}\\{input_file_name}")

# Columns to drop
columns_to_drop = ['updated', 'address2', 'firstname', 'lastname']
df = df.drop(columns=columns_to_drop)

# Drop NaN values
df = df.dropna()

# Clean 'zip' column
df['zip'] = df['zip'].apply(lambda x: ''.join(filter(str.isdigit, str(x))))

# Remove rows with blank zipcode
df = remove_rows_with_blank_zipcode(df)

# Sort DataFrame by 'city'
df = df.sort_values(by='city')

# Convert 'created' to datetime
df['created'] = pd.to_datetime(df['created'])

# Extract 'date' and 'time'
df['date'] = df['created'].dt.strftime('%Y-%m-%d')
df['time'] = df['created'].dt.strftime('%H:%M:%S.%f%z')

# Drop the original 'created' column
df = df.drop(columns=['created'])

# Display the cleaned DataFrame
print("\nCleaned DataFrame:")
print(df)

# Define PostgreSQL connection details
postgres_url = 'postgresql://postgres:farah@127.0.0.1:5432/ETL'
table_name = 'address'  # Replace 'YourTableName' with the desired table name

# Load data into PostgreSQL
load_data_to_postgres(df, table_name, postgres_url)

# Save the cleaned DataFrame to a new Excel file
output_excel_path = r'C:\Users\user\OneDrive\Bureau\webshop-data\addresses_test.xlsx'
df.to_excel(output_excel_path, index=False)

