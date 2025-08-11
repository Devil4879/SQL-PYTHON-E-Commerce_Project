import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sales'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'delivery'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Jangra@4879',
    database='ecommerce'
)
cursor = conn.cursor()

# ✅ Corrected folder path using raw string
folder_path = r'C:\Users\rahul\OneDrive\Desktop\Project\PYTHON+SQL PROJECT'

# Function to map pandas dtypes to SQL data types
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# Loop through each CSV file and process
for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    print(f"Processing file: {csv_file}")
    
    # Read CSV
    df = pd.read_csv(file_path)

    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Replace NaNs with None for SQL compatibility
    df = df.where(pd.notnull(df), None)

    # Generate CREATE TABLE SQL dynamically
    columns_def = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_sql = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_def})'
    cursor.execute(create_table_sql)

    # Prepare INSERT statement
    col_names = ', '.join([f'`{col}`' for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f'INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})'

    # Insert each row
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(x) else x for x in row)
        cursor.execute(insert_sql, values)

    conn.commit()
    print(f"Inserted data into `{table_name}`\n")

# Close connection
cursor.close()
conn.close()
print("✅ All files imported successfully.")
