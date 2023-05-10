import pandas as pd
import psycopg2
from sqlalchemy import create_engine

engine = create_engine(
    'postgresql://postgres:postgres@localhost:5432/hachibytokyodb')

queries = {
    'clients': "SELECT * FROM clients",
    'pets': "SELECT * FROM pets",
    'appointments': "SELECT * FROM appointments",
    'bank_transactions': "SELECT * FROM bank_transactions",
}

# Create a new Excel writer object
writer = pd.ExcelWriter("output.xlsx", engine='xlsxwriter')

# Execute each query and store the result in a pandas DataFrame
# Then write the DataFrame to a different sheet in the Excel file
for sheet, query in queries.items():
    df = pd.read_sql_query(query, con=engine)
    df.to_excel(writer, sheet_name=sheet, index=False)

# Save the Excel file
writer.save()
