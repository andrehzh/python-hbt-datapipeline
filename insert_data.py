import pandas as pd
import clients_etl
import psycopg2
import psycopg2.extras

file_path = "/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/HBT/Data/Clients-HachibyTokyo-2023-05-07-06-38-58.csv"

clients_df, pets_df = clients_etl.process_clients_data(file_path)


def connect_to_db():
    # connect to db
    conn = psycopg2.connect(
        host="localhost",
        database="hachibytokyodb",
        user="postgres",
        password="postgres",
        port="5432"
    )
    print(conn)
    return conn


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS clients CASCADE;
        CREATE TABLE IF NOT EXISTS clients (
            contact_no VARCHAR(255) PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            address TEXT
        )
        """)

        cur.execute("""
        DROP TABLE IF EXISTS pets CASCADE;
        CREATE TABLE IF NOT EXISTS pets (
            id SERIAL PRIMARY KEY,
            owner_contact_no VARCHAR(255),
            pet_name VARCHAR(255),
            pet_breed VARCHAR(255),
            FOREIGN KEY (owner_contact_no) REFERENCES clients (contact_no)
        )
        """)
        conn.commit()


conn = connect_to_db()
create_tables(conn)


def insert_data_to_db(conn, df, table_name):
    tuples = [tuple(x) for x in df.to_records(index=False)]
    cols = ','.join(list(df.columns))

    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_values(cur, query, tuples)
            conn.commit()
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            conn.rollback()


insert_data_to_db(conn, clients_df, 'clients')
insert_data_to_db(conn, pets_df, 'pets')
