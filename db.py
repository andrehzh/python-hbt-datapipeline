import psycopg2
import logging

# connect to db
con = psycopg2.connect(
    host="localhost",
    database="hachibytokyodb",
    user="postgres",
    password="postgres",
    port="5432"
)

print(con)


# cursor
cur = con.cursor()

create_tables = '''
    CREATE TABLE Client(
        CONTACT_NO INT PRIMARY KEY NOT NULL,
        FIRST_NAME TEXT NOT NULL,
        LAST_NAME TEXT NOT NULL,
        EMAIL TEXT,
        ADDRESS TEXT
    );

    CREATE TABLE Pet(
        ID INT PRIMARY KEY NOT NULL,
        OWNER_CONTACT_NO INT NOT NULL,
        PET_NAME TEXT NOT NULL,
        PET_BREED TEXT NOT NULL
    );
'''

delete_tables = '''
    DROP TABLES IF EXISTS client;
    DROP TABLES IF EXISTS pet;
'''

try:
    cur.execute(delete_tables)
    con.commit()
    logging.info("Table is created")

except Exception as e:
    logging.error("Table is duplicated", e)

finally:
    con.close()
