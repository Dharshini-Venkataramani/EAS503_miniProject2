import sqlite3
import psycopg2
import pandas as pd
from utils import get_db_url
import time
import os

SQLITE_DB_FILE = "normalized_mini_project.db"  
DATABASE_URL = get_db_url()                    
CSV_DIR = "/tmp"

TABLES_ORDER = [
    "region",
    "country",
    "customer",
    "productcategory",
    "product",
    "orderdetail"
]

TABLES_TRUNCATE = list(reversed(TABLES_ORDER))

CREATE_TABLE_SQL = {
    "region": """
        CREATE TABLE IF NOT EXISTS region (
            regionid SERIAL PRIMARY KEY,
            region TEXT NOT NULL
        )
    """,
    "country": """
        CREATE TABLE IF NOT EXISTS country (
            countryid SERIAL PRIMARY KEY,
            country TEXT NOT NULL,
            regionid INTEGER NOT NULL REFERENCES region(regionid)
        )
    """,
    "productcategory": """
        CREATE TABLE IF NOT EXISTS productcategory (
            productcategoryid SERIAL PRIMARY KEY,
            productcategory TEXT NOT NULL,
            productcategorydescription TEXT NOT NULL
        )
    """,
    "product": """
        CREATE TABLE IF NOT EXISTS product (
            productid SERIAL PRIMARY KEY,
            productname TEXT NOT NULL,
            productunitprice REAL NOT NULL,
            productcategoryid INTEGER NOT NULL REFERENCES productcategory(productcategoryid)
        )
    """,
    "customer": """
        CREATE TABLE IF NOT EXISTS customer (
            customerid SERIAL PRIMARY KEY,
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            countryid INTEGER NOT NULL REFERENCES country(countryid)
        )
    """,
    "orderdetail": """
        CREATE TABLE IF NOT EXISTS orderdetail (
            orderid SERIAL PRIMARY KEY,
            customerid INTEGER NOT NULL REFERENCES customer(customerid),
            productid INTEGER NOT NULL REFERENCES product(productid),
            orderdate DATE NOT NULL,
            quantityordered INTEGER NOT NULL
        )
    """
}


def rename_columns_to_lowercase(pg_conn):
    cursor = pg_conn.cursor()
    
    # Region
    cursor.execute('ALTER TABLE region RENAME COLUMN "RegionID" TO regionid;')
    cursor.execute('ALTER TABLE region RENAME COLUMN "Region" TO region;')
    
    # Country
    cursor.execute('ALTER TABLE country RENAME COLUMN "CountryID" TO countryid;')
    cursor.execute('ALTER TABLE country RENAME COLUMN "Country" TO country;')
    cursor.execute('ALTER TABLE country RENAME COLUMN "RegionID" TO regionid;')
    
    # Customer
    cursor.execute('ALTER TABLE customer RENAME COLUMN "CustomerID" TO customerid;')
    cursor.execute('ALTER TABLE customer RENAME COLUMN "FirstName" TO firstname;')
    cursor.execute('ALTER TABLE customer RENAME COLUMN "LastName" TO lastname;')
    cursor.execute('ALTER TABLE customer RENAME COLUMN "Address" TO address;')
    cursor.execute('ALTER TABLE customer RENAME COLUMN "City" TO city;')
    cursor.execute('ALTER TABLE customer RENAME COLUMN "CountryID" TO countryid;')
    
    # ProductCategory
    cursor.execute('ALTER TABLE productcategory RENAME COLUMN "ProductCategoryID" TO productcategoryid;')
    cursor.execute('ALTER TABLE productcategory RENAME COLUMN "ProductCategory" TO productcategory;')
    cursor.execute('ALTER TABLE productcategory RENAME COLUMN "ProductCategoryDescription" TO productcategorydescription;')
    
    # Product
    cursor.execute('ALTER TABLE product RENAME COLUMN "ProductID" TO productid;')
    cursor.execute('ALTER TABLE product RENAME COLUMN "ProductName" TO productname;')
    cursor.execute('ALTER TABLE product RENAME COLUMN "ProductUnitPrice" TO productunitprice;')
    cursor.execute('ALTER TABLE product RENAME COLUMN "ProductCategoryID" TO productcategoryid;')
    
    # OrderDetail
    cursor.execute('ALTER TABLE orderdetail RENAME COLUMN "OrderID" TO orderid;')
    cursor.execute('ALTER TABLE orderdetail RENAME COLUMN "CustomerID" TO customerid;')
    cursor.execute('ALTER TABLE orderdetail RENAME COLUMN "ProductID" TO productid;')
    cursor.execute('ALTER TABLE orderdetail RENAME COLUMN "OrderDate" TO orderdate;')
    cursor.execute('ALTER TABLE orderdetail RENAME COLUMN "QuantityOrdered" TO quantityordered;')

    pg_conn.commit()
    cursor.close()
    print("All columns renamed to lowercase.")

def migrate_small_table(sqlite_conn, pg_conn, table_name):
    df = pd.read_sql(f'SELECT * FROM {table_name}', sqlite_conn)
    df = df.where(pd.notnull(df), None) 
    df.columns = [col.strip().lower() for col in df.columns] 

    columns = [f'{col}' for col in df.columns]     
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'

    cursor = pg_conn.cursor()
    if not df.empty:
        cursor.executemany(sql, df.values.tolist())
        pg_conn.commit()
        print(f"Inserted {len(df)} rows into {table_name}")
    else:
        print(f"No data in {table_name}, skipped insert")
    cursor.close()

def migrate_large_table(sqlite_conn, pg_conn, table_name):
    df = pd.read_sql(f'SELECT * FROM {table_name}', sqlite_conn)
    df = df.where(pd.notnull(df), None)
    df.columns = [col.strip().lower() for col in df.columns]
    csv_file = os.path.join(CSV_DIR, f"{table_name}.csv")
    df.to_csv(csv_file, index=False)
    print(f"Exported {len(df)} rows of {table_name} to CSV for COPY")

    cursor = pg_conn.cursor()
    with open(csv_file, "r") as f:
        cursor.copy_expert(f'COPY {table_name} FROM STDIN WITH CSV HEADER', f)
    pg_conn.commit()
    cursor.close()
    print(f" Bulk loaded {len(df)} rows into {table_name}")

def truncate_table(TABLES_TRUNCATE):
    print("Truncating all tables (CASCADE) to clear existing data...")
    cursor = pg_conn.cursor()
    for t in TABLES_TRUNCATE:
        cursor.execute(f'TRUNCATE TABLE {t} CASCADE;')
    pg_conn.commit()
    cursor.close()
    print(" All tables cleared\n")

if __name__ == "__main__":                  
    sqlite_conn = sqlite3.connect(SQLITE_DB_FILE)
    pg_conn = psycopg2.connect(DATABASE_URL)

    # print("Renaming tables to lowercase (if not already)...")
    # cursor = pg_conn.cursor()
    # rename_sql = [
    #     'ALTER TABLE IF EXISTS "Region" RENAME TO region;',
    #     'ALTER TABLE IF EXISTS "Country" RENAME TO country;',
    #     'ALTER TABLE IF EXISTS "Customer" RENAME TO customer;',
    #     'ALTER TABLE IF EXISTS "ProductCategory" RENAME TO productcategory;',
    #     'ALTER TABLE IF EXISTS "Product" RENAME TO product;',
    #     'ALTER TABLE IF EXISTS "OrderDetail" RENAME TO orderdetail;'
    # ]
    # for statement in rename_sql:
    #     try:
    #         cursor.execute(statement)
    #         print("Executed:", statement)
    #     except Exception as e:
    #         print("Skipped:", statement, "| Reason:", e)
    # pg_conn.commit()
    # cursor.close()
    # print(" Renaming complete.\n")

    # rename_columns_to_lowercase(pg_conn)

    #truncate_table(TABLES_TRUNCATE)

    start_time = time.monotonic()

    cursor = pg_conn.cursor()
    for table in TABLES_ORDER:
        cursor.execute(CREATE_TABLE_SQL[table])
    pg_conn.commit()
    cursor.close()
    print("All tables created in lowercase.")


    print("Migrating small tables...")
    for table in TABLES_ORDER[:-1]:  # all except OrderDetail
        migrate_small_table(sqlite_conn, pg_conn, table)

    print("\nMigrating large table OrderDetail...")
    migrate_large_table(sqlite_conn, pg_conn, TABLES_ORDER[-1])

    sqlite_conn.close()
    pg_conn.close()

    end_time = time.monotonic()
    print(f"\n Migration complete! Total time: {end_time - start_time:.2f} seconds")

