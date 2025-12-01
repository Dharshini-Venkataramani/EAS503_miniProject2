import sqlite3
import pandas as pd
import psycopg2
import os
from utils import get_db_url
import time

SQLITE_DB_FILE = "normalized_mini_project.db"  
DATABASE_URL = get_db_url()                    
CSV_DIR = "/tmp"                               

TABLES_ORDER = [
    "Region",
    "Country",
    "Customer",
    "ProductCategory",
    "Product",
    "OrderDetail"
]

# Reverse order for truncation
TABLES_TRUNCATE = list(reversed(TABLES_ORDER))

def migrate_small_table(sqlite_conn, pg_conn, table_name):
    df = pd.read_sql(f'SELECT * FROM {table_name}', sqlite_conn)
    df = df.where(pd.notnull(df), None) 
    df.columns = [col.strip() for col in df.columns] 

    columns = [f'"{col}"' for col in df.columns]     
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
    csv_file = os.path.join(CSV_DIR, f"{table_name}.csv")
    df.to_csv(csv_file, index=False)
    print(f"Exported {len(df)} rows of {table_name} to CSV for COPY")

    cursor = pg_conn.cursor()
    with open(csv_file, "r") as f:
        cursor.copy_expert(f'COPY {table_name} FROM STDIN WITH CSV HEADER', f)
    pg_conn.commit()
    cursor.close()
    print(f" Bulk loaded {len(df)} rows into {table_name}")

if __name__ == "__main__":
    sqlite_conn = sqlite3.connect(SQLITE_DB_FILE)
    pg_conn = psycopg2.connect(DATABASE_URL)

    start_time = time.monotonic()
    print("Truncating all tables (CASCADE) to clear existing data...")
    cursor = pg_conn.cursor()
    truncate_query = ", ".join([f'"{t}"' for t in TABLES_TRUNCATE])
    cursor.execute(f"TRUNCATE {truncate_query} CASCADE;")
    pg_conn.commit()
    cursor.close()
    print(" All tables cleared\n")

    print("Migrating small tables...")
    for table in TABLES_ORDER[:-1]:  # all except OrderDetail
        migrate_small_table(sqlite_conn, pg_conn, table)

    print("\nMigrating large table OrderDetail...")
    migrate_large_table(sqlite_conn, pg_conn, TABLES_ORDER[-1])

    sqlite_conn.close()
    pg_conn.close()

    end_time = time.monotonic()
    print(f"\n Migration complete! Total time: {end_time - start_time:.2f} seconds")
