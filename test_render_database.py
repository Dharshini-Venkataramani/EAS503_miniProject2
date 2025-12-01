import psycopg2
from utils import get_db_url


def test_postgress_connection(connection_string):
    try:
        conn = psycopg2.connect(connection_string)

        cur = conn.cursor()

        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print("Connection successful!")
        print(f"PostgresSQL version: {db_version}")

        cur.close()


        cur1 = conn.cursor()

        cur1.execute("SELECT * from region;")
        db_region = cur1.fetchall()
        print("Connection successful!")
        print(f"All regions: {db_region}")

        cur1.close()
        conn.close()
        print("Connection closed")
        return True
    except Exception as e:
        print("Connection failed")
        print(e)
        return False

DATABASE_URL = get_db_url()

test_postgress_connection(DATABASE_URL)