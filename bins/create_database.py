"""Create landing database

Author: Enrique Olivares <enrique.olivares@wizeline.com>
"""
import psycopg2
from psycopg2.errors import DuplicateDatabase
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main():
    """Main function"""
    conn = psycopg2.connect(
        user="postgres",
        password="postgres",
        host="db",
        port=5432,
        database="postgres",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cur:
        try:
            cur.execute("CREATE DATABASE ml_samples")
        except DuplicateDatabase as _:
            print("Database already exists.")
        else:
            print("Database has been created successfully.")
        finally:
            conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
