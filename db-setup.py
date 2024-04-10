import psycopg2
import argparse
import getpass
import os


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")

class Psycopg2Driver:
    def __init__(self, username:str, password:str, host_addr:str, port:str) -> None:
        self.__username = username
        self.__password = password
        self.database_name = None
        self.host_addr = host_addr
        self.port = port

    def connect(self):
        if self.database_name is not None:
            _conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.__username,
                password=self.__password,
                host=self.host_addr,
                port=self.port
            )
        else:
            _conn = psycopg2.connect(
                user=self.__username,
                password=self.__password,
                host=self.host_addr,
                port=self.port
            )
        return _conn

    def check_database_existence(self, cursor, db_name):
        cursor.execute("SELECT datname FROM pg_database;")
        databases = [row[0] for row in cursor.fetchall()]
        print(f"existed databases - {databases}")
        return db_name.lower() in databases
        
    def create_database(self, db_name):
        try:
            _temp_db_connection = self.connect()
            _temp_db_connection.autocommit = True
            with _temp_db_connection.cursor() as cur:
                if not self.check_database_existence(cursor=cur, db_name=db_name):
                    _sql_command_str = "CREATE DATABASE %s;"%db_name
                    cur.execute(_sql_command_str)
                    print(f"Database {db_name} created successfully.")
                    self.database_name = db_name
        except Exception as e:
            print(f"(f) create_database - An error occured: {e}")
            raise SystemExit(1)
        
    def delete_database(self, db_name):
        try:
            with self.connect() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
            print(f"Database '{db_name}' deleted successfully.")
            self.database_name = None
        except psycopg2.errors.InvalidCatalogName:
            print(f"Database '{db_name}' does not exist.")
        
    def check_table_existence(self, cursor, table_name):
        cursor.execute(
                f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                    );
                """
            )
        return cursor.fetchone()[0]

    def create_test_table(self, table_name):
        try:
            _temp_db_connection = self.connect()
            _temp_db_connection.autocommit = True
            with _temp_db_connection.cursor() as cur:
                if not self.check_table_existence(cur, table_name):
                    _sql_create_table_str = f"""
                            CREATE TABLE {table_name} (
                                id SERIAL PRIMARY KEY,
                                status VARCHAR(50) NOT NULL,
                                status_desc VARCHAR(255) NOT NULL,
                                status_level INTEGER NOT NULL,
                                color_code VARCHAR(50) NOT NULL,
                                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                            );
                        """
                    cur.execute(_sql_create_table_str)
                    print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"(f) create_test_table - An error occured: {e}")
            raise SystemExit(1)        
    
    def delete_table(self, table_name):
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' deleted successfully.")
        except psycopg2.errors.UndefinedTable:
            print(f"Table '{table_name}' does not exist.")
 
def parse_args():
    parser = argparse.ArgumentParser(description="Manage PostgreSQL databases and tables")
    # parser.add_argument("action", choices=["create_db", "delete_db", "create_table", "delete_table"],
    #                     help="Action to perform: create_db, delete_db, create_table, or delete_table")
    parser.add_argument("--dbname", help="Name of the database")
    parser.add_argument("--user", default="postgres", help="PostgreSQL username")
    parser.add_argument("--password", default="", help="PostgreSQL password")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", default="5432", help="PostgreSQL port")
    parser.add_argument("--table_name", help="Name of the table (for create_table and delete_table actions)")
    return parser.parse_args()

def get_user_credentials():
    username = input("Enter username: ")
    password = getpass.getpass("Enter password: ")
    return username, password

if __name__=="__main__":
    args = parse_args()
    username, password = get_user_credentials()
    test = Psycopg2Driver(username=username, password=password, host_addr=args.host, port=args.port)
    test.create_database(args.dbname)
    test.create_test_table(args.table_name)