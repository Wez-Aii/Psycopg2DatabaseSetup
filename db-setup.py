import csv
import psycopg2
import argparse
import getpass
import os
import pwinput


POSTGRES_DB_ADDRESS = os.getenv("POSTGRES_DB_ADDRESS", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DM_USERNAME = os.getenv("POSTGRES_DM_USERNAME")
POSTGRES_DM_PASSWORD = os.getenv("POSTGRES_DM_PASSWORD")

class Psycopg2Driver:
    def __init__(self) -> None:
        self.database_name = None
        self.existing_databases = []
        
    def prompt_db_connection_info(self):
        self.host_addr = input("Enter host_addr (eg. localhost or 127.0.0.1): ")
        self.host_port = input("Enter host_port (eg. 5432): ")
        self.__username = input("Enter username: ")
        # self.__password = getpass.getpass("Enter password: ")
        self.__password = pwinput.pwinput(prompt="Enter password: ", mask='*')

    def check_db_connection(self):
        while True:
            try:
                self.connect()
                break
            except Exception as e:
                print(str(e))
                if input("Do you want to reenter database connection info (Y/N): ").lower() == "y":
                    self.prompt_db_connection_info()
                else:
                    print("Program existing...")
                    raise SystemExit(1)
                
    def connect(self):
        try:
            if self.database_name is not None:
                _conn = psycopg2.connect(
                    dbname=self.database_name,
                    user=self.__username,
                    password=self.__password,
                    host=self.host_addr,
                    port=self.host_port
                )
            else:
                _conn = psycopg2.connect(
                    user=self.__username,
                    password=self.__password,
                    host=self.host_addr,
                    port=self.host_port
                )
            return _conn
        except Exception as e:
            print(f"(f) connect - An error occured: {e}")
            raise Exception(f"(f) connect - An error occured: {e}")

    def check_database_existence(self, cursor, db_name):
        cursor.execute("SELECT datname FROM pg_database;")
        self.existing_databases = [row[0] for row in cursor.fetchall()]
        print(f"existed databases - {self.existing_databases}")
        return db_name.lower() in self.existing_databases
        
    def select_database(self):
        try:
            db_name = input("Enter database name: ")
            _temp_db_connection = self.connect()
            _temp_db_connection.autocommit = True
            cur = _temp_db_connection.cursor()
            if not self.check_database_existence(cursor=cur, db_name=db_name):
                print(f"Database({db_name}) do not exist")
                if input(f"Do you want to create database - {db_name} (Y/N): ").lower() == "y":
                    _sql_command_str = "CREATE DATABASE %s;"%db_name
                    cur.execute(_sql_command_str)
                    print(f"Database {db_name} created successfully.")
                    _temp_db_connection.close()
                    self.database_name = db_name
                    self.select_action_to_continue()
                else:
                    if input("Exist or Continue the program (exit or continue): ").lower() == "continue":
                        _temp_db_connection.close()
                        self.select_database()
                    else:
                        # Program exiting...
                        raise Exception("Program exiting...")
            else:
                print(f"Database {db_name} has been selected.")
                _temp_db_connection.close()
                self.database_name = db_name
                self.select_action_to_continue()

        except Exception as e:
            print(f"(f) select_database - {e}")
            _temp_db_connection.close()
            raise SystemExit(1)
        
    def select_action_to_continue(self):
        print("Available actions :\n\t1) delete selected database\n\t2) execute sql file\n\t3) delete table\n\t4) insert test data\n\t5) exist program (enter any other key)")
        _continue_action = input("Select the action suggested above by enter a number(1, 2, 3, 4, 5): ")
        if _continue_action == "1":
            self.delete_database(self.database_name)
        elif _continue_action == "2":
            self.execute_sql_file()
        elif _continue_action == "3":
            self.delete_table()
        elif _continue_action == "4":
            self.load_data_from_csv_files_folder()
        else:
            print("Program exiting...")
    
    def delete_database(self, db_name):
        try:
            _temp_holder = self.database_name
            self.database_name = None
            _temp_db_connection = self.connect()
            _temp_db_connection.autocommit = True
            with _temp_db_connection.cursor() as cur:
                cur.execute(f"DROP DATABASE {db_name};")
            _temp_db_connection.close()
            print(f"Database '{db_name}' deleted successfully.")
        except Exception as e:
            _temp_db_connection.close()
            self.database_name = _temp_holder
            print(f"(f) delete_database - An error occured: {e}")
        if input("Exist or Continue the program (exit or continue): ").lower() == "continue":
            self.select_database()
        else:
            print("Program exiting...")
        
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
            _temp_db_connection.close()
        except Exception as e:
            print(f"(f) create_test_table - An error occured: {e}")
            raise SystemExit(1)        
        
    def execute_sql_file(self):
        _sql_file_path = input("Enter sql file path to execute: ")
        if os.path.isfile(_sql_file_path) and _sql_file_path[-4:] == ".sql":
            try:
                _temp_db_connection = self.connect()
                _temp_db_connection.autocommit = True
                with _temp_db_connection.cursor() as cur:
                    cur.execute(open(_sql_file_path, "r").read())
                    print("Sql file executed successfully.")
                _temp_db_connection.close()
            except Exception as e:
                _temp_db_connection.close()
                print("Sql file failed to execute.")
                print(f"(f) execute_sql_file - An error occured: {e}")
        else:
            print("Invalid Input: the file path not exist")
        self.select_action_to_continue()
    
    def load_data_from_csv_files_folder(self):
        _csv_files_folder = input("Enter csv files folder path: ")
        if os.path.isdir(_csv_files_folder):
            for each in os.listdir(_csv_files_folder):
                _temp_db_connection = self.connect()
                _temp_db_connection.autocommit = True
                _rows_data = []
                _column_names = []
                if each[-4:] == ".csv":
                    _table_name = each[:-4]
                    _file_path = f"{_csv_files_folder}/{each}"
                    with open(_file_path, 'r', newline='') as _csvfile:
                        _all_rows = csv.reader(_csvfile)
                        _column_names = next(_all_rows)
                        _rows_data = [_row for _row in _all_rows]
                    _column_names_str = ','.join(_column_names)
                    _placeholders_str = ','.join("%s" for each in _column_names)
                    _sql_script = f"""
                            INSERT INTO {_table_name.lower()}({_column_names_str}) VALUES ({_placeholders_str})
                        """
                    try:
                        with _temp_db_connection.cursor() as cur:
                            # cur.execute(_sql_script)
                            for _row in _rows_data:
                                cur.execute(_sql_script, _row)
                        _temp_db_connection.close()
                        print("csv file data inserts successfully")
                    except Exception as e:
                        _temp_db_connection.close()
                        print(f"Failed to execute {each}.")
                        print(f"(f) load_data_from_csv_files_folder - An error occured: {e}")
            if len(os.listdir(_csv_files_folder)) == 0:
                print("The folder is empty.")
        else:
            print("The input path is not a folder.")                    
        self.select_action_to_continue()
    
    def delete_table(self):
        table_name = input("Enter table name to delete: ")
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    if self.check_table_existence(cur, table_name):
                        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                        print(f"Table '{table_name}' deleted successfully.")
                    else:
                        print(f"Table '{table_name}' does not exist.")
                conn.close()
        except Exception as e:
            print(f"(f) delete_table - An error occured: {e}")
            raise Exception(f"(f) delete_table - An error occured: {e}") 
        self.select_action_to_continue()

def get_user_credentials():
    username = input("Enter username: ")
    # password = getpass.getpass("Enter password: ")
    password = pwinput.pwinput(prompt="Enter password: ", mask='*')
    return username, password

if __name__=="__main__":
    # args = parse_args()
    # username, password = get_user_credentials()
    # test = Psycopg2Driver(username=username, password=password, host_addr=args.host, port=args.port)
    # test.select_database(args.dbname)
    # test.create_test_table(args.table_name)

    test = Psycopg2Driver()
    test.prompt_db_connection_info()
    test.check_db_connection()
    test.select_database()

