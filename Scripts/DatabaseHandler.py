import mysql.connector
import sys
import json


class DatabaseHandler(object):

    # Configures parameters by machine (Matthew's 'my_mac' or 'my_win')
    def __init__(self, machine):
        if machine == 'my_mac':
            self.path = '/opt/homebrew/bin/chromedriver'
            with open("mac_creds.json") as f:
                data = json.load(f)
                self.db_host = data["hostW"]
                self.db_user = data["user"]
                self.db_password = data["password"]

        elif machine == 'my_win':
            self.path = 'C:/WebDriver/chromedriver'
            self.db_user = 'root'
            self.db_host = 'localhost'
            with open('win_creds.json') as f:
                data = json.load(f)
                self.db_password = data['password']

        else:
            sys.exit(f"Machine \"{machine}\" not recognized")

    # returns connection object
    def connect_to_db(self, db_name):
        cnx = mysql.connector.connect(
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            database=self.db_name
        )
        return cnx

    # returns boolean
    def table_exists(cursor, tbl_name):
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = \"""" + tbl_name + """\";
        """)

        if cursor.fetchone()[0] == 1:
            return True
        return False
