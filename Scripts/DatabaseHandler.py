import mysql.connector
import sys
import json


def table_exists(cursor, tbl_name):
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = '{tbl_name}';
    """)

    return cursor.fetchone()[0] == 1


# returns connection object
def connect_to_db(user, db_name):
    cnx = mysql.connector.connect(
        user=user.db_user,
        password=user.db_password,
        host=user.db_host,
        database=db_name
    )
    return cnx


class User(object):

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
