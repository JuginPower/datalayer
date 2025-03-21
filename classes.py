import sqlite3
import mysql.connector
import logging
from time import sleep

# Logging if something goes wrong
logger = logging.getLogger(__name__)
logging.basicConfig(filename="datalayer.log", encoding="utf-8", level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d, %H:%M:%S')


class MysqlConnectorManager:

    """
    My Mysql Driver which I use for the connection to my mariadb-server.
    The intention to write this class is to commit every action and close the connection automatically after each
    operation on the database
    """

    def __init__(self, config: dict):

        self.config = config

    def init_conn(self, attempts=3, delay=2):

        """
        Initialize the connection with my mariadb database.

        :param attempts: amount of attempts
        :param delay: waiting seconds for trying to reconnect
        """

        attempt = 1
        # Implement a reconnection routine
        while attempt < attempts + 1:
            try:
                return mysql.connector.connect(**self.config)
            except (mysql.connector.Error, IOError) as err:
                if attempts is attempt:
                    # Attempts to reconnect failed; returning None
                    logger.error("Failed to connect, exiting without a connection: %s", err)
                    return None
                logger.error(
                    "Connection failed: %s. Retrying (%d/%d)...",
                    err,
                    attempt,
                    attempts - 1,
                )
                # progressive reconnect delay
                sleep(delay ** attempt)
                attempt += 1
        return None

    def select(self, sqlstring) -> list:

        """
        Used only for reading actions to the database.

        :param sqlstring: The select statement as string given
        :return: Return always a list, can also be empty
        """

        mydb = self.init_conn()
        cursor = mydb.cursor()

        try:
            cursor.execute(sqlstring)
        except (mysql.connector.Error, IOError) as err:
            raise err

        result = cursor.fetchall()
        mydb.close()
        return result

    def query(self, sqlstring, val=None) -> int:

        """
        Can be used for create, delete, update method to the database

        :param sqlstring: The query sql statement given as string
        :param val: Can be None or a list or tuple
        :return: integer about how much rows were affected, can be also 0
        """

        mydb = self.init_conn()
        mycursor = mydb.cursor()

        try:
            if isinstance(val, list):
                # if list is given, then executemany!
                mycursor.executemany(sqlstring, val)
            elif isinstance(val, tuple):
                mycursor.execute(sqlstring, val)
            elif not val:
                mycursor.execute(sqlstring)

        except (mysql.connector.Error, IOError) as err:
            raise err

        mydb.commit()
        mydb.close()
        return mycursor.rowcount


class SqliteDatamanager:
    """
    My Sqlite Driver which I use for the connection to file based databases.
    The intention to write this class is to commit every action and close the connection automatically after each
    operation on the database.
    """

    def __init__(self, database_name: str, sql_script=None):

        """
        Needs a connection string for the sqlite database.
        If sql_script is given, parameter maybe executed if the database is empty while creating the connection.
        Sqlite always creates a new database if there is no one.

        :param database_name: name as string for the database
        :param sql_script: sql script as string for creating a schema
        """

        self.connection_string = database_name
        self.sql_script = sql_script

    def init_database(self, conn: sqlite3.Connection):

        """
        Initialize the sqlite database if the database is empty and execute the script for creating the tables.

        :param conn: The connection which is needed .
        :return: Nothing, only closes the cursor but not the connection.
        """

        try:
            cursor = conn.cursor()
            cursor.executescript(self.sql_script)
        except sqlite3.Error as err:
            raise err
        else:
            cursor.close()
            conn.commit()

    def init_conn(self):

        """
        Create the connection with the database and returns it.
        Checks also if there are enough tables in the database, if not then self.sql_script will be executed if given.

        :return: Connection
        """

        try:
            conn = sqlite3.connect(self.connection_string)
        except sqlite3.Error as err:
            raise err
        else:
            list_tables = conn.execute("select name from sqlite_master where type='table';").fetchall()

            if len(list_tables) < 2 and self.sql_script:
                self.init_database(conn)

            return conn

    def select(self, sqlstring) -> list:

        """
        Used only for reading actions to the database.

        :param sqlstring: The select statement as string given.
        :return: Return always a list, can also be empty.
        """

        mydb = self.init_conn()
        mycursor = mydb.cursor()

        try:
            mycursor.execute(sqlstring)
        except sqlite3.Error as err:
            raise err

        result = mycursor.fetchall()
        mydb.close()
        return result

    def select_pragma_info(self, tablename: str) -> list:

        """
        Used for getting the structure from a specific table in the database.

        :param tablename: table name as string.
        :return: A List with all results, can be empty.
        """

        return self.select(f"select * from pragma_table_info('{tablename}');")

    def query(self, sqlstring, val=None) -> int:

        mydb = self.init_conn()
        mycursor = mydb.cursor()

        try:
            if isinstance(val, list):
                mycursor.executemany(sqlstring, val)
            elif isinstance(val, tuple):
                mycursor.execute(sqlstring, val)
            elif not val:
                mycursor.execute(sqlstring)

        except sqlite3.Error as err:
            raise err

        mydb.commit()
        mydb.close()
        return mycursor.rowcount

