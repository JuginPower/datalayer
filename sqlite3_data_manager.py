import sqlite3
import logging

# Logging if something goes wrong
logger = logging.getLogger(__name__)
logging.basicConfig(filename="sqlite3_data_manager.log", encoding="utf-8", level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d, %H:%M:%S')


class SqliteDataManager:

    def __init__(self, database_name: str, sql_script=None):

        """
        Needs a connection string for the sqlite database.
        If sql_script is given, parameter maybe executed if the database is empty while creating the connection.
        Sqlite always creates a new database if there is no one.

        :param database_name: name as string for the database
        :type database_name: str
        :param sql_script: sql-script as string for creating a schema if necessary.
        :type sql_script: str or None
        """

        self.connection_string = database_name
        self.sql_script = sql_script

    def init_database(self, conn: sqlite3.Connection):

        """
        Initialize the sqlite database if the database is empty and execute the script for creating the tables.

        :param conn: The connection which is necessary to initialize a database.
        :type conn: sqlite3.Connection.
        :raise sqlite3.Error: If it occurs will be logged.
        """

        try:
            cursor = conn.cursor()
            cursor.executescript(self.sql_script)
        except sqlite3.Error as err:
            logger.error("Something goes wrong while initializing the sqlite3 database: %s", err)
        else:
            cursor.close()
            conn.commit()

    def init_conn(self) -> sqlite3.Connection:

        """
        Create the connection with the database and returns it.
        Checks also if there are enough tables in the database.
        If not, then self.sql_script will be executed if given.

        :raise sqlite3.Error: If it occurs will be logged.
        :return: SQLite database connection object.
        :rtype: sqlite3.Connection
        """

        try:
            conn = sqlite3.connect(self.connection_string)
        except sqlite3.Error as err:
            logger.error("Something goes wrong while initializing the connection to an sqlite3 database: %s", err)
            raise err
        else:
            list_tables = conn.execute("select name from sqlite_master where type='table';").fetchall()

            if len(list_tables) < 2 and self.sql_script:
                self.init_database(conn)

            return conn

    def select(self, sqlstring: str) -> list:

        """
        Used only for reading actions to the database.

        :param sqlstring: The select statement as string given.
        :raise sqlite3.Error: If it occurs will be logged.
        :return: Returns a list with tuple/s. Can also be an empty list.
        :rtype: list[tuple]
        """

        mydb = self.init_conn()
        mycursor = mydb.cursor()

        try:
            mycursor.execute(sqlstring)
        except sqlite3.Error as err:
            logger.error("Something goes wrong while selecting data: %s", err)
            raise err

        result = mycursor.fetchall()
        mydb.close()
        return result

    def select_pragma_info(self, tablename: str) -> list:

        """
        Used for getting the structure from a specific table in the database.

        :param tablename: table name as string.
        :return: A List with all results as tuples.
        :rtype: list[tuple]
        """

        return self.select(f"select * from pragma_table_info('{tablename}');")

    def query(self, sqlstring, val=None) -> int:

        """
        Is used as creating, delete and update method to the database.

        :param sqlstring: The query sql-statement given as string.
        :param val: Can be a tuple (if one record to insert), list (if many data) or None (if only querying).
        :type val: tuple | list | None.
        :return: integer.
        :rtype: int.
        """

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
            logger.error("Something goes wrong while querying data: %s", err)
            raise err

        mydb.commit()
        mydb.close()
        return mycursor.rowcount
