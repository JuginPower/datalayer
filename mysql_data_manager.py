import mysql.connector
import logging
from time import sleep

# Logging if something goes wrong
logger = logging.getLogger(__name__)
logging.basicConfig(filename="mysql_data_manager.log", encoding="utf-8", level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d, %H:%M:%S')


class MysqlDataManager:

    def __init__(self, config: dict):

        self.config = config

    def init_conn(self, attempts=3, delay=2):

        """
        Initialize the connection with my mariadb database.

        :param attempts: amount of attempts.
        :type attempts: int, default 3.
        :param delay: waiting seconds for trying to reconnect.
        :type delay: int, default 2.
        :raise mysql.connector.Error | IOError: If it occurs, will be logged
        :return: A MySQLConnectionAbstract subclass instance (such as MySQLConnection or CMySQLConnection) or a PooledMySQLConnection instance.
        :rtype: mysql.connector.MySQLConnection | mysql.connector.MySQLConnectionAbstract | mysql.connector.PooledMySQLConnection
        """

        attempt = 1
        # Implement a reconnection routine
        while attempt < attempts + 1:
            try:
                return mysql.connector.connect(**self.config)
            except (mysql.connector.Error, IOError) as err:
                if attempts is attempt:
                    # Attempts to reconnect failed;
                    logger.error("Failed to connect, exiting without a connection: %s", err)
                    raise err
                logger.info(
                    "Connection failed: %s. Retrying (%d/%d)...",
                    err,
                    attempt,
                    attempts - 1,
                )
                # progressive reconnect delay
                sleep(delay ** attempt)
                attempt += 1

    def select(self, sqlstring: str) -> list:

        """
        Used only for reading actions to the database.

        :param sqlstring: The select statement as string given.
        :type sqlstring: str
        :raise mysql.connector.Error | IOError: If it occurs, will be logged
        :return: Return always a list with at least one tuple, can also be empty.
        :rtype: list[tuple]
        """

        mydb = self.init_conn()
        cursor = mydb.cursor()

        try:
            cursor.execute(sqlstring)
        except (mysql.connector.Error, IOError) as err:
            logger.error("Something goes wrong while selecting data: %s", err)
            raise err

        result = cursor.fetchall()
        mydb.close()
        return result

    def query(self, sqlstring, val=None) -> int:

        """
        Can be used for create, delete, update method to the database.

        :param sqlstring: The query sql statement given as string
        :param val: Can be None or a list or tuple
        :raise mysql.connector.Error | IOError: If it occurs, will be logged
        :return: integer about how many rows were affected, can be also 0.
        :rtype: int
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

        except (mysql.connector.Error, IOError) as err:
            logger.error("Something goes wrong while querying the data: %s", err)
            raise err

        mydb.commit()
        mydb.close()
        return mycursor.rowcount


