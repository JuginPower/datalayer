import unittest
import mysql.connector
import logging
from mysql_data_manager import MysqlDataManager
from settings import mariadb_config

# Logging-Level für den Test anpassen, um die Ausgabe zu reduzieren
logger = logging.getLogger(__name__)
logging.basicConfig(filename="test_mysql_data_manager.log", encoding="utf-8", level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d, %H:%M:%S')


class TestMysqlDataManagerLive(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Wird einmal vor allen Tests ausgeführt. Es wird eine
        Verbindung zur Datenbank hergestellt und eine Testtabelle
        sowie eine Prozedur erstellt.
        """
        cls.db_config = mariadb_config
        cls.data_manager = MysqlDataManager(cls.db_config)

        try:
            # Verbindung zur Datenbank herstellen
            conn = cls.data_manager.init_conn()
            cursor = conn.cursor()

            # Tabelle für Tests erstellen
            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(255),
                        email VARCHAR(255)
                    );
                """)

            # Prozedur für Tests erstellen
            cursor.execute("""
                    CREATE OR REPLACE PROCEDURE get_user_by_name(IN uname VARCHAR(255))
                    BEGIN
                        SELECT id, name, email FROM users WHERE name = uname;
                    END;
                """)
            conn.commit()
            cursor.close()
            conn.close()

            print("Testtabelle 'users' und Prozedur 'get_user_by_name' erfolgreich erstellt.")

        except mysql.connector.Error as err:
            message = "Fehler bei der Vorbereitung der Testumgebung: %s"
            logger.error(message, err)
            print(message, err)
            raise err

    @classmethod
    def tearDownClass(cls):
        """
        Wird einmal nach allen Tests ausgeführt, um die erstellten
        Tabellen und Prozeduren zu löschen.
        """
        try:
            conn = cls.data_manager.init_conn()
            cursor = conn.cursor()

            # Testtabelle löschen
            cursor.execute("DROP TABLE IF EXISTS users;")

            # Prozedur löschen
            cursor.execute("DROP PROCEDURE IF EXISTS get_user_by_name;")

            conn.commit()
            cursor.close()
            conn.close()

            print("Testtabelle und Prozedur erfolgreich gelöscht.")

        except mysql.connector.Error as err:
            message = "Fehler beim Bereinigen der Testumgebung: %s"
            logger.error(message, err)
            print(message, err)
            raise err

    def setUp(self):
        """
        Wird vor jedem Test ausgeführt, um die Tabelle zu leeren.
        """
        conn = self.data_manager.init_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users;")
        conn.commit()
        cursor.close()
        conn.close()

    def test_select(self):
        """Testet die select-Methode."""
        # Daten einfügen, um sie später zu selektieren
        sql_insert = "INSERT INTO users (name, email) VALUES (%s, %s);"
        values = [('Alice', 'alice@test.com'), ('Bob', 'bob@test.com')]
        self.data_manager.query(sql_insert, values)

        # Daten selektieren und Ergebnis überprüfen
        sql_select = "SELECT name, email FROM users ORDER BY name;"
        result = self.data_manager.select(sql_select)

        expected_result = [('Alice', 'alice@test.com'), ('Bob', 'bob@test.com')]
        self.assertEqual(result, expected_result)
        self.assertIsInstance(result, list)
        if result:
            self.assertIsInstance(result[0], tuple)

    def test_query_insert_single(self):
        """Testet die query-Methode für einen einzelnen Insert."""
        sql_query = "INSERT INTO users (name, email) VALUES (%s, %s);"
        val = ('Charlie', 'charlie@test.com')
        rows_affected = self.data_manager.query(sql_query, val)

        self.assertEqual(rows_affected, 1)

        # Überprüfen, ob der Datensatz wirklich eingefügt wurde
        sql_select = "SELECT COUNT(*) FROM users WHERE name = 'Charlie';"
        result = self.data_manager.select(sql_select)
        self.assertEqual(result[0][0], 1)

    def test_query_insert_multiple(self):
        """Testet die query-Methode für multiple Inserts."""
        sql_query = "INSERT INTO users (name, email) VALUES (%s, %s);"
        val = [('David', 'david@test.com'), ('Eva', 'eva@test.com')]
        rows_affected = self.data_manager.query(sql_query, val)

        self.assertEqual(rows_affected, 2)

        # Überprüfen, ob die Datensätze wirklich eingefügt wurden
        sql_select = "SELECT COUNT(*) FROM users;"
        result = self.data_manager.select(sql_select)
        self.assertEqual(result[0][0], 2)

    def test_query_update(self):
        """Testet die query-Methode für ein Update."""
        # Daten einfügen, die dann aktualisiert werden
        affected_rows = self.data_manager.query("INSERT INTO users (name, email) VALUES ('Frank', 'frank@test.com');")

        sql_query = "UPDATE users SET email = %s WHERE name = %s;"
        val = ('frank_new@test.com', 'Frank')
        rows_affected = self.data_manager.query(sql_query, val)

        self.assertEqual(rows_affected, 1)

        # Überprüfen, ob das Update erfolgreich war
        sql_select = "SELECT email FROM users WHERE name = 'Frank';"
        result = self.data_manager.select(sql_select)
        self.assertEqual(result[0][0], 'frank_new@test.com')

    def test_call_proc(self):
        """Testet die call_proc-Methode."""
        # Testdaten einfügen
        affected_rows = self.data_manager.query("INSERT INTO users (name, email) VALUES ('Grace', 'grace@test.com');")

        # Prozedur aufrufen
        result = self.data_manager.call_proc("get_user_by_name", args=('Grace',))

        self.assertEqual(len(result[0]), 3)
        self.assertEqual(result[0][1], 'Grace')
        self.assertEqual(result[0][2], 'grace@test.com')

    def test_init_conn_retry(self):
        """
        Testet die Wiederverbindung, indem eine falsche Konfiguration
        erzwungen wird.
        """
        wrong_config = self.db_config.copy()
        wrong_config['port'] = 9999  # Ein falscher Port, um den Fehler zu erzwingen

        data_manager_wrong_conn = MysqlDataManager(wrong_config)
        with self.assertRaises(mysql.connector.Error):
            data_manager_wrong_conn.init_conn(attempts=1)


if __name__ == '__main__':
    unittest.main()