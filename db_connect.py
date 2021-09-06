import pymysql


class mysql_connect:
    def __init__(self) -> None:
        self.db_settings = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "pwd",
            "db": "finance",
        }

    def connect(self):
        conn = pymysql.connect(**self.db_settings)

        return conn
