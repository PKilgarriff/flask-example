import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

load_dotenv()


class DataWarehouse:
    def __init__(self):
        self.connection = self.connect()

    def connect(self):
        connection = None
        try:
            connection = psycopg2.connect(
                database="postgres",
                user="postgres",
                password=os.environ.get("aws-pass"),
                host=os.environ.get("aws-url"),
                port=5432,
            )
            print(f"Connection to AWS Hosted PostgreSQL DB successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return connection

    def connection(self):
        return self.connection
