import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

load_dotenv()

# Does this actually need to hold state? Could these just be class methods?

class CountryDatabase:
    def __init__(self, country_code):
        self.country_code = country_code
        self.connection = self.connect()

    def connect(self):
        connection = None
        try:
            connection = psycopg2.connect(
                database=self.country_code,
                user=os.environ.get("country-user"),
                password=os.environ.get("country-pass"),
                host=f"seta-{self.country_code}.cvcpj1fhj3k9.us-east-2.rds.amazonaws.com",
                port=5432,
            )
            print(
                f"Connection to External Country DB {self.country_code} successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return connection

    def connection(self):
        return self.connection
