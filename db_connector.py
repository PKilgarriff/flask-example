import psycopg2
from psycopg2 import OperationalError


class DatabaseConnection:
    def __init__(self):
        self.connection = self.create_local_database_connection()

    def create_local_database_connection(self, db_host="127.0.0.1", db_port="5432", db_name="pisa_backend_dev"):
        connection = None
        try:
            connection = psycopg2.connect(
                database=db_name,
                host=db_host,
                port=db_port,
            )
            print(f"Connection to PostgreSQL DB at {db_host} successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return connection

# The below functions should be extracted

    def execute_read_query(self, query):
        cursor = self.connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except OperationalError as e:
            print(f"The error '{e}' occurred")

# Or possibly just the below functions should be extracted, leaving execute_read_query

    def count_rows(self, table="submission_timestamps"):
        count_ids_query = f"SELECT COUNT(id) FROM {table}"
        output = self.execute_read_query(count_ids_query)
        return int(output[0][0])

    def submissions_by_time(self, table="submission_timestamps"):
        count_by_submission_hour_query = f"SELECT submission_hour, COUNT(submission_hour) FROM {table} GROUP BY submission_hour;"
        output = self.execute_read_query(count_by_submission_hour_query)

        return_data = []
        for hour_tuple in output:
            return_data.append({
                "x": hour_tuple[0],
                "y": hour_tuple[1]
            })
        return return_data


# db_connection = DatabaseConnection()
# print(f"Output: {db_connection.submission_by_time()}")
