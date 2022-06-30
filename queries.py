import psycopg2
from psycopg2 import OperationalError


class Queries:
    def __init__(self):
        pass

    def execute_query_fetch_all(self, connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        finally:
            if (cursor != None):
                cursor.close()

    def insert_many(self, connection, record_list, query):
        try:
            cursor = connection.cursor()
            cursor.executemany(query, record_list)
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if cursor != None:
                cursor.close()

    def insert_learning_hours_records(self, connection, record_list):
        sql = """INSERT INTO learning_hours(
                    country_code,
                    class_periods,
                    average_mins
                    )
                 VALUES(%s, %s, %s);"""
        self.insert_many(connection, record_list, sql)

    def insert_submission_times_records(self, connection, record_list):
        sql = """INSERT INTO submission_times(
                    created_at
                    )
                 VALUES(%s);"""
        self.insert_many(connection, record_list, sql)

    def query_learning_hours_by_country(self, warehouse_conn):
        average_tmins_by_country_query = """SELECT country_code, AVG(class_periods * learning_hours.average_mins)
                  FROM learning_hours
                  GROUP BY country_code
                  ORDER BY country_code;"""
        return self.execute_query_fetch_all(warehouse_conn, average_tmins_by_country_query)

    def query_submissions_by_hour(self, warehouse_conn):
        count_of_submissions_by_hour_query = """SELECT extract(hour FROM created_at) AS hour, count(id)
                  FROM submission_times
                  GROUP BY hour
                  ORDER BY hour;"""
        return self.execute_query_fetch_all(warehouse_conn, count_of_submissions_by_hour_query)

    def query_submissions_count(self, warehouse_conn):
        submissions_count_query = """SELECT count(*) FROM submission_times;"""
        return self.execute_query_fetch_all(warehouse_conn, submissions_count_query)

    def build_learning_hour_record_rows(self, country_code, query_output):
        return [
            (country_code.upper(), item[0], item[1])
            for item in query_output
            if (item[0] != 'NA' and item[1] != 'NA')
        ]

    def update_learning_hours_table(self, warehouse_conn, country_connections):
        for country_code, connection in country_connections.items():
            query_response = self.execute_query_fetch_all(
                connection, """SELECT st060q01na, st061q01na FROM responses;""")
            learning_time_rows = self.build_learning_hour_record_rows(
                country_code, query_response)
            self.insert_learning_hours_record(
                warehouse_conn, learning_time_rows)

    def update_submission_times_table(self, warehouse_conn, country_connections):
        for connection in country_connections.values():
            created_at_rows = self.execute_query_fetch_all(
                connection, """SELECT created_at FROM responses;""")
            print(created_at_rows)
            self.insert_submission_times_records(
                warehouse_conn, created_at_rows)

    def count_submissions_json(self, warehouse_conn):
        count = self.query_submissions_count(warehouse_conn)
        return {"count": sum(count[0])}

    def learning_hours_json(self, warehouse_conn, countries):
        sql_response = self.query_learning_hours_by_country(warehouse_conn)
        datasets = []
        for item in sql_response:
            if len(countries) == 0 or item[0] in countries:
                datasets.append({
                    "country": item[0],
                    "hours": round(item[1], None)
                })
        return {"datasets": datasets}

    def submissions_by_hour_json(self, warehouse_conn):
        sql_response = self.query_submissions_by_hour(warehouse_conn)
        data = []
        for item in sql_response:
            hour_string = f"{int(item[0]):02d}:00"
            data.append({
                "x": hour_string,
                "y": item[1]
            })
        return {
            "datasets": [
                {
                    "id": "Submissions",
                    "data": data
                }
            ]
        }
