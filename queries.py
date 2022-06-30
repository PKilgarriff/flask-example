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

    def build_learning_hour_record_rows(self, country_code, query_output):
        return [
            (country_code.upper(), item[0], item[1])
            for item in query_output
            if (item[0] != 'NA' and item[1] != 'NA')
        ]

    def insert_learning_hours_records(self, connection, record_list):
        sql = """INSERT INTO learning_hours(
                    country_code,
                    class_periods,
                    average_mins
                    )
                 VALUES(%s, %s, %s);"""
        try:
            cursor = connection.cursor()
            cursor.executemany(sql, record_list)
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if cursor != None:
                cursor.close()

    def query_learning_hours_by_country(self, warehouse_conn):
        average_tmins_by_country_query = """SELECT country_code, AVG(class_periods * learning_hours.average_mins)
                      FROM learning_hours
                      GROUP BY country_code
                      ORDER BY country_code;"""
        return self.execute_query_fetch_all(warehouse_conn, average_tmins_by_country_query)

    def update_learning_hours_table(self, country_connections, warehouse_conn):
        for country_code, connection in country_connections:
            query_response = self.execute_query_fetch_all(
                connection, 'SELECT st060q01na, st061q01na FROM responses')
            learning_time_rows = self.build_learning_hour_record_rows(
                country_code, query_response)
            self.insert_learning_hours_record(
                warehouse_conn, learning_time_rows)

    def learning_hours_json(self, warehouse_conn, countries):
        sql_response = self.query_learning_hours_by_country(warehouse_conn)
        datasets = []
        for item in sql_response:
            if len(countries) == 0 or item[0] in countries:
                datasets.append({
                    "country": item[0],
                    "hours": round(item[1], None)
                })
        return datasets
