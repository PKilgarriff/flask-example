import psycopg2


class Queries:
    def __init__(self):
        pass

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

    def build_learning_hour_record_rows(self, country_code, query_output):
        return [
            (country_code.upper(), item[0], item[1])
            for item in query_output
            if (item[0] != 'NA' and item[1] != 'NA')
        ]

    def update_learning_hours_table(self, warehouse_conn, country_connections):
        for country_code, connection in country_connections.items():
            query_response = self.select_many(
                connection, """SELECT st060q01na, st061q01na FROM responses;""")
            learning_time_rows = self.build_learning_hour_record_rows(
                country_code, query_response)
            self.insert_learning_hours_record(
                warehouse_conn, learning_time_rows)

    def update_submission_times_table(self, warehouse_conn, country_connections):
        for connection in country_connections.values():
            created_at_rows = self.select_many(
                connection, """SELECT created_at FROM responses;""")
            self.insert_submission_times_records(
                warehouse_conn, created_at_rows)
