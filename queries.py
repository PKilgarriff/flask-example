import psycopg2
from psycopg2 import OperationalError


class Queries:
    def __init__(self):
        pass

    def select_many(self, connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        finally:
            if cursor != None:
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

    def insert_early_education_and_belonging_records(self, connection, record_list):
        sql = """INSERT INTO early_education_and_belonging(
                    country_code,
                    durecec,
                    belong
                    )
                 VALUES(%s, %s, %s);"""
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
            self.insert_learning_hours_records(
                warehouse_conn, learning_time_rows)

    def update_submission_times_table(self, warehouse_conn, country_connections):
        for connection in country_connections.values():
            created_at_rows = self.select_many(
                connection, """SELECT created_at FROM responses;""")
            self.insert_submission_times_records(
                warehouse_conn, created_at_rows)

    def durecec(self, item):
        isced0_start = int(item[0])
        isced1_start = int(item[1])
        return (isced1_start - isced0_start) + 2

    def belong(self, item):
        st034_qus = [int(item[2]), int(item[3]), int(item[4]),
                     int(item[5]), int(item[6]), int(item[7])]
        st034_avg = sum(st034_qus) / len(st034_qus)
        print(st034_avg)
        return st034_avg

    def update_early_education_and_belonging_table(self, warehouse_conn, country_connections):
        for country_code, connection in country_connections.items():
            query_response = self.select_many(
                connection,
                """SELECT st125q01na, st126q01ta, st034q01ta, st034q02ta, st034q03ta, st034q04ta, st034q05ta, st034q06ta FROM responses;""")

            early_education_and_belonging_rows = [
                (country_code.upper(), self.durecec(item), self.belong(item))
                for item in query_response
                if not ('NA' in item)
            ]
            self.insert_early_education_and_belonging_records(
                warehouse_conn, early_education_and_belonging_rows)
