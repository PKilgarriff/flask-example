import psycopg2
from psycopg2 import OperationalError


class Queries:
    def __init__(self):
        pass

# Generic SQL execution methods

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

# Specific SQL Query method wrappers

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

# Mapping SQL responses to useful data for analysis tables

    def build_learning_hour_record_rows(self, country_code, query_output):
        return [
            (country_code.upper(), item[0], item[1])
            for item in query_output
            if (item[0] != 'NA' and item[1] != 'NA')
        ]

    def durecec(self, item):
        # PISA variable - student years in early care and education
        isced0_start = int(item[0])
        isced1_start = int(item[1])
        return (isced1_start - isced0_start) + 2

    def belong(self, item):
        # PISA variable - student sense of belonging at school
        # ST034Q01TA - Thinking about your school: I feel like an outsider (or left out of things) at school. NEGATIVE
        q01 = int(item[2])
        # ST034Q02TA - Thinking about your school: I make friends easily at school. POSITIVE
        q02 = int(item[3])
        # ST034Q03TA - Thinking about your school: I feel like I belong at school. POSITIVE
        q03 = int(item[4])
        # ST034Q04TA - Thinking about your school: I feel awkward and out of place in my school. NEGATIVE
        q04 = int(item[5])
        # ST034Q05TA - Thinking about your school: Other students seem to like me. POSITIVE
        q05 = int(item[6])
        # ST034Q06TA - Thinking about your school: I feel lonely at school. NEGATIVE
        q06 = int(item[7])

        positive_qus = [q02, q03, q05]
        # positive_feeling = sum(positive_qus)
        negative_qus = [q01, q04, q06]
        negative_qus = [response * -1 for response in negative_qus]
        all_questions = positive_qus + negative_qus
        sense_of_belonging = sum(all_questions) / len(all_questions)
        return sense_of_belonging

        # st034_qus = [int(item[2]), int(item[3]), int(item[4]),
        #              int(item[5]), int(item[6]), int(item[7])]
        # st034_avg = sum(st034_qus) / len(st034_qus)
        # return st034_avg

    def pared(item):
        # PISA variable - parents’ highest level of education
        pass

    def hisei(item):
        # PISA variable - parents’ highest occupational status
        pass

    def homepos(item):
        # PISA variable - home possessions (including books)
        pass

# Methods called from db_scraper script to update each table

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
