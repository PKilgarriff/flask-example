import psycopg2
from psycopg2 import OperationalError


class Queries:
    def __init__(self):
        self.mismatched_record_counter = 0

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
                    student_id,
                    country_code,
                    class_periods,
                    mins_per_period
                    )
                 VALUES(%s, %s, %s, %s);"""
        self.insert_many(connection, record_list, sql)

    def insert_submission_times_records(self, connection, record_list):
        sql = """INSERT INTO submission_times(
                    student_id,
                    created_at
                    )
                 VALUES(%s, %s);"""
        self.insert_many(connection, record_list, sql)

    def insert_early_education_and_belonging_records(self, connection, record_list):
        sql = """INSERT INTO early_education_and_belonging(
                    student_id,
                    country_code,
                    durecec,
                    belong
                    )
                 VALUES(%s, %s, %s, %s);"""
        self.insert_many(connection, record_list, sql)

    def truncate_warehouse_tables(self, connection, tables_to_truncate):
        cursor = connection.cursor()
        for table in tables_to_truncate:
            print(f"Truncating {table}")
            try:
                print(f"attempting to truncate")
                # This is hanging - have checked on RDS
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"attempting to commit")
                connection.commit()
                print(f"committed")
            except OperationalError as e:
                print(f"The error '{e}' occurred")
        if cursor != None:
            cursor.close()

# Mapping SQL responses to useful data for analysis tables

    def durecec(self, item):
        # PISA variable - student years in early care and education
        isced0_start, isced1_start = int(item[1]), int(item[2])
        if isced0_start > isced1_start:
            self.mismatched_record_counter += 1
            print(
                f"Mismatch in starting years for record {item[0]} 0: {isced0_start} 1: {isced1_start}")
        return (isced1_start - isced0_start) + 2

    def belong(self, item):
        # PISA variable - student sense of belonging at school
        # ST034Q01TA - Thinking about your school: I feel like an outsider (or left out of things) at school. NEGATIVE
        q01 = int(item[3]) * -1
        # ST034Q02TA - Thinking about your school: I make friends easily at school. POSITIVE
        q02 = int(item[4])
        # ST034Q03TA - Thinking about your school: I feel like I belong at school. POSITIVE
        q03 = int(item[5])
        # ST034Q04TA - Thinking about your school: I feel awkward and out of place in my school. NEGATIVE
        q04 = int(item[6]) * -1
        # ST034Q05TA - Thinking about your school: Other students seem to like me. POSITIVE
        q05 = int(item[7])
        # ST034Q06TA - Thinking about your school: I feel lonely at school. NEGATIVE
        q06 = int(item[8]) * -1

        all_questions = [q01, q02, q03, q04, q05, q06]
        sense_of_belonging = sum(all_questions) / len(all_questions)
        return sense_of_belonging

    def pared(item):
        # PISA variable - parents’ highest level of education
        pass

    def hisei(item):
        # PISA variable - parents’ highest occupational status
        pass

    def homepos(item):
        # PISA variable - home possessions (including books)
        pass

    def build_learning_hour_record_rows(self, country_code, query_output):
        return [
            (item[0], country_code.upper(), item[1], item[2])
            for item in query_output
            if (item[1] != 'NA' and item[2] != 'NA')
        ]

    def build_early_education_and_belonging_rows(self, country_code, query_output):
        return [
            (item[0], country_code.upper(),
             self.durecec(item), self.belong(item))
            for item in query_output
            if ('NA' not in item)
        ]

# Methods called from db_scraper script to update each table

    def update_learning_hours_table(self, warehouse_conn, country_connections):
        print(f"Updating 'Learning Hours' table")
        for country_code, connection in country_connections.items():
            query_response = self.select_many(
                connection, """SELECT id, st060q01na, st061q01na FROM responses;""")
            learning_time_rows = self.build_learning_hour_record_rows(
                country_code, query_response)
            self.insert_learning_hours_records(
                warehouse_conn, learning_time_rows)
        print(
            f"There are {self.mismatched_record_counter} mis-matched records")

    def update_submission_times_table(self, warehouse_conn, country_connections):
        print(f"Updating 'Submission Times' table")
        for connection in country_connections.values():
            created_at_rows = self.select_many(
                connection, """SELECT id, created_at FROM responses;""")
            self.insert_submission_times_records(
                warehouse_conn, created_at_rows)

    def update_early_education_and_belonging_table(self, warehouse_conn, country_connections):
        print(f"Updating 'Early Education and Belonging' table")
        for country_code, connection in country_connections.items():
            query_response = self.select_many(
                connection,
                """SELECT id, st125q01na, st126q01ta, st034q01ta, st034q02ta, st034q03ta, st034q04ta, st034q05ta, st034q06ta FROM responses;""")
            early_education_and_belonging_rows = self.build_early_education_and_belonging_rows(
                country_code, query_response)
            self.insert_early_education_and_belonging_records(
                warehouse_conn, early_education_and_belonging_rows)
