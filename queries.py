import psycopg2
from psycopg2 import OperationalError


def execute_query_fetch_all(connection, query):
    cur = connection.cursor()
    try:
        cur.execute(query)
        response = cur.fetchall()
        return response
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    finally:
        cur.close()


def build_record_rows(country_code, query_output):
    return [(country_code.upper(), item[0], item[1]) for item in query_output if (item[0] != 'NA' and item[1] != 'NA')]


def insert_learning_hours_record(connection, country_code):
    sql = """INSERT INTO learning_hours(country_code, class_periods, average_mins)
             VALUES(%s, %s, %s);"""
    try:
        cursor = connection.cursor()
        cursor.execute(sql, (country_code, 70, 45,))
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cursor != None:
            cursor.close()


def insert_learning_hours_records(connection, record_list):
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


def update_learning_hours_table(country_connections, warehouse_conn):
    for country_code, connection in country_connections:
        query_response = execute_query_fetch_all(
            connection, 'SELECT st060q01na, st061q01na FROM responses')
        learning_time_rows = build_record_rows(country_code, query_response)
        print(
            f"Inserting {len(learning_time_rows)} rows from {len(query_response)} Query Response(s)")
        insert_learning_hours_record(warehouse_conn, learning_time_rows)
