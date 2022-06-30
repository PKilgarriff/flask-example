from flask import Flask, jsonify, request
from flask_cors import CORS
from warehouse_db import DataWarehouse

app = Flask(__name__)
warehouse = DataWarehouse()

CORS(app)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    # count = warehouse.connection.count_rows() or "unknown"
    response = {"count": 1022}
    return jsonify(response)


@app.route("/submissions-over-time")
def submissions_over_time():
    # data = warehouse.connection.submissions_by_time() or "unknown"
    data = [
        {"x": "12:00", "y": 82},
        {"x": "13:00", "y": 88},
        {"x": "14:00", "y": 101},
        {"x": "15:00", "y": 97},
        {"x": "16:00", "y": 121},
        {"x": "17:00", "y": 83},
        {"x": "18:00", "y": 59}
    ]
    response = {
        "datasets": [
            {
                "id": "Submissions",
                "data": data
            }
        ]
    }
    return jsonify(response)


@app.route("/early-education-and-belonging")
def early_education_and_belonging():
    response = {
        "datasets": [
            {
                "id": "FRA",
                "data": [
                    {
                        "x": 4,
                        "y": 1.8,
                        "submissions": 85
                    }
                ]
            },
            {
                "id": "GBR",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "ESP",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "DOM",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "JPN",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "UKR",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "TUR",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
            {
                "id": "SWE",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            },
        ]
    }
    return jsonify(response)


@app.route("/economic-social-and-cultural-score")
def economic_social_and_cultural_score():
    response = {
        "datasets": [
            {
                "id": "FRA",
                "value": 1.6
            },
            {
                "id": "GBR",
                "value": 1.6
            },
            {
                "id": "ESP",
                "value": 1.6
            },
            {
                "id": "DOM",
                "value": 1.6
            },
            {
                "id": "JPN",
                "value": 1.6
            },
            {
                "id": "UKR",
                "value": 1.6
            },
            {
                "id": "TUR",
                "value": 1.6
            },
            {
                "id": "SWE",
                "value": 1.6
            },
        ]
    }
    return jsonify(response)


def execute_query_fetch_all(connection, query):
    cur = connection.cursor()
    cur.execute(query)
    response = cur.fetchall()
    cur.close()
    return response


def generate_country_learning_hours(warehouse_conn):
    average_tmins_by_country_query = """SELECT country_code, AVG(class_periods * learning_hours.average_mins)
                FROM learning_hours
                GROUP BY country_code
                ORDER BY country_code;"""
    return execute_query_fetch_all(warehouse_conn, average_tmins_by_country_query)


@app.route("/learning-hours-per-week")
def learning_hours_per_week():
    countries = request.args.get('countries', default=[])
    print(f"Arguments: {countries}")
    response = {
        "datasets": []
    }
    sql_response = generate_country_learning_hours(warehouse.connection)
    for item in sql_response:
        if len(countries) == 0 or item[0] in countries:
            response["datasets"].append({
                "country": item[0],
                "hours": round(item[1], None)
            })
    return jsonify(response)
