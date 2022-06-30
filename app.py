from flask import Flask, jsonify, request
from flask_cors import CORS
from warehouse_db import DataWarehouse
from queries import Queries

app = Flask(__name__)
warehouse = DataWarehouse()
queries = Queries()

CORS(app)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    response = queries.count_submissions_json(warehouse.connection)
    return jsonify(response)


@app.route("/submissions-over-time")
def submissions_over_time():
    response = queries.submissions_by_hour_json(warehouse.connection)
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


@app.route("/learning-hours-per-week")
def learning_hours_per_week():
    countries = request.args.get('countries', default=[])
    response = queries.learning_hours_json(
        warehouse.connection, countries)
    return jsonify(response)
