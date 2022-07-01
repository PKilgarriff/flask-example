from flask import Flask, jsonify, request
from flask_cors import CORS
from warehouse_db import DataWarehouse
from json_builder import JSONBuilder

app = Flask(__name__)
warehouse = DataWarehouse()
json_builder = JSONBuilder(warehouse.connection)

CORS(app)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    response = json_builder.count_submissions()
    return jsonify(response)


@app.route("/submissions-over-time")
def submissions_over_time():
    response = json_builder.submissions_by_hour()
    return jsonify(response)


@app.route("/early-education-and-belonging")
def early_education_and_belonging():
    countries = request.args.get('countries', default=[])
    response = json_builder.early_education_and_belonging(countries)
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
    response = json_builder.learning_hours(countries)
    return jsonify(response)
