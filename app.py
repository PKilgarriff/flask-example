from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    response = {"count": 1022}
    return jsonify(response)


@app.route("/submissions-over-time")
def submissions_over_time():
    response = {
        "datasets": [
            {
                "id": "Submissions",
                "data": [
                    {"x": "12:00", "y": 82},
                    {"x": "13:00", "y": 88},
                    {"x": "14:00", "y": 101},
                    {"x": "15:00", "y": 97},
                    {"x": "16:00", "y": 121},
                    {"x": "17:00", "y": 83},
                    {"x": "18:00", "y": 59}
                ]
            }
        ]
    }
    return jsonify(response)


@app.route("/early-education-and-belonging")
def early_education_and_belonging():
    response = {
        "dataset": [
            {
                "id": "GBR",
                "data": [
                    {
                        "x": 6,
                        "y": 1.1,
                        "submissions": 412
                    }
                ]
            }
        ]
    }
    return jsonify(response)


@app.route("/economic-social-and-cultural-score")
def economic_social_and_cultural_score():
    response = {
        "dataset": [
            {
                "id": "GBR",
                "value": 1.6
            }
        ]
    }
    return jsonify(response)


@app.route("/learning-hours-per-week")
def learning_hours_per_week():
    response = {
        "dataset": [
            {
                "country": "GBR",
                "hours": 1640
            }
        ]
    }
    return jsonify(response)
