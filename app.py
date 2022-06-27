from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    return jsonify({
        "count": "1022"
    })


@app.route("/submissions-over-time")
def submissions_over_time():
    return jsonify({
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
    })


@app.route("/early-education-and-belonging")
def early_education_and_belonging():
    return jsonify({
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
    })


@app.route("/economic-social-and-cultural-score")
def economic_social_and_cultural_score():
    return jsonify({
        "dataset": [
            {
                "id": "GBR",
                "value": 1.6
            }
        ]
    })


@app.route("/learning-hours-per-week")
def learning_hours_per_week():
    return jsonify({
        "dataset": [
            {
                "country": "GBR",
                "hours": 1640
            }
        ]
    })
