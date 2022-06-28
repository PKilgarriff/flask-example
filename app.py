from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2 import OperationalError

app = Flask(__name__)
CORS(app)

country_codes = [
    "alb",
    "arg",
    "aus",
    "aut",
    "bel",
    "bgr",
    "bih",
    "blr",
    "bra",
    "brn",
    "can",
    "che",
    "chl",
    "col",
    "cri",
    "cze",
    "deu",
    "dnk",
    "dom",
    "esp",
    "est",
    "fin",
    "fra",
    "gbr",
    "geo",
    "grc",
    "hkg",
    "hrv",
    "hun",
    "idn",
    "irl",
    "isl",
    "isr",
    "ita",
    "jor",
    "jpn",
    "kaz",
    "kor",
    "ksv",
    "lbn",
    "ltu",
    "lux",
    "lva",
    "mac",
    "mar",
    "mda",
    "mex",
    "mkd",
    "mlt",
    "mne",
    "mys",
    "nld",
    "nor",
    "nzl",
    "pan",
    "per",
    "phl",
    "pol",
    "prt",
    "qat",
    "qaz",
    "qci",
    "qmr",
    "qrt",
    "rou",
    "rus",
    "sau",
    "sgp",
    "srb",
    "svk",
    "svn",
    "swe",
    "tap",
    "tha",
    "tur",
    "ukr",
    "ury",
    "usa",
    "vnm",
]


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_country_database_connection(country_code):
    # Postgres username, password, and database name
    # INSERT YOUR DB ADDRESS IF IT'S NOT ON PANOPLY
    POSTGRES_ADDRESS = f"seta-{country_code}.cvcpj1fhj3k9.us-east-2.rds.amazonaws.com"
    POSTGRES_PORT = '5432'
    POSTGRES_USERNAME = 'seta'
    POSTGRES_PASSWORD = 'defaultUnsafePassword'  # Data is public
    POSTGRES_DBNAME = country_code  # The country code that the database is named after
    return create_connection(POSTGRES_DBNAME, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_ADDRESS, POSTGRES_PORT)


country_connections = {}
for country_code in country_codes:
    print(f"Generating Connection for {country_code}")
    country_connections[country_code] = create_country_database_connection(
        country_code)


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def get_submissions_count():
    count_ids = []
    for connection in list(country_connections.values()):
        count_ids_query = "SELECT COUNT(id) FROM responses"
        [actual_count] = execute_read_query(connection, count_ids_query)
        count_ids.append(actual_count[0])

    total = 0
    for count in count_ids:
        total += count

    return total


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/submissions")
def count_submissions():
    response = {"count": get_submissions_count()}
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
    response = {
        "datasets": [
            {
                "country": "FRA",
                "hours": 1640
            },
            {
                "country": "GBR",
                "hours": 1640
            }
        ]
    }
    return jsonify(response)
