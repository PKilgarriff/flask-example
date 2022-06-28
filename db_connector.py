import psycopg2
from psycopg2 import OperationalError

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
