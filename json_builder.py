from psycopg2 import OperationalError
from random import randrange


class JSONBuilder:
    def __init__(self, connection):
        self.connection = connection

# Generic SQL execution method

    def select_many(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        finally:
            if cursor != None:
                cursor.close()

# Specific SQL Query method wrappers

    def query_learning_hours_by_country(self):
        average_tmins_by_country_query = """
            SELECT country_code, AVG(class_periods * average_mins)
            FROM learning_hours
            GROUP BY country_code
            ORDER BY country_code;
            """
        return self.select_many(average_tmins_by_country_query)

    def query_submissions_by_hour(self):
        count_of_submissions_by_hour_query = """
            SELECT extract(hour FROM created_at) AS hour, count(id)
            FROM submission_times
            GROUP BY hour
            ORDER BY hour;
            """
        return self.select_many(count_of_submissions_by_hour_query)

    def query_early_education_and_belonging(self):
        early_education_and_belonging_query = """
            SELECT country_code, AVG(durecec), AVG(belong), count(country_code)
            FROM early_education_and_belonging
            GROUP BY country_code
            ORDER BY country_code;
            """
        return self.select_many(early_education_and_belonging_query)

    def query_submissions_count(self):
        submissions_count_query = """SELECT count(*) FROM submission_times;"""
        return self.select_many(submissions_count_query)

# Methods available to Endpoints in the Flask Application - these build JSON

    def count_submissions(self):
        count = self.query_submissions_count()
        return {"count": sum(count[0])}

    def submissions_by_hour(self):
        sql_response = self.query_submissions_by_hour()
        data = []
        for item in sql_response:
            hour_string = f"{int(item[0]):02d}:00"
            data.append({
                "x": hour_string,
                "y": item[1]
            })
        return {
            "datasets": [
                {
                    "id": "Submissions",
                    "data": data
                }
            ]
        }

    def learning_hours(self, countries):
        sql_response = self.query_learning_hours_by_country()
        datasets = []
        for item in sql_response:
            if len(countries) == 0 or item[0] in countries:
                datasets.append({
                    "country": item[0],
                    "hours": round(item[1] / 60, None)
                })
        return {"datasets": datasets}

    def early_education_and_belonging(self, countries):
        sql_response = self.query_early_education_and_belonging()
        datasets = []
        for item in sql_response:
            if len(countries) == 0 or item[0] in countries:
                datasets.append({
                    "id": item[0],
                    "data": [
                        {
                            "x": round(item[1], None),
                            "y": round(item[2], 2),
                            "submissions": int(item[3])
                        }
                    ]
                })
        return {"datasets": datasets}

    def economic_social_and_cultural_score(self, countries):
        country_codes = [
            "alb", "arg", "aus", "aut", "bel", "bgr", "bih", "blr", "bra", "brn", "can", "che", "chl", "col", "cri", "cze", "deu", "dnk", "dom", "esp", "est", "fin", "fra", "gbr", "geo", "grc", "hkg", "hrv", "hun", "idn", "irl", "isl", "isr", "ita", "jor", "jpn", "kaz", "kor", "ksv", "lbn", "ltu", "lux", "lva", "mac", "mar", "mda", "mex", "mkd", "mlt", "mne", "mys", "nld", "nor", "nzl", "pan", "per", "phl", "pol", "prt", "qat", "qaz", "qci", "qmr", "qrt", "rou", "rus", "sau", "sgp", "srb", "svk", "svn", "swe", "tap", "tha", "tur", "ukr", "ury", "usa", "vnm"
        ]
        all_countries = [country.upper() for country in country_codes]
        if len(countries) == 0:
            example_countries = ["FRA", "GBR", "ESP",
                                 "DOM", "JPN", "UKR", "TUR", "SWE"]
        datasets = []
        for country in example_countries:
            datasets.append({
                "id": country,
                "value": randrange(-80, 40) / 10
            })
        return {"datasets": datasets}
