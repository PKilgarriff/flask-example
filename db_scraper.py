from warehouse_db import DataWarehouse
from country_db import CountryDatabase
from queries import Queries

warehouse = DataWarehouse()
queries = Queries()

country_codes = [
    "alb", "arg", "aus", "aut", "bel", "bgr", "bih", "blr", "bra", "brn", "can", "che", "chl", "col", "cri", "cze", "deu", "dnk", "dom", "esp", "est", "fin", "fra", "gbr", "geo", "grc", "hkg", "hrv", "hun", "idn", "irl", "isl", "isr", "ita", "jor", "jpn", "kaz", "kor", "ksv", "lbn", "ltu", "lux", "lva", "mac", "mar", "mda", "mex", "mkd", "mlt", "mne", "mys", "nld", "nor", "nzl", "pan", "per", "phl", "pol", "prt", "qat", "qaz", "qci", "qmr", "qrt", "rou", "rus", "sau", "sgp", "srb", "svk", "svn", "swe", "tap", "tha", "tur", "ukr", "ury", "usa", "vnm"
]

country_connections = {}
for country_code in country_codes:
    country_instance = CountryDatabase(country_code)
    country_connections[country_code] = country_instance.connection

queries.update_submission_times_table(
    warehouse.connection, country_connections)
queries.update_learning_hours_table(
    warehouse.connection, country_connections)
