__author__ = "fccoelho"

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

PSQL_URI = {
    "database": os.getenv("PSQL_DB"),
    "user": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "host": os.getenv("PSQL_HOST"),
    "port": os.getenv("PSQL_PORT"),
}
