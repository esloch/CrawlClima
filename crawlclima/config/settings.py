import os

from dotenv import find_dotenv, load_dotenv

# local = os.path.dirname(
#     os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# )

load_dotenv(find_dotenv())


base_url = 'http://observatorio.inweb.org.br/dengueapp/api/1.0/totais'
token = 'XXXXX'

db_config = {
    'database': os.getenv('PSQL_DB'),
    'user': os.getenv('PSQL_USER'),
    'password': os.getenv('PSQL_PASSWORD'),
    'host': os.getenv('PSQL_HOST'),
    'port': os.getenv('PSQL_PORT'),
}
