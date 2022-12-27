__author__ = 'fccoelho'

import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


INWEB_URL = 'http://observatorio.inweb.org.br/dengueapp/api/1.0/totais'
INWEB_TOKEN = 'XXXXX'

DB_CONNECTION = {
    'database': os.getenv('PSQL_DB'),
    'user': os.getenv('PSQL_USER'),
    'password': os.getenv('PSQL_PASSWORD'),
    'host': os.getenv('PSQL_HOST'),
    'port': os.getenv('PSQL_PORT'),
}


"""
Dados para acesso aos Webservices do CEMADEN
"""
CEMADEN_KEY = 'bc10602ea62759fab1578f8eb1ff6f7abbf8678d'
CEMADEN_DADOS_REDE = 'http://150.163.255.246:18383/dados_rede'
CEMADEN_DADOS_PCD = 'http://150.163.255.246:18383/dados_pcd'
