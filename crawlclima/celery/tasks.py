import csv
import os
import sys
import time
from datetime import datetime, timedelta, date
from loguru import logger
from io import StringIO

from pathlib import Path
from utils.cemaden import coleta_dados_cemaden

import psycopg2
import requests
from celery.utils.log import get_task_logger

from crawlclima.utils import cemaden
from crawlclima.config.settings import base_url, db_config, token
from crawlclima.celery.celeryapp import app
from crawlclima.utils.models import save
from crawlclima.utils.rmet import capture_date_range


work_dir = os.getcwd()
route_abs = os.path.dirname(os.path.abspath(work_dir))
sys.path.insert(0, route_abs)

log_path = Path(__file__).parent / 'logs' / 'tasks.log'
logger.add(log_path, colorize=True, retention=timedelta(days=15))

def get_connection():
    try:
        conn = psycopg2.connect(**db_config)

    except Exception as e:
        logger.error('Unable to connect to Postgresql: {}'.format(e))
        raise e
    return conn


@app.task
def mock(t):
    time.sleep(t)
    return 'done'


@app.task(bind=True)
def fetch_redemet(self, station, date):
    ...


@app.task(bind=True)
def pega_tweets(self, inicio, fim, cidades=None, CID10='A90'):
    ...


@app.task(bind=True)
def dados_cemaden(ufs: list = ['PR', 'RJ', 'MG', 'ES', 'CE', 'SP']):
    """
    Fetch a week of data from cemaden
    Once a week go over the entire year to fill in possible gaps in the local database
    requires celery worker to be up and running
    but this script will actually be executed by cron
    """

    today = datetime.fromordinal(date.today().toordinal())
    week_ago = datetime.fromordinal(date.today().toordinal()) - timedelta(8)
    year_start = datetime(date.today().year, 1, 1)

    date_from = week_ago if today.isoweekday() != 5 else year_start

    for uf in ufs:
        try:
            coleta_dados_cemaden(uf, date_from, today, 'uf')
            logger.info(f'🌧️ Data for {uf} collected.')
        except Exception as e:
            logger.error(f'🔴 Task `captura_chuva` for {uf} failed.\n{e}')


@app.task(name='captura_chuva', bind=True)
def captura_chuva():
    ufs = ['PR', 'RJ', 'MG', 'ES', 'CE', 'SP']
    dados_cemaden(ufs)
