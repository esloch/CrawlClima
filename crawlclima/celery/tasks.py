from pathlib import Path
from loguru import logger
from crawlclima.celery.celeryapp import app
from crawlclima.utils.models import find_all
from datetime import datetime, timedelta, date
from crawlclima.utils.rmet import fetch_redemet
from crawlclima.utils.cemaden import coleta_dados_cemaden
from crawlclima.captura.tweets import fetch_tweets, chunk, municipios

log_path = Path(__file__).parent / 'logs' / 'tasks.log'
logger.add(log_path, colorize=True, retention=timedelta(days=15))


# Tasks executed by Celery Beat:

@app.task(name='captura_temperatura', bind=True)
def pega_temperatura():
    today, _, year_start = dates()

    yesterday = today - timedelta(days=1)

    rows = find_all(schema='Municipio', table='Estacao_wu')
    stations = [row['estacao_id'] for row in rows]

    day = year_start if today.isoweekday() == 5 else yesterday

    for station in stations:
        fetch_redemet(station, day)
        logger.info(f'🌡️ Data from {station} fetched for day {day}')


@app.task(name='captura_tweets', bind=True)
def pega_tweets(self):
    """
    Fetch a week of tweets
    Once a week go over the entire year to fill in possible gaps in the local database
    requires celery worker to be up and running
    but this script will actually be executed by cron
    """
    today, week_ago, year_start = dates()

    if today.isoweekday() == 5:
        date_start = year_start
    else:
        date_start = week_ago

    for cidades in chunk(municipios, 50):
        fetch_tweets(
            self, 
            date_start.isoformat(), 
            today.isoformat(), 
            cidades, 
            'A90'
        )


@app.task(name='captura_chuva', bind=True)
def captura_chuva(self):
    ufs = ['PR', 'RJ', 'MG', 'ES', 'CE', 'SP']
    dados_cemaden(self, ufs)


####


@app.task(bind=True)
def dados_cemaden(self, ufs: list = ['PR', 'RJ', 'MG', 'ES', 'CE', 'SP']):
    """
    Fetch a week of data from cemaden
    Once a week go over the entire year to fill in possible gaps in the local database
    requires celery worker to be up and running
    but this script will actually be executed by celery beat
    """
    today, week_ago, year_start = dates()

    date_from = week_ago if today.isoweekday() != 5 else year_start
    for uf in ufs:
        try:
            coleta_dados_cemaden(self, uf, date_from, today, by='uf')
            logger.info(f'🌧️ Data for {uf} collected.')
        except Exception as e:
            logger.error(f'🔴 Task `captura_chuva` for {uf} failed.\n{e}')


def dates():
    today = datetime.fromordinal(date.today().toordinal())
    week_ago = datetime.fromordinal(date.today().toordinal()) - timedelta(8)
    year_start = datetime(date.today().year, 1, 1)
    return today, week_ago, year_start
