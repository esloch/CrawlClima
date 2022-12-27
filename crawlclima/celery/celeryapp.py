# Create app celery  to start Crawlclima
from celery.schedules import crontab
from datetime import timedelta
from celery import Celery

app = Celery('crawlclima')

app.config_from_object('crawlclima.celery.celeryconfig')


# Celery Beat Scheduler
app.conf.beat_schedule = {
    'captura-chuva-cemaden': {
        'task': 'captura_chuva',
        'schedule': crontab(),
    },
    'captura-temperatura-redmet': {
        'task': 'captura_temperatura',
        'schedule': crontab(),
    },
    'captura-tweets': {
        'task': 'captura_tweets',
        'schedule': crontab(),
    },
}

if __name__ == '__main__':
    app.start()
