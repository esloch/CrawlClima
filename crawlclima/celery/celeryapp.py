# Create app celery  to start Crawlclima
from datetime import timedelta
from celery import Celery

app = Celery('crawlclima')

app.config_from_object('crawlclima.celery.celeryconfig')


# Celery Beat Scheduler
app.conf.beat_schedule = {
    'captura-chuva-cemaden': {
        'task': 'captura_chuva',
        'schedule': timedelta(minutes=1),
    },

}

if __name__ == '__main__':
    app.start()
