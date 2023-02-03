# Create app celery  to start Crawlclima
from celery import Celery
from celery.schedules import crontab

app = Celery("crawlclima")

app.config_from_object("crawlclima.config.celeryconfig")


# Celery Beat Scheduler
app.conf.beat_schedule = {
    "captura-temperatura-redemet": {
        "task": "captura_temperatura",
        "schedule": crontab(minute="*", hour="22"),
    },
    # "captura-tweets": {
    #     "task": "captura_tweets",
    #     "schedule": crontab(minute="*", hour="23"),
    # },
}

if __name__ == "__main__":
    app.start()
