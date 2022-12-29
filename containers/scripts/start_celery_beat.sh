#!/usr/bin/env bash

echo "Starting celery worker"

exec celery --workdir /opt/services/crawlclima --config config/celeryconfig -A tasks worker -B --loglevel=DEBUG -s /tmp/celerybeat-schedule --pidfile /tmp/celerybeat.pid
