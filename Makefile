SHELL:=/bin/bash

include .env

# options: dev, prod
ENV:=$(ENV)
HOST_UID:=$(HOST_UID)
HOST_GID:=$(HOST_GID)
SERVICE:=
SERVICES:=


# PREPARE ENVIRONMENT
.PHONY:prepare-env
prepare-env:
	envsubst < env.tpl > .env

# -----------

# LINTING CODE
.PHONY: lint
lint: ## formatting linter with poetry
	pre-commit install
	pre-commit run --all-files

# -----------


.PHONY: tests
tests:
	python -m unittest tests/unit/*.py


# DOCKER
DOCKER=docker-compose \
	--env-file .env \
	--project-name $(ENV)_crawlclima \
	--file containers/docker-compose.yaml


.PHONY:container-build
container-build:
	$(DOCKER) build ${SERVICES}

.PHONY:container-start
container-start:
	$(DOCKER) up -d ${SERVICES}

.PHONY:container-logs-follow
container-logs-follow:
	$(DOCKER) logs --follow --tail 300 ${SERVICES}

.PHONY:container-stop
container-stop:
	$(DOCKER) down -v --remove-orphans

.PHONY: container-wait
container-wait:
	ENV=${ENV} timeout 90 ./containers/scripts/healthcheck.sh ${SERVICE}

.PHONY: container-wait-all
container-wait-all:
	$(MAKE) docker-wait ENV=${ENV} SERVICE="${ENV}_crawlclima"


# Python
.PHONY: clean
clean: ## clean all artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr .idea/
	rm -fr */.eggs
	rm -fr db
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -fr {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '*.ipynb_checkpoints' -exec rm -rf {} +
	find . -name '*.pytest_cache' -exec rm -rf {} +
