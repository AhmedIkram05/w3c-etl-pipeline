AIRFLOW_DIR := airflow
COMPOSE_FILE := $(AIRFLOW_DIR)/docker-compose.yaml
DOCKER_CONTEXT := $(AIRFLOW_DIR)

# Auto-detect Docker socket path (macOS vs Linux)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  DOCKER_SOCKET_PATH := $(shell test -e $(HOME)/.docker/run/docker.sock && echo "$(HOME)/.docker/run/docker.sock" || echo "/var/run/docker.sock")
else
  DOCKER_SOCKET_PATH := /var/run/docker.sock
endif
COMPOSE_CMD := DOCKER_SOCKET_PATH=$(DOCKER_SOCKET_PATH) docker compose -f $(COMPOSE_FILE)

.PHONY: build up down restart logs ps clean validate

build:
	$(COMPOSE_CMD) build

up:
	$(COMPOSE_CMD) up -d

down:
	$(COMPOSE_CMD) down

restart: down up

logs:
	$(COMPOSE_CMD) logs -f

ps:
	$(COMPOSE_CMD) ps

clean:
	$(COMPOSE_CMD) down -v --rmi local

validate:
	$(COMPOSE_CMD) config --quiet && echo "Compose file: valid"
	python3 -c "import json; json.load(open('$(AIRFLOW_DIR)/grafana/dashboards/airflow-etl-overview.json')); json.load(open('$(AIRFLOW_DIR)/grafana/dashboards/container-metrics.json')); print('Dashboards: valid JSON')"
