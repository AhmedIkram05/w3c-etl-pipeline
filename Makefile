AIRFLOW_DIR := airflow
COMPOSE_FILE := $(AIRFLOW_DIR)/docker-compose.yaml
# Auto-detect Docker socket path (macOS vs Linux)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  DOCKER_SOCKET_PATH := $(shell test -e $(HOME)/.docker/run/docker.sock && echo "$(HOME)/.docker/run/docker.sock" || echo "/var/run/docker.sock")
else
  DOCKER_SOCKET_PATH := /var/run/docker.sock
endif
COMPOSE_CMD := DOCKER_SOCKET_PATH=$(DOCKER_SOCKET_PATH) docker compose --env-file $(AIRFLOW_DIR)/.env -f $(COMPOSE_FILE)

.PHONY: start clean rebuild test db-reset

# Start all containers (build then up)
start:
	$(COMPOSE_CMD) build
	$(COMPOSE_CMD) up -d

# Delete all containers, volumes, and local images
clean:
	$(COMPOSE_CMD) down -v --rmi local

# Delete all containers then rebuild and start
rebuild:
	$(MAKE) clean
	$(MAKE) start

# Run unit tests (excludes integration and DAG integrity tests which require active pipeline runs/setup)
test:
	$(COMPOSE_CMD) exec -T airflow-scheduler bash -c 'pip install --no-cache-dir -r /opt/airflow/tests/requirements-test.txt && PYTHONPATH=/opt/airflow/spark/jobs:/opt:/opt/airflow/plugins:$$PYTHONPATH python -m pytest /opt/airflow/tests/ -v --tb=short -m "not integration and not dag_integrity"'

# Reset the database
db-reset:
	@echo "=== Resetting w3c_warehouse database ==="
	$(COMPOSE_CMD) exec -T postgres psql -U airflow -d airflow -c "DROP DATABASE IF EXISTS w3c_warehouse;"
	$(COMPOSE_CMD) exec -T postgres psql -U airflow -d airflow -c "CREATE DATABASE w3c_warehouse;"
	@echo "w3c_warehouse recreated (empty)."
