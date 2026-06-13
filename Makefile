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

.PHONY: start clean rebuild test

# Start all containers (build then up)
start:
	$(COMPOSE_CMD) build
	$(COMPOSE_CMD) up -d

# Delete all containers, volumes, local images, and runtime state artifacts
clean:
	$(COMPOSE_CMD) down -v --rmi local
	rm -rf $(AIRFLOW_DIR)/logs/*
	rm -rf $(AIRFLOW_DIR)/spark/delta/*

# Delete all containers then rebuild and start
rebuild:
	$(MAKE) clean
	$(MAKE) start

# Copy DLT source modules into container (not mounted by default) then run unit tests
# Excluded markers:
#   terraform    — needs terraform binary on PATH (not in Docker)
#   dbt_compile  — needs 'dbt compile --profile w3c_azure' output (run in CI)
#   dag_integrity — now passes in Docker (conftest avoids namespace shadowing)
#   integration  — reserved for future Azure-dependent end-to-end tests
test:
	$(COMPOSE_CMD) cp $(AIRFLOW_DIR)/spark/databricks/ airflow-scheduler:/opt/airflow/spark/databricks/
	$(COMPOSE_CMD) exec -T airflow-scheduler bash -c 'pip install --no-cache-dir -r /opt/airflow/tests/requirements-test.txt && PYTHONPATH=/opt/airflow/spark/jobs:/opt/airflow/spark/databricks:/opt:/opt/airflow/plugins:$$PYTHONPATH python -m pytest /opt/airflow/tests/ -v --tb=short -m "not integration and not terraform and not dbt_compile"'
