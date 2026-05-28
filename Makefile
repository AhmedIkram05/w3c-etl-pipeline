AIRFLOW_DIR := airflow
COMPOSE_FILE := $(AIRFLOW_DIR)/docker-compose.yaml
# Auto-detect Docker socket path (macOS vs Linux)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  DOCKER_SOCKET_PATH := $(shell test -e $(HOME)/.docker/run/docker.sock && echo "$(HOME)/.docker/run/docker.sock" || echo "/var/run/docker.sock")
else
  DOCKER_SOCKET_PATH := /var/run/docker.sock
endif
COMPOSE_CMD := DOCKER_SOCKET_PATH=$(DOCKER_SOCKET_PATH) docker compose -f $(COMPOSE_FILE)

.PHONY: build up down restart logs ps clean validate db-reset test-e2e

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

# ── Database reset ───────────────────────────────────────────────

db-reset:
	@echo "=== Resetting w3c_warehouse database ==="
	$(COMPOSE_CMD) exec -T postgres psql -U airflow -d airflow -c "DROP DATABASE IF EXISTS w3c_warehouse;"
	$(COMPOSE_CMD) exec -T postgres psql -U airflow -d airflow -c "CREATE DATABASE w3c_warehouse;"
	@echo "w3c_warehouse recreated (empty)."

# ── End-to-end test ──────────────────────────────────────────────

test-e2e:
	@echo "=== W3C ETL Pipeline — End-to-End Test ==="
	@echo ""
	@echo "[1/2] Resetting database..."
	$(MAKE) db-reset
	@echo ""
	@echo "[2/2] Triggering DAG..."
	$(COMPOSE_CMD) exec airflow-scheduler airflow dags trigger Process_W3C_Data
	@echo ""
	@echo "Monitoring DAG run (300s timeout)..."
	@echo "Checking DAG runs every 15s..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		echo "--- Wait $$(( $$i * 15 ))s ---"; \
		RUN_STATE=$$($(COMPOSE_CMD) exec -T postgres psql -U airflow -d airflow -t -c "SELECT state FROM dag_run WHERE dag_id='Process_W3C_Data' ORDER BY execution_date DESC LIMIT 1" 2>/dev/null | tr -d ' ' || echo ""); \
		if [ "$$RUN_STATE" = "success" ]; then \
			echo "DAG completed successfully!"; \
			break; \
		elif [ "$$RUN_STATE" = "failed" ]; then \
			echo "DAG FAILED!"; \
			$(COMPOSE_CMD) logs --tail=50 airflow-scheduler 2>/dev/null || true; \
			exit 1; \
		fi; \
		sleep 15; \
	done
	@echo ""
	@echo "=== Verifying results ==="
	$(COMPOSE_CMD) exec -T postgres psql -U airflow -d w3c_warehouse -c "SELECT 'fact_webrequest' AS tbl, COUNT(*) AS rows FROM public.fact_webrequest UNION ALL SELECT 'dim_date', COUNT(*) FROM public.dim_date UNION ALL SELECT 'dim_time', COUNT(*) FROM public.dim_time UNION ALL SELECT 'dim_page', COUNT(*) FROM public.dim_page UNION ALL SELECT 'dim_method', COUNT(*) FROM public.dim_method UNION ALL SELECT 'dim_status', COUNT(*) FROM public.dim_status UNION ALL SELECT 'dim_referrer', COUNT(*) FROM public.dim_referrer UNION ALL SELECT 'dim_geolocation', COUNT(*) FROM public.dim_geolocation UNION ALL SELECT 'dim_useragent', COUNT(*) FROM public.dim_useragent UNION ALL SELECT 'dim_visitortype', COUNT(*) FROM public.dim_visitortype UNION ALL SELECT 'dim_visit_buckets', COUNT(*) FROM public.dim_visit_buckets ORDER BY tbl;"
	@echo ""
	@echo "=== Pipeline complete ==="
