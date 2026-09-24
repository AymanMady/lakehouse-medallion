# =============================================================================
#  Lakehouse Medallion — developer shortcuts
#  Using `>` instead of TAB as the recipe prefix (GNU Make >= 3.82)
# =============================================================================
.RECIPEPREFIX := >
.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose

## ---------------------------------------------------------------- Setup ----

help:  ## Show this help
> @grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
>   | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

init:  ## Create .env with freshly generated secrets
> @./scripts/bootstrap_env.sh

build:  ## Build all custom images (spark, airflow, dbt, python-tools)
> $(COMPOSE) build

## ------------------------------------------------------------ Lifecycle ----

up:  ## Start the stack (profiles come from COMPOSE_PROFILES in .env)
> $(COMPOSE) up -d
> @echo ""
> @echo "Stack starting. Run 'make health' in ~60s to verify."

up-core:  ## Start only MinIO + Postgres + Kafka + Kafka UI
> COMPOSE_PROFILES= $(COMPOSE) up -d

down:  ## Stop all containers (data volumes are kept)
> $(COMPOSE) --profile "*" down

restart:  ## Restart the whole stack
> $(MAKE) down && $(MAKE) up

clean:  ## DESTRUCTIVE: stop everything and delete all data volumes
> @read -p "This deletes MinIO, Postgres and Kafka data. Type 'yes': " c; \
>  [ "$$c" = "yes" ] && $(COMPOSE) --profile "*" down -v --remove-orphans || echo "aborted"

## ------------------------------------------------------- Observability ----

ps:  ## Show container status
> $(COMPOSE) ps

health:  ## Check that every running service actually answers
> @./scripts/health_check.sh

logs:  ## Tail all logs  (make logs s=kafka  -> only one service)
> $(COMPOSE) logs -f --tail=100 $(s)

smoke:  ## Phase 1 end-to-end check: Delta + MinIO + Metastore
> $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit \
>   --master "local[2]" /opt/lakehouse/scripts/smoke_test_delta.py

## ------------------------------------------------------------ Pipeline ----
#  Each step can be run on its own; `make pipeline` chains them, which is what
#  the Airflow DAG does too. RUN_ID ties together the monitoring rows that the
#  Spark jobs of one run record (Airflow passes its own run_id instead).

export RUN_ID := $(or $(RUN_ID),manual__$(shell date -u +%Y%m%dT%H%M%S))
p ?= small
t ?= orders
SPARK_SUBMIT := $(COMPOSE) exec -T -e PIPELINE_RUN_ID=$(RUN_ID) spark-master \
                /opt/spark/bin/spark-submit --master "local[2]"
DBT := $(COMPOSE) exec -T dbt dbt

generate:  ## Phase 2: generate the dataset  (make generate p=tiny|small|medium|large)
> $(COMPOSE) exec -T datagen python3 -m ingestion.generator.generate \
>   --preset $(p) --out /opt/lakehouse/data/raw

produce:  ## Phase 3: publish data/raw/*.jsonl to Kafka
> $(COMPOSE) exec -T datagen python3 -m ingestion.producers.kafka_producer \
>   --source /opt/lakehouse/data/raw

inspect:  ## Phase 3: show offsets and sample messages  (make inspect t=orders)
> $(COMPOSE) exec -T datagen python3 -m ingestion.consumers.inspect_topic --topic $(t)

bronze:  ## Phase 4: Kafka -> Bronze Delta tables
> $(SPARK_SUBMIT) /opt/lakehouse/spark/jobs/bronze/ingest_kafka.py --mode batch

silver:  ## Phases 5-6: Bronze -> Silver + quarantine
> $(SPARK_SUBMIT) /opt/lakehouse/spark/jobs/silver/bronze_to_silver.py

gold:  ## Phase 7: Silver -> Gold star schema
> $(SPARK_SUBMIT) /opt/lakehouse/spark/jobs/gold/build_star_schema.py

dbt-run:  ## Phase 8: build the dbt marts
> $(DBT) run

dbt-test:  ## Phase 8: run the dbt tests (sources, marts, singular tests)
> $(DBT) test

monitor:  ## Phase 11: health report - quality drift, Gold volume, freshness
> $(SPARK_SUBMIT) /opt/lakehouse/spark/jobs/monitoring/pipeline_report.py

pipeline:  ## Run every step, generate -> monitor, under one RUN_ID
> @echo "  run id: $(RUN_ID)"
> @for step in generate produce bronze silver gold dbt-run dbt-test monitor; do \
>   echo ""; echo "==> $$step"; \
>   $(MAKE) --no-print-directory $$step RUN_ID=$(RUN_ID) || exit 1; \
> done

reset:  ## DESTRUCTIVE: empty topics, lake and catalog (containers keep running)
> @./scripts/reset_data.sh

dag-trigger:  ## Phase 9: unpause and trigger the Airflow DAG
> $(COMPOSE) exec -T airflow-scheduler airflow dags unpause lakehouse_pipeline
> $(COMPOSE) exec -T airflow-scheduler airflow dags trigger lakehouse_pipeline

## -------------------------------------------------------------- Tests -----

test:  ## Phase 10: unit tests (pure Python, ~1 s)
> $(COMPOSE) exec -T -w /opt/lakehouse datagen python3 -m pytest -p no:cacheprovider -q tests/unit

test-integration:  ## Phase 10: Spark integration tests, in the Spark image (~1 min)
> $(COMPOSE) run --rm --no-deps -T -e SPARK_MODE=client -w /opt/lakehouse spark-master \
>   python3 -m pytest -p no:cacheprovider -q tests

lint:  ## Lint the Python code (needs ruff on the host: pip install ruff)
> ruff check .

## ------------------------------------------------------------- Shells -----

shell-spark:  ## Bash inside the Spark master container
> $(COMPOSE) exec spark-master bash

pyspark:  ## Interactive PySpark shell wired to Delta + MinIO
> $(COMPOSE) exec spark-master /opt/spark/bin/pyspark --master local[2]

spark-sql:  ## Interactive Spark SQL shell
> $(COMPOSE) exec spark-master /opt/spark/bin/spark-sql --master local[2]

shell-dbt:  ## Bash inside the dbt container
> $(COMPOSE) exec dbt bash

shell-gen:  ## Bash inside the data-generator container
> $(COMPOSE) exec datagen bash

psql:  ## PostgreSQL client
> $(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-lakehouse} -d $${POSTGRES_DB:-lakehouse}

## --------------------------------------------------------------- Kafka -----

topics:  ## List Kafka topics
> $(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

topic-describe:  ## Describe a topic  (make topic-describe t=orders)
> $(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $(t)

consume:  ## Read a topic from the beginning  (make consume t=orders)
> $(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
>   --bootstrap-server localhost:9092 --topic $(t) --from-beginning --max-messages 10

## --------------------------------------------------------------- MinIO -----

ls-lake:  ## List every object in the lakehouse bucket
> $(COMPOSE) run --rm --entrypoint sh minio-init -c \
>   'mc alias set lake "$$S3_ENDPOINT" "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null && mc ls -r "lake/$$LAKEHOUSE_BUCKET"'

## --------------------------------------------------------------- Creds -----

creds:  ## Print local UI credentials from .env
> @echo ""
> @echo "  MinIO    http://localhost:$$(grep '^MINIO_CONSOLE_PORT=' .env | cut -d= -f2)"
> @echo "           $$(grep '^MINIO_ROOT_USER=' .env | cut -d= -f2) / $$(grep '^MINIO_ROOT_PASSWORD=' .env | cut -d= -f2)"
> @echo "  Airflow  http://localhost:$$(grep '^AIRFLOW_WEB_PORT=' .env | cut -d= -f2)"
> @echo "           $$(grep '^AIRFLOW_ADMIN_USER=' .env | cut -d= -f2) / $$(grep '^AIRFLOW_ADMIN_PASSWORD=' .env | cut -d= -f2)"
> @echo "  Kafka UI http://localhost:$$(grep '^KAFKA_UI_PORT=' .env | cut -d= -f2)"
> @echo "  Spark    http://localhost:$$(grep '^SPARK_MASTER_UI_PORT=' .env | cut -d= -f2)"
> @echo ""

.PHONY: help init build up up-core down restart clean ps health logs smoke \
        generate produce inspect bronze silver gold dbt-run dbt-test monitor \
        pipeline reset dag-trigger test test-integration lint \
        shell-spark pyspark spark-sql shell-dbt shell-gen psql \
        topics topic-describe consume ls-lake creds
