# Variables
PYTHON := python
VENV_DIR := .pyspark-env
REQUIREMENTS := requirements.txt
POSTGRES_CONTAINER := postgres-container
POSTGRES_DB := postgres
POSTGRES_USER := postgres
SQL_FILE := chinook.sql
CASSANDRA_CONTAINER := cassandra-container
CASSANDRA_SCHEMA := schema.cql

NOTEBOOK := data-migration-pipeline.ipynb
OUTPUT_FILE := data-migration-pipeline-output.ipynb

setup:
	$(PYTHON) -m venv $(VENV_DIR)
	pip install -r $(REQUIREMENTS)

compose:
	docker-compose up -d

load-postgres:
	docker exec -i $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) < $(SQL_FILE)

load-cassandra:
	Get-content $(CASSANDRA_SCHEMA) | docker exec -i $(CASSANDRA_CONTAINER) cqlsh

run-notebook:
	jupyter nbconvert --to notebook --execute $(NOTEBOOK) --output $(OUTPUT_FILE)

clean:
	docker-compose down
