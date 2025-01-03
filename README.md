# data-migration-pipeline


## Quick Start

1. Clone the repo:

```
$ https://gitlab.com/tin3ga/data-migration-pipeline.git
$ cd data-migration-pipeline
```

2. Initialize and activate a virtualenv:

```
# windows
$ python -m venv .pyspark-env
$ .\.pyspark-env\Scripts\Activate

# mac/linux
$ python -m venv .pyspark-env
$ source .pyspark-env/bin/activate or . .pyspark-env/bin/activate
```

3. Install the dependencies:

```
$ pip install -r requirements.txt
```

4. Open Jupyter Notebook

```
jupyter-notebook
```

5. Populate postgres db 
```
docker exec -i postgres-container psql -U postgres -d postgres < .\chinook.sql
```

6. Create casandra tables

```
cat .\schema.cql | docker exec -i cassandra-container cqlsh
```


## Postgres database sample
The sample PostgreSQL database schema is sourced from [morenoh149/postgresDBSamples](https://github.com/morenoh149/postgresDBSamples.git).