import logging
import json
from json import JSONDecodeError

logging.basicConfig(level=logging.INFO)
lgr = logging.getLogger(__name__)


def get_load_params_from_json(file_path):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config["tables"]
    except JSONDecodeError as e:
        lgr.error("Error encountered when decoding file, The json file is empty")


def load_to_spark(spark, sql_query, psql_server, psql_port, psql_dbname, psql_username, psql_password):
    URL = F"jdbc:postgresql://{psql_server}:{psql_port}/{psql_dbname}"
    try:
        df = spark.read.format("jdbc") \
            .option("url", URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", sql_query) \
            .option("user", psql_username) \
            .option("password", psql_password) \
            .load()
        lgr.info(f"Successfully loaded data from {psql_dbname} into spark.")
        return df
    except Exception as e:
        lgr.error(f"Error encountered when loading data into spark: {e}")


def write_to_cassandra(df, cassandra_table, postgres_table, keyspace) -> bool:
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=cassandra_table, keyspace=keyspace) \
            .mode("append") \
            .save()
        lgr.info(f"Data written successfully from {postgres_table} into keyspace {keyspace}.")
        return True
    except Exception as e:
        lgr.error(f"Error encountered when writing data from {postgres_table} into keyspace {keyspace}: {e}")
        return False
