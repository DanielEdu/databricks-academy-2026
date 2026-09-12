# Databricks notebook source
# MAGIC %md
# MAGIC # Landing -> Bronze ingestion (Auto Loader)
# MAGIC
# MAGIC Parameterized notebook executed once per source table by a Lakeflow job task (see `resources/adls_landing.job.yml`).
# MAGIC
# MAGIC Reads parquet files from `landing_path/<source>/<table>` and writes them into a Unity Catalog Delta table using Auto Loader (`cloudFiles`), running as a single `availableNow` micro-batch.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("source", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("checkpoint_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source = dbutils.widgets.get("source")
table = dbutils.widgets.get("table")
landing_path = dbutils.widgets.get("landing_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

target_table = f"`{catalog}`.`{schema}`.`{table}`"
source_path = f"{landing_path.rstrip('/')}/{source}/{table}/"
schema_location = f"{checkpoint_path.rstrip('/')}/{schema}/{table}/_schema"
checkpoint_location = f"{checkpoint_path.rstrip('/')}/{schema}/{table}/_checkpoint"

print(f"source_path: {source_path}")
print(f"target_table: {target_table}")
print(f"schema_location: {schema_location}")
print(f"checkpoint_location: {checkpoint_location}")

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)
