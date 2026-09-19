# Databricks notebook source
# MAGIC %md
# MAGIC # Landing -> Bronze ingestion (Auto Loader)
# MAGIC
# MAGIC Parameterized notebook executed once per source table by a Lakeflow job task (see `resources/adls_landing.job.yml`), after the matching `create_bronze_tables` task has created the table.
# MAGIC
# MAGIC Reads parquet files from `landing_path/<source>/<table>` and writes them into a Unity Catalog Delta table using Auto Loader (`cloudFiles`), running as a single `availableNow` micro-batch. The read schema is the same fixed, all-`STRING` schema (from `_table_schemas`) used to create the table, with schema evolution disabled — a new/renamed column in the source shows up in `_rescued_data` instead of silently altering Bronze.

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

# MAGIC %run ./_table_schemas

# COMMAND ----------

if table not in TABLE_COLUMNS:
    raise ValueError(f"No column definition for table '{table}' in _table_schemas.py")

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Same fixed, all-STRING schema used to create the table in create_bronze_tables.py.
source_schema = StructType([StructField(c, StringType()) for c in TABLE_COLUMNS[table]])

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "none")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(source_schema)
    .load(source_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)
