# Databricks notebook source
# MAGIC %md
# MAGIC # Landing -> Bronze DDL
# MAGIC
# MAGIC Creates one Bronze table explicitly, ahead of Auto Loader ever writing to it:
# MAGIC all source columns as `STRING` (raw as landed) plus the standardized control
# MAGIC columns from `_table_schemas`. Run once per source table by a Lakeflow job
# MAGIC task, before the matching `ingest_autoloader` task (see
# MAGIC `resources/adls_landing.job.yml`).
# MAGIC
# MAGIC Existing tables are left untouched (`CREATE TABLE IF NOT EXISTS`) so this is
# MAGIC safe to run on every job execution.
# MAGIC
# MAGIC Tables are append-only (`delta.appendOnly = true`): Auto Loader is the only
# MAGIC writer and only ever appends, so `UPDATE`/`DELETE`/`MERGE` are rejected at the
# MAGIC table level.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("table", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")

# COMMAND ----------

# MAGIC %run ./_table_schemas

# COMMAND ----------

if table not in TABLE_COLUMNS:
    raise ValueError(f"No column definition for table '{table}' in _table_schemas.py")

target_table = f"`{catalog}`.`{schema}`.`{table}`"

column_defs = [f"`{c}` STRING" for c in TABLE_COLUMNS[table]]
column_defs += [f"`{c}` {t}" for c, t in CONTROL_COLUMNS.items()]

ddl = (
    "CREATE TABLE IF NOT EXISTS {target} (\n  {cols}\n)\n"
    "USING DELTA\n"
    "TBLPROPERTIES ('delta.appendOnly' = 'true')"
).format(target=target_table, cols=",\n  ".join(column_defs))

print(ddl)

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(ddl)
