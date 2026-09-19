# Databricks notebook source
# MAGIC %md
# MAGIC # Shared column definitions — ADLS landing -> Bronze
# MAGIC
# MAGIC Single source of truth for the two notebooks in this job:
# MAGIC `create_bronze_tables.py` (DDL) and `ingest_autoloader.py` (Auto Loader).
# MAGIC Business columns are all `STRING` (raw as landed, no type inference) and
# MAGIC every table gets the same standardized control columns appended at the end.

# COMMAND ----------

TABLE_COLUMNS = {
    "cuotas": [
        "numero_credito",
        "numero_cuota",
        "fecha_vencimiento",
        "monto_cuota",
        "monto_capital",
        "monto_interes",
        "estado_cuota",
        "fecha_creacion",
        "fecha_modificacion",
    ],
    "pagos": [
        "numero_credito",
        "numero_cuota",
        "fecha_pago",
        "monto_pagado",
        "medio_pago",
        "fecha_creacion",
        "fecha_modificacion",
    ],
    "gestiones_cobranza": [
        "numero_credito",
        "fecha_gestion",
        "tipo_gestion",
        "resultado",
        "dias_mora_al_momento",
        "gestor",
        "fecha_creacion",
        "fecha_modificacion",
    ],
}

CONTROL_COLUMNS = {
    "_ingested_at": "TIMESTAMP",
    "_source_file": "STRING",
    "_rescued_data": "STRING",
}
