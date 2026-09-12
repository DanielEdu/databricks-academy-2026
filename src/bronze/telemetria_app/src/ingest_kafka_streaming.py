# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka (Event Hubs) -> Bronze streaming ingestion
# MAGIC
# MAGIC Reads the telemetry event topic `wizard.lending.eventos-app` from Azure Event
# MAGIC Hubs via its Kafka-compatible endpoint, flattens the event contract (Sesion 8 /
# MAGIC Sesion 11) into top-level string columns, keeps the Kafka record metadata as a
# MAGIC `_metadata` JSON column, and stamps `_ingested_at` with the time the record
# MAGIC landed in Bronze.
# MAGIC
# MAGIC Executed by a job-level `continuous` trigger (see `resources/telemetria_app.job.yml`).
# MAGIC Serverless compute only supports bounded Structured Streaming triggers, so this
# MAGIC query uses `trigger(availableNow=True)`; the job scheduler restarts the task as
# MAGIC soon as it finishes, and the checkpoint below ensures no offset is reprocessed
# MAGIC across restarts.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("kafka_topic", "")
dbutils.widgets.text("checkpoint_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
kafka_topic = dbutils.widgets.get("kafka_topic")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

# COMMAND ----------

conn_str = dbutils.secrets.get("wizardbank", "eventhub-connection-string")
namespace = dbutils.secrets.get("wizardbank", "eventhub-namespace")
bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

target_table = f"`{catalog}`.`{schema}`.`{table}`"
checkpoint_location = f"{checkpoint_path.rstrip('/')}/{schema}/{table}/_checkpoint"

print(f"kafka_topic: {kafka_topic}")
print(f"target_table: {target_table}")
print(f"checkpoint_location: {checkpoint_location}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, from_json, struct, to_json

# Espejo del contrato de eventos de la Sesion 8 / Sesion 11, pero con todos los
# campos (incluidos los numericos del contrato original) como StringType.

esquema_sesion = StructType([
    StructField("id_sesion", StringType()),
    StructField("version_app", StringType()),
    StructField("sistema_operativo", StringType()),
    StructField("modelo_dispositivo", StringType()),
])

esquema_contexto = StructType([
    StructField("ubicacion_pantalla", StringType()),
    StructField("posicion", StringType()),
    StructField("tiempo_visible_seg", StringType()),
    StructField("monto_simulado", StringType()),   # solo en simulacion_realizada
    StructField("plazo_simulado", StringType()),   # solo en simulacion_realizada
])

esquema_evento = StructType([
    StructField("id_evento", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("timestamp_evento", StringType()),
    StructField("id_cliente", StringType()),
    StructField("id_oferta", StringType()),
    StructField("id_pais", StringType()),
    StructField("canal", StringType()),
    StructField("sesion", esquema_sesion),
    StructField("contexto", esquema_contexto),
])

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{conn_str}";'
    ),
}

raw = spark.readStream.format("kafka").options(**kafka_options).load()

# COMMAND ----------

parsed = raw.select(
    from_json(col("value").cast("string"), esquema_evento).alias("data"),
    col("key").cast("string").alias("_kafka_key"),
    col("topic").alias("_kafka_topic"),
    col("partition").alias("_kafka_partition"),
    col("offset").alias("_kafka_offset"),
    col("timestamp").alias("_kafka_timestamp"),
    col("timestampType").alias("_kafka_timestamp_type"),
)

bronze = parsed.select(
    col("data.id_evento").alias("id_evento"),
    col("data.tipo_evento").alias("tipo_evento"),
    col("data.timestamp_evento").alias("timestamp_evento"),
    col("data.id_cliente").alias("id_cliente"),
    col("data.id_oferta").alias("id_oferta"),
    col("data.id_pais").alias("id_pais"),
    col("data.canal").alias("canal"),
    col("data.sesion.id_sesion").alias("id_sesion"),
    col("data.sesion.version_app").alias("version_app"),
    col("data.sesion.sistema_operativo").alias("sistema_operativo"),
    col("data.sesion.modelo_dispositivo").alias("modelo_dispositivo"),
    col("data.contexto.ubicacion_pantalla").alias("ubicacion_pantalla"),
    col("data.contexto.posicion").alias("posicion"),
    col("data.contexto.tiempo_visible_seg").alias("tiempo_visible_seg"),
    col("data.contexto.monto_simulado").alias("monto_simulado"),
    col("data.contexto.plazo_simulado").alias("plazo_simulado"),
    to_json(struct(
        col("_kafka_key"),
        col("_kafka_topic"),
        col("_kafka_partition"),
        col("_kafka_offset"),
        col("_kafka_timestamp"),
        col("_kafka_timestamp_type"),
    )).alias("_metadata"),
    current_timestamp().alias("_ingested_at"),
)

# COMMAND ----------

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)

print(f"Estado del stream al iniciar: {query.status}")

query.awaitTermination()

print(f"Estado del stream al finalizar: {query.status}")
if query.lastProgress:
    print(
        f"Ultimo micro-batch: batchId={query.lastProgress.get('batchId')}, "
        f"filas procesadas={query.lastProgress.get('numInputRows')}"
    )
