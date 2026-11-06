# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest
# MAGIC
# MAGIC Reads the Event Hubs Kafka endpoint and appends every message, unparsed, to
# MAGIC `<catalog>.bronze.telemetry_raw`.
# MAGIC
# MAGIC Runs with `availableNow`: it processes everything waiting since the last run, then
# MAGIC stops. The checkpoint records which offsets were committed, so each run picks up
# MAGIC exactly where the previous one ended.

# COMMAND ----------

import json
import os
import sys

# Bundle deploys the repo as files; notebooks run from <root>/notebooks.
sys.path.append(os.path.abspath(".."))

from src.iot.config import bronze_paths, kafka_source_options  # noqa: E402
from src.iot.transforms import to_bronze  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_dev", "Catalog")
dbutils.widgets.text("secret_scope", "iot", "Secret scope")
dbutils.widgets.text("secret_key", "eventhub-listen-conn", "Secret key")
dbutils.widgets.text("max_offsets_per_trigger", "10000", "Max offsets per batch")

catalog = dbutils.widgets.get("catalog")
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
max_offsets = int(dbutils.widgets.get("max_offsets_per_trigger"))

paths = bronze_paths(catalog)
conn = dbutils.secrets.get(secret_scope, secret_key)
options = kafka_source_options(conn, max_offsets_per_trigger=max_offsets)

print(f"target table : {paths['table']}")
print(f"checkpoint   : {paths['checkpoint']}")
print(f"topic        : {options['subscribe']}")

# COMMAND ----------

rows_before = spark.table(paths["table"]).count() if spark.catalog.tableExists(paths["table"]) else 0

raw = spark.readStream.format("kafka").options(**options).load()

query = (
    to_bronze(raw)
    .writeStream.format("delta")
    .option("checkpointLocation", paths["checkpoint"])
    .trigger(availableNow=True)
    .queryName("bronze_ingest")
    .toTable(paths["table"])
)
query.awaitTermination()

# COMMAND ----------

rows_after = spark.table(paths["table"]).count()
batches = [p for p in query.recentProgress if p.get("numInputRows", 0) > 0]

summary = {
    "table": paths["table"],
    "rows_before": rows_before,
    "rows_ingested": rows_after - rows_before,
    "rows_total": rows_after,
    "micro_batches": len(batches),
}
print(json.dumps(summary, indent=2))

# COMMAND ----------

display(
    spark.table(paths["table"])
    .groupBy("kafka_partition")
    .agg({"kafka_offset": "max", "*": "count"})
    .orderBy("kafka_partition")
)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary))