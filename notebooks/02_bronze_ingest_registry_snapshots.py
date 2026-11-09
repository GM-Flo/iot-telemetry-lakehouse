# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingest registry snapshots
# MAGIC
# MAGIC Auto Loader watches the registry landing folder and appends each new file, as one raw
# MAGIC row, to `<catalog>.bronze.raw_registry_snapshots`. Files already loaded are skipped.
# MAGIC
# MAGIC A file whose name carries no snapshot date fails the run instead of loading: CDC
# MAGIC orders snapshots by that date, so an undated snapshot cannot be placed in history.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(".."))

from src.iot.bronze import to_registry_bronze  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_dev", "Catalog")
catalog = dbutils.widgets.get("catalog")

landing_path = f"/Volumes/{catalog}/bronze/landing/registry"
table = f"{catalog}.bronze.raw_registry_snapshots"
checkpoint = f"/Volumes/{catalog}/bronze/checkpoints/raw_registry_snapshots"
application_id = f"{catalog}.bronze_ingest_registry_snapshots"

print(f"landing    : {landing_path}")
print(f"table      : {table}")
print(f"checkpoint : {checkpoint}")

# COMMAND ----------


def write_batch(batch_df, batch_id: int) -> None:
    """Validate then append one micro-batch.

    txnAppId and txnVersion make the append idempotent: if a batch is retried
    after a failure, Delta recognises the batch id and does not write it twice.
    """
    undated = [r.source_file_path for r in batch_df.filter("snapshot_date IS NULL").collect()]
    if undated:
        raise ValueError(
            "Registry files must be named fleet_registry_YYYY-MM-DD.json. "
            f"Rejected: {undated}"
        )

    (
        batch_df.write.format("delta")
        .mode("append")
        .option("txnAppId", application_id)
        .option("txnVersion", batch_id)
        .saveAsTable(table)
    )


rows_before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("wholetext", "true")
    .load(landing_path)
)

query = (
    to_registry_bronze(raw)
    .writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .queryName("bronze_ingest_registry_snapshots")
    .start()
)
query.awaitTermination()

# COMMAND ----------

rows_after = spark.table(table).count()
summary = {
    "table": table,
    "files_before": rows_before,
    "files_ingested": rows_after - rows_before,
    "files_total": rows_after,
}
print(json.dumps(summary, indent=2))

display(
    spark.table(table)
    .select("snapshot_date", "source_file_path", "file_modified_at", "ingested_at")
    .orderBy("snapshot_date")
)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary))
