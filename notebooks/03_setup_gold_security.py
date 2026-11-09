# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Setup gold security
# MAGIC
# MAGIC Creates `gold.client_access` and the two functions the data products use: the row
# MAGIC filter (each client sees only its own rows) and the VIN mask. Run once per catalog,
# MAGIC **before** the first run of the `gold_build` pipeline. Safe to re-run.
# MAGIC
# MAGIC The admin principal gets `*` (every client). In prod, also add the principal the
# MAGIC pipeline runs as, so it can read everything it builds from.

# COMMAND ----------

import sys

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("admin_principal", "", "Admin principal (blank: you)")
dbutils.widgets.text("source_path", "", "Repository root in the workspace (blank: this notebook's parent)")

catalog = dbutils.widgets.get("catalog")
admin = dbutils.widgets.get("admin_principal") or spark.sql("SELECT current_user()").first()[0]
notebook_dir = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
root = dbutils.widgets.get("source_path") or "/Workspace" + notebook_dir.rsplit("/notebooks/", 1)[0]
if not catalog:
    raise ValueError("Set the catalog widget, e.g. iot_dev")
sys.path.append(root)

from src.iot.gold import security  # noqa: E402

# COMMAND ----------

for statement in security.setup_statements(catalog, admin):
    spark.sql(statement)

display(spark.sql(f"SELECT * FROM {security.qualified(catalog, security.ACCESS_TABLE)}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Grant a client user access** (example):
# MAGIC ```sql
# MAGIC INSERT INTO <catalog>.gold.client_access VALUES ('user@client.com', 'C001', current_timestamp(), current_user());
# MAGIC ```
# MAGIC They also need `SELECT` on the data products, but not on the dimensions and facts.