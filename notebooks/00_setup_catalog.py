# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup Unity Catalog
# MAGIC
# MAGIC One-time setup: external locations, the project catalog, medallion schemas and the
# MAGIC checkpoint volume. Safe to re-run.
# MAGIC
# MAGIC **Prerequisite:** a storage credential named `iot_lake_cred` created in the UI
# MAGIC (Catalog → External data → Credentials) from the Terraform `access_connector_id` output.
# MAGIC Storage credentials for Azure managed identities cannot be created from SQL.

# COMMAND ----------

dbutils.widgets.text("storage_account", "iotdevlakexxxxxx", "Storage account")
dbutils.widgets.text("catalog", "iot_dev", "Catalog")
dbutils.widgets.text("credential", "iot_lake_cred", "Storage credential")

storage_account = dbutils.widgets.get("storage_account")
catalog = dbutils.widgets.get("catalog")
credential = dbutils.widgets.get("credential")


def abfss(container: str, path: str = "") -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"

# COMMAND ----------

# The storage account name has a random suffix and changes after a terraform
# destroy/apply, which is why it is a widget rather than hardcoded in SQL.
for container in ["bronze", "silver", "gold", "volumes"]:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS iot_{container}
        URL '{abfss(container)}'
        WITH (STORAGE CREDENTIAL {credential})
    """)

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalog}
    MANAGED LOCATION '{abfss("volumes", catalog)}'
""")

# Medallion layers each get their own container; reference and operations are
# small and share the volumes container under their own paths.
schemas = {
    "bronze": (abfss("bronze"), "Raw messages exactly as received from Event Hubs."),
    "silver": (abfss("silver"), "Validated, normalized readings, plus quarantine tables."),
    "gold": (abfss("gold"), "Business-ready facts, dimensions and aggregates."),
    "reference": (abfss("volumes", f"{catalog}/reference"), "Lookup and master data."),
    "operations": (abfss("volumes", f"{catalog}/operations"), "Pipeline run logs and audit tables."),
}

for schema, (location, comment) in schemas.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
        MANAGED LOCATION '{location}'
        COMMENT '{comment}'
    """)

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

# Landing zone for file sources. External, so upstream systems can drop files straight
# into ADLS. It sits outside the catalog's managed paths, which Unity Catalog requires.
environment = catalog.removeprefix("iot_")
spark.sql(f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.bronze.landing
    LOCATION '{abfss("volumes", f"landing/{environment}")}'
    COMMENT 'Landing zone for file sources, such as daily registry snapshots.'
""")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
