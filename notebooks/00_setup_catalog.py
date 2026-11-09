# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup Unity Catalog
# MAGIC
# MAGIC One-time setup per environment: external locations, the project catalog, medallion
# MAGIC schemas, the checkpoint and landing volumes, and access for the deploying service
# MAGIC principal. Safe to re-run.
# MAGIC
# MAGIC **Prerequisite:** a storage credential named `iot_lake_cred` created in the UI
# MAGIC (Catalog → External data → Credentials) from the Terraform `access_connector_id` output.
# MAGIC Storage credentials for Azure managed identities cannot be created from SQL.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage account (blank: the one iot_bronze already uses)")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("credential", "iot_lake_cred", "Storage credential")
dbutils.widgets.text("deployer_principal", "", "Deploying service principal application ID (blank: skip)")
dbutils.widgets.text("secret_scope", "iot", "Secret scope")

storage_account = dbutils.widgets.get("storage_account").strip()
catalog = dbutils.widgets.get("catalog").strip()
credential = dbutils.widgets.get("credential").strip()
deployer = dbutils.widgets.get("deployer_principal").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()

if not catalog:
    raise ValueError("Fill in the catalog widget.")

# The second environment reuses the storage account of the first: read it from the
# external location the first setup created. Only the very first setup has to name it.
if not storage_account:
    existing = [r for r in spark.sql("SHOW EXTERNAL LOCATIONS").collect() if r["name"] == "iot_bronze"]
    if not existing:
        raise ValueError("No external location iot_bronze yet: fill in the storage_account widget.")
    storage_account = existing[0]["url"].split("@", 1)[1].split(".", 1)[0]
print(f"storage account: {storage_account}")


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

# The service principal that CI/CD deploys with owns and runs this environment's jobs
# and pipelines. It gets every privilege on this catalog only, and read access to the
# Event Hubs secrets; you stay the catalog's owner.
if deployer:
    if "`" in deployer:
        raise ValueError(f"not a valid principal: {deployer!r}")
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{deployer}`")

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import AclPermission

    WorkspaceClient().secrets.put_acl(scope=secret_scope, principal=deployer, permission=AclPermission.READ)
    print(f"{deployer}: ALL PRIVILEGES on {catalog}, READ on secret scope {secret_scope}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
if deployer:
    display(spark.sql(f"SHOW GRANTS `{deployer}` ON CATALOG {catalog}"))
