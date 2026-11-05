output "resource_group" {
  description = "Resource group holding the whole stack. Target for a fast teardown."
  value       = azurerm_resource_group.main.name
}

output "storage_account" {
  description = "ADLS Gen2 account name."
  value       = azurerm_storage_account.lake.name
}

output "storage_account_id" {
  description = "Full resource ID, needed when granting the access connector a role."
  value       = azurerm_storage_account.lake.id
}

output "abfss_paths" {
  description = "abfss URIs for each medallion layer. Use these in Unity Catalog external locations, never dbutils.fs.mount."
  value = {
    for c in local.containers :
    c => "abfss://${c}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  }
}

output "kafka_bootstrap_servers" {
  description = "Event Hubs Kafka endpoint for Structured Streaming."
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "eventhub_name" {
  description = "Event Hub name, used as the Kafka topic in subscribe."
  value       = azurerm_eventhub.telemetry.name
}

output "consumer_group" {
  description = "Dedicated consumer group for the bronze stream."
  value       = azurerm_eventhub_consumer_group.bronze.name
}

output "catalog_name" {
  description = "Unity Catalog name this environment expects."
  value       = local.catalog_name
}

output "producer_connection_string" {
  description = "Send-only connection string for the local producer. Export as EVENTHUB_CONN_STR."
  value       = azurerm_eventhub_authorization_rule.producer.primary_connection_string
  sensitive   = true
}

output "consumer_connection_string" {
  description = "Listen-only connection string for Databricks. Put this in a secret scope, never in a notebook."
  value       = azurerm_eventhub_authorization_rule.consumer.primary_connection_string
  sensitive   = true
}

output "databricks_workspace_url" {
  description = "Workspace URL, or null when deploy_databricks is false."
  value       = var.deploy_databricks ? "https://${azurerm_databricks_workspace.main[0].workspace_url}" : null
}

output "access_connector_id" {
  description = "Access connector resource ID. Paste this into the Unity Catalog storage credential."
  value       = var.deploy_databricks ? azurerm_databricks_access_connector.uc[0].id : null
}
