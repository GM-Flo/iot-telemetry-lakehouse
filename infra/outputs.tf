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

output "eventhub_names" {
  description = "Event Hub (Kafka topic) per data environment."
  value       = { for env, hub in azurerm_eventhub.telemetry : env => hub.name }
}

output "catalog_name" {
  description = "Unity Catalog name for the platform's default environment."
  value       = local.catalog_name
}

output "producer_connection_strings" {
  description = "Send-only connection string per environment, for the producer. Read one with: terraform output -json producer_connection_strings"
  value       = { for env, rule in azurerm_eventhub_authorization_rule.producer : env => rule.primary_connection_string }
  sensitive   = true
}

output "consumer_connection_strings" {
  description = "Listen-only connection string per environment, for Databricks secrets. Never put these in a notebook."
  value       = { for env, rule in azurerm_eventhub_authorization_rule.consumer : env => rule.primary_connection_string }
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
