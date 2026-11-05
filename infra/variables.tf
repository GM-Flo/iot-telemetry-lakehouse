variable "subscription_id" {
  description = "Azure subscription ID. Required by azurerm 4.x."
  type        = string
}

variable "project" {
  description = "Short project slug used to name resources."
  type        = string
  default     = "iot"

  validation {
    condition     = can(regex("^[a-z0-9]{2,10}$", var.project))
    error_message = "project must be 2-10 lowercase alphanumeric characters."
  }
}

variable "environment" {
  description = "Deployment environment. Drives the Unity Catalog name (iot_dev / iot_prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "location" {
  description = <<-EOT
    Azure region. Azure for Students subscriptions carry an allowed-regions policy,
    so confirm this region is permitted before applying:
      az account list-locations --query "[].name" -o tsv
      az policy assignment list --query "[].displayName" -o tsv
  EOT
  type        = string
  default     = "eastus"
}

variable "deploy_databricks" {
  description = <<-EOT
    Whether to deploy the Azure Databricks workspace and its Unity Catalog access connector.
    Set false on the first apply so the storage and Event Hubs layer lands even if the
    Databricks ARM deployment is refused on an Azure for Students subscription.
  EOT
  type        = bool
  default     = false
}

variable "databricks_sku" {
  description = <<-EOT
    Databricks workspace tier. Must be premium for Unity Catalog, Databricks SQL and
    serverless. Never use standard: it has no Unity Catalog.
    Use trial for the 14-day free Premium DBU window.
  EOT
  type        = string
  default     = "trial"

  validation {
    condition     = contains(["trial", "premium"], var.databricks_sku)
    error_message = "databricks_sku must be trial or premium. standard has no Unity Catalog."
  }
}

variable "eventhub_name" {
  description = "Event Hub (Kafka topic) name."
  type        = string
  default     = "vehicle-telemetry"
}

variable "eventhub_partitions" {
  description = "Partition count. Cannot be changed after creation on Standard tier."
  type        = number
  default     = 4
}

variable "eventhub_retention_days" {
  description = "Message retention in days. Standard tier allows 1-7. Raise it to keep a replayable backlog across gaps in producer uptime."
  type        = number
  default     = 3

  validation {
    condition     = var.eventhub_retention_days >= 1 && var.eventhub_retention_days <= 7
    error_message = "Standard tier retention must be between 1 and 7 days."
  }
}

variable "budget_amount" {
  description = "Monthly budget ceiling in USD. Azure for Students grants 100."
  type        = number
  default     = 100
}

variable "budget_start_date" {
  description = "Budget period start. Must be the first day of a month, UTC."
  type        = string
  default     = "2026-11-01T00:00:00Z"
}

variable "budget_alert_emails" {
  description = "Addresses that receive budget threshold alerts. Leave empty to skip creating the budget."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project   = "iot-telemetry-lakehouse"
    managedby = "terraform"
  }
}
