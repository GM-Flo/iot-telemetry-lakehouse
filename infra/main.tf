resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  name_prefix  = "${var.project}-${var.environment}"
  storage_name = substr("${var.project}${var.environment}lake${random_string.suffix.result}", 0, 24)
  catalog_name = "${var.project}_${var.environment}"

  containers = ["bronze", "silver", "gold", "volumes"]

  tags = merge(var.tags, {
    environment = var.environment
  })
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name_prefix}-lakehouse"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2. Hierarchical namespace is what makes this a Data Lake rather than
# blob storage, and it is required for Unity Catalog external locations.
# ---------------------------------------------------------------------------

resource "azurerm_storage_account" "lake" {
  name                = local.storage_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  access_tier              = "Hot"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

resource "azurerm_storage_container" "layer" {
  for_each = toset(local.containers)

  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Event Hubs. Standard tier is mandatory: Basic exposes no Kafka endpoint and
# allows only the $Default consumer group, which would force the legacy JVM
# connector and rule out serverless compute.
# ---------------------------------------------------------------------------

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${local.name_prefix}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  sku                   = "Standard"
  capacity              = 1
  auto_inflate_enabled  = false
  minimum_tls_version   = "1.2"
  public_network_access_enabled = true

  tags = local.tags
}

resource "azurerm_eventhub" "telemetry" {
  name              = var.eventhub_name
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = var.eventhub_partitions
  message_retention = var.eventhub_retention_days
}

resource "azurerm_eventhub_consumer_group" "bronze" {
  name                = "bronze-ingest"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "producer-send"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name

  listen = false
  send   = true
  manage = false
}

resource "azurerm_eventhub_authorization_rule" "consumer" {
  name                = "databricks-listen"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name

  listen = true
  send   = false
  manage = false
}

# ---------------------------------------------------------------------------
# Databricks. Gated behind deploy_databricks so the rest of the stack still
# lands if the ARM deployment is refused on a student subscription.
# ---------------------------------------------------------------------------

resource "azurerm_databricks_workspace" "main" {
  count = var.deploy_databricks ? 1 : 0

  name                = "dbw-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku

  managed_resource_group_name = "rg-${local.name_prefix}-dbw-managed"

  tags = local.tags
}

resource "azurerm_databricks_access_connector" "uc" {
  count = var.deploy_databricks ? 1 : 0

  name                = "dbac-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_role_assignment" "uc_storage" {
  count = var.deploy_databricks ? 1 : 0

  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc[0].identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Budget. The single most important resource here on a 100 USD student credit.
# ---------------------------------------------------------------------------

resource "azurerm_consumption_budget_subscription" "main" {
  count = length(var.budget_alert_emails) > 0 ? 1 : 0

  name            = "budget-${local.name_prefix}-lakehouse"
  subscription_id = "/subscriptions/${var.subscription_id}"

  amount     = var.budget_amount
  time_grain = "Monthly"

  time_period {
    start_date = var.budget_start_date
  }

  dynamic "notification" {
    for_each = [25, 50, 75, 90]

    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"
      contact_emails = var.budget_alert_emails
    }
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Forecasted"
    contact_emails = var.budget_alert_emails
  }
}
