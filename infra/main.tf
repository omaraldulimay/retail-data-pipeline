locals {
  tags = {
    project    = "retail-data-pipeline"
    managed-by = "terraform"
  }

  # Same settings for the job and the dashboard.
  env = {
    AZURE_STORAGE_ACCOUNT_URL = azurerm_storage_account.warehouse.primary_blob_endpoint
    AZURE_CLIENT_ID           = azurerm_user_assigned_identity.app.client_id
    WAREHOUSE_CONTAINER       = azurerm_storage_container.warehouse.name
  }
}

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.name_prefix}"
  location = var.location
  tags     = local.tags
}

resource "azurerm_storage_account" "warehouse" {
  # Globally unique, lowercase, at most 24 characters.
  name                            = "${substr(var.name_prefix, 0, 14)}${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  tags                            = local.tags
}

resource "azurerm_storage_container" "warehouse" {
  name                  = "warehouse"
  storage_account_id    = azurerm_storage_account.warehouse.id
  container_access_type = "private"
}

resource "azurerm_user_assigned_identity" "app" {
  name                = "id-${var.name_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags
}

resource "azurerm_role_assignment" "blob_contributor" {
  scope                = azurerm_storage_account.warehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.app.principal_id
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${var.name_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

resource "azurerm_container_app_environment" "main" {
  name                       = "cae-${var.name_prefix}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  tags                       = local.tags
}

resource "azurerm_container_app_job" "pipeline" {
  name                         = "pipeline"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  container_app_environment_id = azurerm_container_app_environment.main.id
  replica_timeout_in_seconds   = 1800
  replica_retry_limit          = 0
  tags                         = local.tags

  # 05:00 UTC is 06:00 in Berlin in winter and 07:00 in summer (cron has no time zone here).
  schedule_trigger_config {
    cron_expression          = "0 5 * * *"
    parallelism              = 1
    replica_completion_count = 1
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.app.id]
  }

  template {
    container {
      name    = "pipeline"
      image   = var.image
      cpu     = 1
      memory  = "2Gi"
      command = ["python", "-m", "pipeline.cloud", "run"]

      dynamic "env" {
        for_each = local.env
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

  depends_on = [azurerm_role_assignment.blob_contributor]
}

resource "azurerm_container_app" "dashboard" {
  name                         = "dashboard"
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id
  revision_mode                = "Single"
  tags                         = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.app.id]
  }

  ingress {
    external_enabled = true
    target_port      = 8501

    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  template {
    min_replicas = 0
    max_replicas = 1

    container {
      name   = "dashboard"
      image  = var.image
      cpu    = 0.5
      memory = "1Gi"
      command = [
        "streamlit", "run", "dashboard/app.py",
        "--server.address", "0.0.0.0",
        "--server.port", "8501",
      ]

      dynamic "env" {
        for_each = local.env
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

  depends_on = [azurerm_role_assignment.blob_contributor]
}
