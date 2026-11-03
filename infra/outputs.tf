output "dashboard_url" {
  description = "Public URL of the dashboard"
  value       = "https://${azurerm_container_app.dashboard.ingress[0].fqdn}"
}

output "storage_account_name" {
  value = azurerm_storage_account.warehouse.name
}

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "job_name" {
  value = azurerm_container_app_job.pipeline.name
}
