# Azure deployment (Terraform)

Runs the pipeline as a scheduled Container App Job and the dashboard as a Container App. The DuckDB
file lives in a private blob container; the job downloads it, runs the daily pipeline and uploads it
again. The dashboard downloads it at most every 15 minutes.

## What gets created

- Resource group, storage account (LRS, TLS 1.2, no public access, shared keys disabled) and a private
  container `warehouse`
- User-assigned managed identity with *Storage Blob Data Contributor* on the storage account
- Log Analytics workspace (30 days) and a Container Apps environment (consumption plan)
- Container App Job `pipeline`, daily at 05:00 UTC (06:00 Berlin in winter, 07:00 in summer)
- Container App `dashboard` with public HTTPS, scales to zero

## Prerequisites

- [Azure CLI](https://learn.microsoft.com/cli/azure/) and [Terraform](https://developer.hashicorp.com/terraform) >= 1.6
- The image `ghcr.io/omaraldulimay/retail-data-pipeline` must exist and be public. CI publishes it on
  every push to `main`.

## Deploy

```bash
az login
cd infra
terraform init
terraform apply -var subscription_id=<your-subscription-id>
```

Or copy `terraform.tfvars.example` to `terraform.tfvars` and fill it in. The dashboard shows
"Noch keine Daten" until the job has run once. Start it manually instead of waiting for 05:00 UTC:

```bash
az containerapp job start --name pipeline --resource-group rg-retailpipe
```

Terraform prints the dashboard URL as `dashboard_url`.

## Cost

Everything is consumption-based. The job runs about a minute a day and the dashboard scales to zero
when nobody uses it, so compute cost is close to nothing. Storage holds a few MB. Log Analytics charges
a small amount per GB ingested; at this volume that is cents per month.

## Tear down

```bash
terraform destroy -var subscription_id=<your-subscription-id>
```

This deletes the warehouse file as well.
