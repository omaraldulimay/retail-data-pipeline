variable "subscription_id" {
  description = "Azure subscription to deploy into"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "germanywestcentral"
}

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "retailpipe"
}

variable "image" {
  description = "Container image with the pipeline and the dashboard"
  type        = string
  default     = "ghcr.io/omaraldulimay/retail-data-pipeline:latest"
}
