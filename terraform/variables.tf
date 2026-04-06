resource "random_id" "bucket_suffix" {
  byte_length = 4
}

variable "buckets" {
    description = "List of buckets"
    type        = list(string)
    default     = ["raw", "trusted"]
}

variable "glue_database_name" {
  description = "Nome do banco de dados Glue para o Data Catalog"
  type        = string
  default     = "lakehouse_db"
}

variable "iceberg_table_location_prefix" {
  description = "Prefixo S3 para armazenar tabelas Iceberg"
  type        = string
  default     = "iceberg/"
}
