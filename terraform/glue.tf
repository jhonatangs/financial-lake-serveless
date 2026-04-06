# Banco de dados Glue para o Data Catalog
resource "aws_glue_catalog_database" "lakehouse_db" {
  name        = "lakehouse_db"
  description = "Database for Lakehouse trusted layer (Iceberg tables)"
}

# Permissões para a role da Lambda acessar o banco de dados
# (As tabelas Iceberg serão criadas dinamicamente pela Lambda via awswrangler)
