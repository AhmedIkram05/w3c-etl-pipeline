databricks_host = "https://adb-7405616994554630.10.azuredatabricks.net"
# databricks_token: sourced from DATABRICKS_TOKEN env var
storage_account_name = "stw3cetlwestus3"
azure_sql_server     = "sql-w3c-etl.database.windows.net"
azure_sql_database   = "w3c-etl-db"
unity_catalog_name   = "w3c_etl_databricks"
access_connector_id  = "/subscriptions/2cfbc457-25bd-4007-8585-6bfa6765ec30/resourceGroups/rg-w3c-etl/providers/Microsoft.Databricks/accessConnectors/w3c-etl-databricks-access-connector"
azure_sql_user       = "sqladmin"
# storage_access_key: set via TF_VAR_storage_access_key env var (never commit plaintext)
