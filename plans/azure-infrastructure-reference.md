# Azure Infrastructure & IaC Reference

**Version:** v1.0 — Single source of truth for all Azure resources, Terraform modules, network topology, Unity Catalog schemas, cost controls, and OIDC federation.

---

## Section 1: Azure Infrastructure Overview

All Azure resources are deployed in **westus3** within a single resource group `rg-w3c-etl` (Azure subscription `2cfbc457-25bd-4007-8585-6bfa6765ec30`, tenant `b52c550c-05c2-4689-a595-c1e0e25d4a2e`). The IaC follows a **split structure**: Part A provisions core infrastructure (VNet, ADLS Gen2, Databricks workspace, Azure SQL), while Part B manages Databricks-level resources (DLT pipelines, Unity Catalog schemas, workflow job, secret scope, workspace files). Both parts use Azure Blob Storage as a remote state backend (`tfstatew3cetl/rg-tfstate`). Provider versions are pinned: `azurerm ~> 4.75.0`, `databricks ~> 1.115`. Budget controls enforce a $50 warning threshold and a $100 hard cap.

---

## Section 2: All Deployed Azure Resources

| Resource | Name/ID | Type | Config |
|---|---|---|---|
| Resource Group | rg-w3c-etl | `azurerm_resource_group` | westus3 |
| Virtual Network | vnet-w3c-etl (`/subscriptions/2cfbc457-.../virtualNetworks/vnet-w3c-etl`) | `azurerm_virtual_network` | 10.0.0.0/16 |
| Databricks Subnet | snet-databricks | `azurerm_subnet` | 10.0.1.0/24, delegated to `Microsoft.Databricks/workspaces`, Storage service endpoint |
| SQL Subnet | snet-sql | `azurerm_subnet` | 10.0.2.0/24, no service endpoints (private endpoints disabled) |
| NSG - Databricks | nsg-databricks | `azurerm_network_security_group` | 5 rules: SSH/HTTPS from AzureCloud.westus3, VNet-in, Azure out, deny Internet out |
| NSG - SQL | nsg-sql | `azurerm_network_security_group` | 4 rules: Databricks subnet TCP 1433, VNet-in, Azure out, deny Internet out |
| Storage Account | stw3cetlwestus3 | `azurerm_storage_account` | Standard LRS, StorageV2, hierarchical namespace, network rules Allow with bypass AzureServices/Logging/Metrics |
| ADLS Container 1 | raw-logs | `azurerm_storage_container` | Private access |
| ADLS Container 2 | bronze | `azurerm_storage_container` | Private access |
| ADLS Container 3 | silver | `azurerm_storage_container` | Private access |
| ADLS Container 4 | gold | `azurerm_storage_container` | Private access |
| Databricks Workspace | w3c-etl-databricks (`adb-7405616994554630.10.azuredatabricks.net`) | `azurerm_databricks_workspace` | Premium SKU, managed resource group rg-w3c-etl-managed, no public IP (disabled) |
| Databricks Access Connector | w3c-etl-databricks-access-connector (`/subscriptions/2cfbc457-.../accessConnectors/w3c-etl-databricks-access-connector`) | `azurerm_databricks_access_connector` | SystemAssigned managed identity |
| Managed Identity | dbmanagedidentity (principal ID: `0c3a72bc-5782-4879-8f21-b27dedde6906`) | `azurerm_user_assigned_identity` | Created by Databricks in managed resource group, Storage Blob Data Contributor |
| RBAC Assignment | databricks_access_connector → storage | `azurerm_role_assignment` | Storage Blob Data Contributor on stw3cetlwestus3 |
| RBAC Assignment | datalake → databricks managed identity | `azurerm_role_assignment` | Storage Blob Data Contributor on stw3cetlwestus3 |
| Azure SQL Server | sql-w3c-etl (`sql-w3c-etl.database.windows.net`) | `azurerm_mssql_server` | v12.0, SQL_Latin1_General_CP1_CI_AS |
| Azure SQL Database | w3c-etl-db | `azurerm_mssql_database` | GP_S_Gen5_1, auto-pause 60min, min capacity 1, prevent_destroy |
| Azure AD App (OIDC) | gha-w3c-etl-pipeline | `azuread_application` | Sign-in audience: AzureADMyOrg |
| Azure AD Service Principal | gha-w3c-etl-pipeline SP | `azuread_service_principal` | Linked to OIDC app |
| Federated Credential | gha-azure-dev | `azuread_application_federated_identity_credential` | Subject: `repo:AhmedIkram05/w3c-etl-pipeline:environment:azure-dev` |
| Role Assignment | GitHub Actions → RG | `azurerm_role_assignment` | Contributor on rg-w3c-etl |

---

## Section 3: Network Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                    westus3 — rg-w3c-etl                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              VNet: vnet-w3c-etl                          │   │
│  │                Address: 10.0.0.0/16                      │   │
│  │                                                          │   │
│  │  ┌─────────────────────────┐  ┌──────────────────────┐   │   │
│  │  │  snet-databricks        │  │  snet-sql            │   │   │
│  │  │  10.0.1.0/24            │  │  10.0.2.0/24         │   │   │
│  │  │                         │  │                       │   │   │
│  │  │  Delegation:            │  │  NSG: nsg-sql         │   │   │
│  │  │  Databricks/workspaces  │  │  ─────────────────    │   │   │
│  │  │                         │  │  Allow 10.0.1.0/24:   │   │   │
│  │  │  Service Endpoint:      │  │    1433 (Databricks)  │   │   │
│  │  │  Microsoft.Storage      │  │  Allow VNet Inbound   │   │   │
│  │  │                         │  │  Allow Azure Outbound │   │   │
│  │  │  NSG: nsg-databricks    │  │  Deny Internet Outbound│   │   │
│  │  │  ──────────────────     │  │                       │   │   │
│  │  │  Allow AzureCloud       │  │  ┌─────────────────┐  │   │   │
│  │  │    .westus3:22,443      │  │  │  Azure SQL       │  │   │   │
│  │  │  Allow VNet Inbound     │  │  │  sql-w3c-etl     │  │   │   │
│  │  │  Allow Azure Outbound   │  │  │  w3c-etl-db     │  │   │   │
│  │  │  Deny Internet Outbound │  │  │  GP_S_Gen5_1    │  │   │   │
│  │  │                         │  │  └─────────────────┘  │   │   │
│  │  │  ┌─────────────────┐    │  └──────────────────────┘   │   │
│  │  │  │ Databricks      │    │                              │   │
│  │  │  │ w3c-etl-        │    │                              │   │
│  │  │  │ databricks      │    │                              │   │
│  │  │  │ (Premium)       │    │                              │   │
│  │  │  └─────────────────┘    │                              │   │
│  │  └─────────────────────────┘                              │   │
│  │                                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Storage Account: stw3cetlwestus3                        │   │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐    │   │
│  │  │raw-logs │ │  bronze  │ │  silver  │ │   gold   │    │   │
│  │  └─────────┘ └──────────┘ └──────────┘ └──────────┘    │   │
│  │  Network rules: Allow (default), VNet subnets only     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Network rules:**
- VNet: `10.0.0.0/16` with 2 subnets
- Databricks subnet (`10.0.1.0/24`) is delegated to `Microsoft.Databricks/workspaces` and has a Storage service endpoint
- SQL subnet (`10.0.2.0/24`) has no service endpoints (`enable_private_endpoints = false`)
- Storage account network rules: default action `Allow`, bypass `AzureServices` + `Logging` + `Metrics`, restricted to Databricks subnet
- NSG rules enforce subnet isolation: both subnets deny outbound internet traffic, allow Azure Cloud outbound, and allow VNet-internal inbound. The SQL NSG additionally restricts TCP 1433 to only the Databricks subnet (`10.0.1.0/24`).
- Private endpoints are **disabled** (`var.enable_private_endpoints = false`)

---

## Section 4: Terraform Part A — Core Infrastructure

### Modules

**Module: networking** (`terraform/part_a/modules/networking/`)
- Creates the resource group, VNet (10.0.0.0/16), 2 subnets (Databricks-delegated + SQL), 2 NSGs with security rules, and NSG-subnet associations
- Variables: `resource_group_name`, `location`, `vnet_name`, `address_space`, `enable_private_endpoints`
- Outputs: `vnet_id`, `databricks_subnet_id`, `sql_subnet_id`, `resource_group_name`, `location`, `databricks_nsg_id`, `sql_nsg_id`

**Module: datalake** (`terraform/part_a/modules/datalake/`)
- Creates ADLS Gen2 storage account (Standard LRS, StorageV2, hierarchical namespace enabled, `prevent_destroy`), 4 containers (raw-logs, bronze, silver, gold), storage network rules, and 2 RBAC assignments (Databricks UiD + access connector → Storage Blob Data Contributor)
- Variables: `resource_group_name`, `location`, `storage_account_name`, `containers`, `databricks_managed_identity_id`, `databricks_subnet_id`

**Module: databricks** (`terraform/part_a/modules/databricks/`)
- Creates Premium Databricks workspace with managed resource group, access connector (SystemAssigned MI), and RBAC for access connector identity. Resolves the Databricks-created `dbmanagedidentity` user-assigned identity and applies `Storage Blob Data Contributor` role. Includes a 60-second `time_sleep` propagation wait before resolving access connector identity.
- Variables: `resource_group_name`, `location`, `workspace_name`, `sku`, `vnet_id`, `private_subnet_id`, `enable_private_endpoints`, `storage_account_id`
- Outputs: `workspace_url`, `workspace_id`, `managed_identity_id`, `managed_resource_group_name`, `access_connector_id`, `access_connector_identity_principal_id`

**Module: warehouse** (`terraform/part_a/modules/warehouse/`)
- Creates Azure SQL Server (v12.0, optional Azure AD admin), SQL Serverless database (GP_S_Gen5_1, auto-pause 60min, min capacity 1, `prevent_destroy`, collation `SQL_Latin1_General_CP1_CI_AS`), and optional firewall rules for allowed IPs.
- Variables: `resource_group_name`, `location`, `server_name`, `database_name`, `administrator_login`, `administrator_password`, `azuread_admin_object_id`, `vnet_id`, `sql_subnet_id`, `allowed_ips`
- Outputs: `server_fqdn`, `database_name`, `server_id`, `database_id`

### Remote State Backend

Both parts use Azure Blob Storage:
- Resource group: `rg-tfstate`
- Storage account: `tfstatew3cetl`
- Container: `tfstate`
- Part A key: `w3c-platform/part_a.tfstate`
- Part B key: `w3c-platform/part_b.tfstate`
- Auth: `use_azuread_auth = true`

### OIDC Federation

Managed in `github_oidc.tf` — see Section 8.

### Azure Monitor Alerts

Managed in `monitoring.tf` — see Section 9.

### All Part A Resources (Terraform Resource Types)

| Azure Resource | Terraform Resource Type | Name/ID |
|---|---|---|
| Resource Group | `azurerm_resource_group` | rg-w3c-etl |
| Virtual Network | `azurerm_virtual_network` | vnet-w3c-etl |
| Subnet (Databricks) | `azurerm_subnet` | snet-databricks |
| Subnet (SQL) | `azurerm_subnet` | snet-sql |
| NSG (Databricks) | `azurerm_network_security_group` | nsg-databricks |
| NSG (SQL) | `azurerm_network_security_group` | nsg-sql |
| NSG-Subnet Association | `azurerm_subnet_network_security_group_association` | databricks / sql |
| Storage Account | `azurerm_storage_account` | stw3cetlwestus3 |
| Storage Containers | `azurerm_storage_container` | raw-logs, bronze, silver, gold |
| RBAC (datalake MI) | `azurerm_role_assignment` | databricks_contributor |
| Databricks Workspace | `azurerm_databricks_workspace` | w3c-etl-databricks |
| Databricks Access Connector | `azurerm_databricks_access_connector` | w3c-etl-databricks-access-connector |
| RBAC (access connector) | `azurerm_role_assignment` | databricks_access_connector |
| Azure SQL Server | `azurerm_mssql_server` | sql-w3c-etl |
| Azure SQL Database | `azurerm_mssql_database` | w3c-etl-db |
| SQL Firewall Rules | `azurerm_mssql_firewall_rule` | allow-<ip> (per allowed IP) |
| Time Sleep | `time_sleep` | access_connector_identity_wait |
| Azure AD Application | `azuread_application` | gha-w3c-etl-pipeline |
| Azure AD Service Principal | `azuread_service_principal` | gha-w3c-etl-pipeline SP |
| Federated Credential | `azuread_application_federated_identity_credential` | gha-azure-dev |
| Role Assignment (OIDC) | `azurerm_role_assignment` | github_actions |
| Action Group (P1) | `azurerm_monitor_action_group` | ag-w3c-critical |
| Action Group (P2) | `azurerm_monitor_action_group` | ag-w3c-warning |
| Action Group (P3) | `azurerm_monitor_action_group` | ag-w3c-info |
| Metric Alert (SQL auto-pause) | `azurerm_monitor_metric_alert` | ma-w3c-azure-sql-auto-pause |
| Log Alert (Databricks retry) | `azurerm_monitor_scheduled_query_rules_alert_v2` | sqr-w3c-databricks-job-retry (conditional on Log Analytics) |

### Part A Variables (15 total)

| Variable | Type | Default | Description |
|---|---|---|---|
| subscription_id | string (sensitive) | — | Azure subscription ID |
| tenant_id | string (sensitive) | — | Azure tenant ID |
| client_id | string (sensitive) | — | Service principal client ID |
| client_secret | string (sensitive) | `""` | Service principal secret (env var) |
| resource_group_name | string | `"rg-w3c-etl-dev"` | Resource group name |
| location | string | `"eastus"` | Azure region |
| vnet_name | string | `"vnet-w3c-etl"` | VNet name |
| address_space | string | `"10.0.0.0/16"` | VNet address space |
| storage_account_name | string | — | Storage account name |
| containers | list(string) | `["raw-logs","bronze","silver","gold"]` | ADLS containers |
| databricks_workspace_name | string | `"w3c-etl-databricks-dev"` | Databricks workspace name |
| databricks_sku | string | `"premium"` | Databricks SKU |
| sql_server_name | string | — | SQL server name |
| sql_database_name | string | `"w3c-etl-db"` | SQL database name |
| sql_administrator_login | string (sensitive) | — | SQL admin login |
| sql_administrator_password | string (sensitive) | `""` | SQL admin password (env var) |
| enable_private_endpoints | bool | `false` | Enable private endpoints |
| azuread_admin_object_id | string | `""` | Azure AD admin object ID |
| sql_allowed_ips | list(string) | `[]` | Allowed IPs for SQL firewall |
| github_oidc_enabled | bool | `true` | Enable OIDC federation |
| github_organization | string | `"AhmedIkram05"` | GitHub org |
| github_repository | string | `"w3c-etl-pipeline"` | GitHub repo |
| github_environments | list(string) | `["azure-dev"]` | GitHub environments for OIDC |
| alert_email_critical | string (sensitive) | — | P1 alert email |
| alert_email_warning | string (sensitive) | — | P2 alert email |
| alert_email_info | string (sensitive) | `""` | P3 alert email |
| enable_log_analytics | bool | `false` | Enable Log Analytics |

### Part A Outputs (11 total)

| Output | Source | Description |
|---|---|---|
| storage_account_name | `module.datalake` | Storage account name |
| databricks_workspace_url | `module.databricks` | Databricks workspace URL |
| databricks_workspace_id | `module.databricks` | Workspace resource ID |
| managed_identity_id | `module.databricks` | Databricks managed identity principal ID |
| server_fqdn | `module.warehouse` | SQL server FQDN |
| database_name | `module.warehouse` | Database name |
| resource_group_name | `var.resource_group_name` | Resource group name |
| location | `var.location` | Azure region |
| vnet_id | `module.networking` | VNet ID |
| databricks_subnet_id | `module.networking` | Databricks subnet ID |
| sql_subnet_id | `module.networking` | SQL subnet ID |
| github_actions_application_id | `azuread_application.github_actions` | OIDC app client ID |
| github_actions_principal_id | `azuread_service_principal.github_actions` | OIDC SP object ID |
| access_connector_identity_principal_id | `module.databricks` | Access connector principal ID |

---

## Section 5: Terraform Part B — Databricks Resources

### Managed Resources

| Resource | Terraform Resource | Config |
|---|---|---|
| Secret Scope | `databricks_secret_scope` | `w3c-etl-pipeline`, 8 secrets managed within |
| Secret: SQL Server | `databricks_secret` | `azure.sql.server` = `sql-w3c-etl.database.windows.net` |
| Secret: SQL Database | `databricks_secret` | `azure.sql.database` = `w3c-etl-db` |
| Secret: SQL Username | `databricks_secret` | `azure.sql.username` = `sqladmin` |
| Secret: SQL Password | `databricks_secret` | `azure.sql.password` (from `TF_VAR_azure_sql_password`) |
| Secret: Storage Key | `databricks_secret` | `storage-access-key` (from `TF_VAR_storage_access_key`) |
| Bronze DLT Pipeline | `databricks_pipeline` | Name: `w3c-bronze-pipeline`, ID: `a6ea62d3-5f3a-4f53-ae8b-4bfb156703ad`, serverless, PREVIEW channel, catalog: `w3c_etl_databricks`, target: `bronze` |
| Silver DLT Pipeline | `databricks_pipeline` | Name: `w3c-silver-pipeline`, ID: `98c7675f-5425-4a14-95b6-247af6da9626`, serverless, PREVIEW channel, environment deps: `maxminddb==2.8.*` |
| Workflow Job | `databricks_job` | Name: `w3c-etl-workflow`, ID: `847995192336508`, 3 tasks, serverless, schedule `0 0 2 * * ?` UTC, `lifecycle { ignore_changes = [task] }` |
| UC Schema: bronze | `databricks_schema` | `w3c_etl_databricks.bronze` — "Bronze schema for raw W3C log data" |
| UC Schema: silver | `databricks_schema` | `w3c_etl_databricks.silver` — "Silver schema for enriched W3C log data" |
| UC Schema: gold | `databricks_schema` | `w3c_etl_databricks.gold` — "Gold schema for aggregated/export-ready datasets" |
| UC Storage Credential | `databricks_storage_credential` | `w3c_raw_logs_credential` — Azure managed identity via access connector |
| UC External Location | `databricks_external_location` | `w3c_raw_logs` — `abfss://raw-logs@stw3cetlwestus3.dfs.core.windows.net/` |
| UC Grant (bronze → raw_logs) | `databricks_grants` | `READ_FILES` on external location to `account users` |
| Notebook: DLT Bronze | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_bronze.py` |
| Notebook: DLT Silver | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_silver.py` |
| Notebook: JDBC Export | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/jdbc_export_azure.py` |
| Notebook: dbt Run | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_run.py` |
| Notebook: dbt Test | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_test.py` |
| Notebook: dbt Docs | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_docs.py` |
| Notebook: dbt Freshness | `databricks_notebook` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_freshness.py` |
| File: dbt Common | `databricks_workspace_file` | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_common.py` |
| File: dbt Project ZIP | `databricks_workspace_file` | `/dbt_project/w3c` (ZIP of dbt project) |

### Workflow Task Definitions

The `w3c-etl-workflow` job (ID `847995192336508`) runs 3 sequential serverless tasks:

1. **run_bronze** — `pipeline_task` triggering the Bronze DLT pipeline. Unlimited timeout.
2. **run_silver** — `pipeline_task` triggering the Silver DLT pipeline. Depends on `run_bronze`. Unlimited timeout.
3. **run_jdbc_export** — `notebook_task` running `jdbc_export_azure.py` with pymssql environment (`pymssql>=2.2.11`). Depends on `run_silver`. Unlimited timeout.

The `lifecycle { ignore_changes = [task] }` block suppresses noisy plan diffs from the provider's unordered TypeSet serialization of task blocks.

### Pipeline Configurations

Both DLT pipelines are **serverless** (no `cluster {}` block), use `channel = "PREVIEW"`, `continuous = false`, and target `w3c_etl_databricks.bronze` / `w3c_etl_databricks.silver`. The Bronze pipeline configuration includes `storage.account_name = stw3cetlwestus3` for Auto Loader ABFSS auth. The Silver pipeline additionally includes GeoIP database paths (`geoip.city_db_path`, `geoip.asn_db_path`) under `/Volumes/w3c_etl_databricks/bronze/w3c_data/`.

### Notebooks (9 Workspace Files)

All deployed to `/Repos/w3c-etl-pipeline/airflow/spark/databricks/`:
- `dlt_bronze.py` — Bronze DLT pipeline with W3C file parser
- `dlt_silver.py` — Silver DLT pipeline with MaxMind GeoIP enrichment
- `jdbc_export_azure.py` — Silver → Azure SQL JDBC export via pymssql
- `dbt_run.py` — Runs `dbt run --profile w3c_azure` on serverless
- `dbt_test.py` — Runs `dbt test --profile w3c_azure`
- `dbt_docs.py` — Generates `dbt docs generate` on Azure SQL
- `dbt_freshness.py` — Runs `dbt source freshness` on Azure SQL
- `dbt_common.py` — Shared bootstrap module (pip install, ODBC, ZIP extraction, credential loading)
- `/dbt_project/w3c` — dbt project ZIP at workspace root

### Part B Variables (19 total)

| Variable | Type | Default | Source |
|---|---|---|---|
| databricks_host | string | — | Workspace URL |
| storage_account_name | string | — | From Part A (`stw3cetlwestus3`) |
| storage_access_key | string (sensitive) | — | `TF_VAR_` env var |
| azure_sql_server | string | — | From Part A (`sql-w3c-etl.database.windows.net`) |
| azure_sql_database | string | `"w3c-etl-db"` | From Part A |
| unity_catalog_name | string | `"w3c_etl_databricks"` | Unity Catalog name |
| bronze_notebook_path | string | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_bronze.py` | Workspace path |
| silver_notebook_path | string | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_silver.py` | Workspace path |
| jdbc_export_notebook_path | string | `/Repos/w3c-etl-pipeline/airflow/spark/databricks/jdbc_export_azure.py` | Workspace path |
| access_connector_id | string | — | From Part A |
| azure_sql_user | string (sensitive) | — | SQL admin username (`sqladmin`) |
| azure_sql_password | string (sensitive) | `""` | `TF_VAR_` env var |

### Part B Outputs (5 total)

| Output | Description |
|---|---|
| bronze_pipeline_id | Bronze DLT pipeline ID |
| silver_pipeline_id | Silver DLT pipeline ID |
| workflow_job_id | Databricks Workflow job ID (`847995192336508`) |
| workflow_job_url | Direct URL to the workflow in Databricks |
| (secret_scope) | Scope managed but not exposed as output |

---

## Section 6: Unity Catalog Schemas

| Schema | Managed By | Catalog | Tables / Purpose |
|---|---|---|---|
| w3c_etl_databricks.bronze | Terraform Part B (`databricks_schema`) | w3c_etl_databricks (managed) | `bronze_raw_logs` — DLT Managed, 18 data columns + source_file + _rescued_data + partition cols, 153,380 rows |
| w3c_etl_databricks.silver | Terraform Part B (`databricks_schema`) | w3c_etl_databricks (managed) | `silver_enriched_logs` — DLT Managed, 31 columns (25 core + 6 geo), 153,377 rows |
| w3c_etl_databricks.gold | Terraform Part B (`databricks_schema`) | w3c_etl_databricks (managed) | Reserved for future use (schema created, no tables) |

The Unity Catalog is a Databricks-managed catalog (`w3c_etl_databricks`) — not created by Terraform but referenced via `data.databricks_catalog.w3c`. All 3 schemas are managed as code via `databricks_schema` resources. A storage credential (`w3c_raw_logs_credential`) authenticates to ADLS Gen2 using the Databricks access connector's Azure managed identity, and an external location (`w3c_raw_logs`) maps `abfss://raw-logs@stw3cetlwestus3.dfs.core.windows.net/` into Unity Catalog with `READ_FILES` granted to `account users`.

---

## Section 7: Cost Management

| Control | Threshold | Action | Managed By |
|---|---|---|---|
| Budget alert 1 | $50 | Warning email to `alert_email_critical` | `azurerm_consumption_budget_resource_group` |
| Budget alert 2 | $100 | Hard cap notification | `azurerm_consumption_budget_resource_group` |
| Azure SQL auto-pause | 60 min idle | Auto-pauses serverless DB (GP_S_Gen5_1, min 1 vCore) | `azurerm_mssql_database` |
| Databricks serverless | Auto-scales to zero | No idle VM costs (serverless DLT + serverless SQL warehouse) | Databricks platform |
| Storage tier | Standard LRS | Hot tier for all containers | `azurerm_storage_account` |

---

## Section 8: OIDC Federation

The OIDC federation is fully managed by `github_oidc.tf` — no manual Azure AD CLI commands are needed.

**Architecture:**
1. An Azure AD application (`gha-w3c-etl-pipeline`) is created with sign-in audience `AzureADMyOrg`
2. A service principal is provisioned from the application
3. A federated identity credential is created for each GitHub environment (default: `azure-dev`) with subject format `repo:AhmedIkram05/w3c-etl-pipeline:environment:<environment-name>`, issuer `https://token.actions.githubusercontent.com`, and audience `api://AzureADTokenExchange`
4. A Contributor role assignment is applied on the resource group scope (`/subscriptions/<id>/resourceGroups/rg-w3c-etl`)
5. The `skip_service_principal_aad_check = true` flag avoids timing issues during SP propagation

**GitHub Environment variables (no static secrets):**
- `AZURE_CLIENT_ID` — from output `github_actions_application_id`
- `AZURE_TENANT_ID` — `b52c550c-05c2-4689-a595-c1e0e25d4a2e`
- `AZURE_SUBSCRIPTION_ID` — `2cfbc457-25bd-4007-8585-6bfa6765ec30`

The CD pipeline authenticates via `azure/login@v2` with `client-id`, `tenant-id`, `subscription-id` and `enable-oidc: true` — no client secrets are stored or transmitted.

---

## Section 9: Monitoring & Alerts

### Action Groups

| Group | Name | Short Name | Severity | Receiver |
|---|---|---|---|---|
| P1 — Critical | `ag-w3c-critical` | w3c-p1 | 0 (Critical) | `alert_email_critical` |
| P2 — Warning | `ag-w3c-warning` | w3c-p2 | 1 (Warning) | `alert_email_warning` |
| P3 — Informational | `ag-w3c-info` | w3c-p3 | 2 (Info) | `alert_email_info` |

### Metric Alerts

| Alert | Resource | Severity | Condition | Window | Action Group |
|---|---|---|---|---|---|
| Azure SQL auto-pause | `azurerm_mssql_database.w3c-etl-db` | P1 (sev 0) | `cpu_percent` Average < 0.1 | 1 hour | ag-w3c-critical |
| Databricks job retry | Databricks workspace | P2 (sev 1) | Log query: failed jobs with Attempt > 1, count > 1 | 30 min | ag-w3c-warning (conditional on Log Analytics) |

### Budget Alerts

| Threshold | Scope | Notification |
|---|---|---|
| $50 | Resource group rg-w3c-etl | Email to `alert_email_critical` |
| $100 | Resource group rg-w3c-etl | Email to `alert_email_critical` |

---

## Section 10: Terraform Validation & Testing

| Check | Tool | Part A | Part B | Runs in CI |
|---|---|---|---|---|
| HCL formatting | `terraform fmt --check -recursive` | ✅ | ✅ | ✅ (Tier 1) |
| Module resolution | `terraform init -backend=false` | ✅ | ✅ | ✅ (Tier 1) |
| Config validation | `terraform validate` | ✅ | ✅ | ✅ (Tier 1) |
| HCL test assertions | `terraform test` | 3 assertions | 6 assertions | ✅ (Tier 1) |
| Python unit tests | `pytest -m terraform` | 29 tests | 39 tests | ✅ (Tier 1) |

### Terraform Test Assertions

**Part A** (`terraform/part_a/tests/default.tftest.hcl`) — 3 assertions with mock providers:
1. Azure credentials provided: `subscription_id`, `tenant_id`, `client_id` are non-empty
2. Resource names provided: `storage_account_name` and `sql_server_name` are non-empty
3. Alert emails configured: `alert_email_critical` and `alert_email_warning` are non-empty

**Part B** (`terraform/part_b/tests/default.tftest.hcl`) — 6 assertions with mock providers:
1. `databricks_pipeline.bronze` resource exists
2. `databricks_pipeline.silver` resource exists
3. `databricks_job.w3c_etl_workflow` resource exists
4. All 3 Unity Catalog schemas exist (`bronze`, `silver`, `gold`)
5. `databricks_secret_scope.w3c_etl` exists
6. Outputs defined: `bronze_pipeline_id`, `silver_pipeline_id`, `workflow_job_id`, `workflow_job_url`

---

## Section 11: Key Terraform Configuration Details

| Setting | Part A | Part B |
|---|---|---|
| Terraform version | `>= 1.10.5, < 2.0` | `>= 1.10.5, < 2.0` |
| AzureRM provider | `hashicorp/azurerm ~> 4.75.0` | `hashicorp/azurerm ~> 4.75.0` |
| Databricks provider | `databricks/databricks ~> 1.115` | `databricks/databricks ~> 1.115` |
| AzureAD provider | `hashicorp/azuread ~> 3.1` | — |
| Time provider | `hashicorp/time ~> 0.12` | — |
| Backend | Azure Blob Storage (`w3c-platform/part_a.tfstate`) | Azure Blob Storage (`w3c-platform/part_b.tfstate`) |
| Backend storage | `tfstatew3cetl`, container `tfstate`, RG `rg-tfstate` | Same |
| Backend auth | `use_azuread_auth = true` | `use_azuread_auth = true` |
| Environment | dev | dev |
| Variables | 27 total (15 core + 5 OIDC + 5 monitoring + 2 misc) | 12 |
| Outputs | 13 | 5 |
| Modules | 4 (networking, datalake, databricks, warehouse) | 0 (standalone resources) |
| TFVars file | `terraform/part_a/environments/dev/terraform.tfvars` | `terraform/part_b/environments/dev/terraform.tfvars` |

### Dev Environment Overrides

Dev tfvars override the Part A defaults to use `westus3` and production-level names:
- `resource_group_name = "rg-w3c-etl"` (overrides default `rg-w3c-etl-dev`)
- `location = "westus3"` (overrides default `eastus`)
- `databricks_workspace_name = "w3c-etl-databricks"` (overrides default `w3c-etl-databricks-dev`)
- `enable_private_endpoints = false`
- `azuread_admin_object_id = ""`
