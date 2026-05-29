"""Run Great Expectations validation using GE v1.x EphemeralDataContext.

No config file needed - everything is set up in-memory.
Usage: python run_checkpoint.py
"""
import json
import os
import sys

# Set defaults matching docker-compose
os.environ.setdefault("W3C_DB_HOST", "postgres")
os.environ.setdefault("W3C_DB_PORT", "5432")
os.environ.setdefault("W3C_DB_NAME", "w3c_warehouse")
os.environ.setdefault("W3C_DB_USER", "airflow")
os.environ.setdefault("W3C_DB_PASS", "airflow")

from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.core import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

GE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── 1. Build connection string ─────────────────────────────────────────────
conn_str = (
    f"postgresql://{os.environ['W3C_DB_USER']}:{os.environ['W3C_DB_PASS']}"
    f"@{os.environ['W3C_DB_HOST']}:{os.environ['W3C_DB_PORT']}/{os.environ['W3C_DB_NAME']}"
)

# ── 2. Create ephemeral context (no config file needed) ────────────────────
project_config = DataContextConfig(
    config_version=4,
    expectations_store_name="expectations_store",
    validation_results_store_name="validation_results_store",
    checkpoint_store_name="checkpoint_store",
    stores={
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "validation_results_store": {
            "class_name": "ValidationResultsStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "validation_definition_store": {
            "class_name": "ValidationDefinitionStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
    },
)
context = EphemeralDataContext(project_config=project_config)

# Register context with project_manager (required for GE 1.x suite deserialization)
set_context(context)

# ── 3. Add PostgreSQL datasource & table asset ──────────────────────────────
datasource = context.data_sources.add_postgres(
    name="w3c_raw",
    connection_string=conn_str,
)
table_asset = datasource.add_table_asset(
    name="raw_logs",
    table_name="raw_logs",
    schema_name="public",
)
batch_definition = table_asset.add_batch_definition_whole_table(
    name="raw_logs_batch"
)

# ── 4. Load expectation suite from JSON ────────────────────────────────────
suite_path = os.path.join(GE_DIR, "expectations", "w3c_raw_logs_expectations.json")
with open(suite_path) as f:
    suite_dict = json.load(f)

suite = ExpectationSuite(name=suite_dict["expectation_suite_name"])
for exp in suite_dict["expectations"]:
    suite.add_expectation_configuration(
        ExpectationConfiguration(
            type=exp["expectation_type"],
            kwargs=exp["kwargs"],
            meta=exp.get("meta", {}),
        )
    )
context.suites.add(suite)

# ── 5. Create validation definition ───────────────────────────────────────
validation_definition = ValidationDefinition(
    name="raw_logs_validation",
    data=batch_definition,
    suite=suite,
)
context.validation_definitions.add(validation_definition)

# ── 6. Build and run checkpoint ────────────────────────────────────────────
checkpoint = Checkpoint(
    name="raw_logs_checkpoint",
    validation_definitions=[validation_definition],
)
context.checkpoints.add(checkpoint)

result = checkpoint.run()

# ── 7. Report results ─────────────────────────────────────────────────────
if result.success:
    print(f"✓ GE checkpoint 'raw_logs_checkpoint' PASSED")
    sys.exit(0)
else:
    print(f"✗ GE checkpoint 'raw_logs_checkpoint' FAILED")
    for run_result in result.run_results.values():
        for res in run_result.results:
            if not res.success:
                cfg = res.expectation_config
                print(f"  - {cfg['type']}: FAILED ({cfg['kwargs']})")
    sys.exit(1)
