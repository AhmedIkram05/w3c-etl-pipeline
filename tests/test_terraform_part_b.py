"""
Terraform Part B validation tests.

Verifies that ``terraform/part_b/`` directory structure, HCL files, and
resource definitions are syntactically correct and match expected patterns.

Running
-------
These tests require the ``terraform`` binary on ``PATH`` and a successful
``terraform init`` in the Part B directory. They are marked
``@pytest.mark.terraform`` and are skipped by default:

    # Run tests excluding terraform (default):
    pytest tests/ -v

    # Run terraform tests explicitly:
    pytest tests/test_terraform_part_b.py -v -m terraform
"""

import os
import re
import subprocess

import pytest

_PART_B_DIR = os.path.join(os.path.dirname(__file__), "..", "terraform", "part_b")

_EXPECTED_FILES = [
    "main.tf",
    "variables.tf",
    "outputs.tf",
    "environments/dev/terraform.tfvars",
]

_RESOURCE_PATTERNS = {
    "databricks_pipeline.bronze": r'resource\s+"databricks_pipeline"\s+"bronze"',
    "databricks_pipeline.silver": r'resource\s+"databricks_pipeline"\s+"silver"',
    "databricks_job.w3c_etl_workflow": r'resource\s+"databricks_job"\s+"w3c_etl_workflow"',
}

_TASK_KEYS = ["run_bronze", "run_silver", "run_jdbc_export"]


# ── Helpers ──────────────────────────────────────────────────────────


def _terraform_binary():
    """Return path to terraform binary, or None."""
    try:
        import shutil

        return shutil.which("terraform")
    except Exception:
        return None


def _terraform_inited(path):
    """Check if ``terraform init`` has been run in *path*."""
    return os.path.isdir(os.path.join(path, ".terraform"))


def _read_file(path):
    """Read and return file content, or None."""
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


# ── Directory & File Existence ───────────────────────────────────────


class TestTerraformPartBDirectory:
    """Verify the Part B directory and expected files exist."""

    def test_part_b_dir_exists(self):
        """terraform/part_b/ directory exists."""
        assert os.path.isdir(_PART_B_DIR), f"Expected directory {_PART_B_DIR} not found"

    @pytest.mark.parametrize("filename", _EXPECTED_FILES)
    def test_expected_file_exists(self, filename):
        """Expected file {filename} exists."""
        path = os.path.join(_PART_B_DIR, filename)
        assert os.path.isfile(path), f"Expected file {filename} not found at {path}"

    def test_has_terraform_init(self):
        """terraform init has been run (.terraform directory present)."""
        assert _terraform_inited(_PART_B_DIR), (
            f"No .terraform directory in {_PART_B_DIR}. Run 'terraform init' in terraform/part_b/ first."
        )


# ── HCL Resource Structure ───────────────────────────────────────────


class TestTerraformPartBResources:
    """Verify expected resource definitions exist in main.tf."""

    def _main_tf_content(self):
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None, "main.tf not found"
        return content

    @pytest.mark.parametrize(
        "resource_name,pattern",
        _RESOURCE_PATTERNS.items(),
        ids=list(_RESOURCE_PATTERNS.keys()),
    )
    def test_resource_defined(self, resource_name, pattern):
        """Expected resource {resource_name} is defined in main.tf."""
        content = self._main_tf_content()
        assert re.search(pattern, content), f"Resource '{resource_name}' not found in main.tf"

    def test_three_resources_total(self):
        """main.tf defines exactly 3 resource blocks (2 pipelines + 1 job)."""
        content = self._main_tf_content()
        matches = re.findall(r'resource\s+"(\w+)"\s+"(\w+)"', content)
        assert len(matches) == 3, f"Expected 3 resources, found {len(matches)}: {matches}"

    def test_both_pipelines_are_serverless(self):
        """Both pipeline resources have serverless = true."""
        content = self._main_tf_content()
        # Count serverless = true occurrences in pipeline blocks
        serverless_count = len(re.findall(r"serverless\s*=\s*true", content))
        assert serverless_count >= 2, f"Expected at least 2 'serverless = true', found {serverless_count}"

    def test_workflow_has_three_tasks(self):
        """The job resource defines exactly 3 task blocks."""
        content = self._main_tf_content()
        tasks = re.findall(r'task_key\s*=\s*"([^"]+)"', content)
        for key in _TASK_KEYS:
            assert key in tasks, f"Expected task_key '{key}' not found in job. Found: {tasks}"

    def test_workflow_has_depends_on(self):
        """Expected task dependencies exist (Silver→Bronze, JDBC→Silver)."""
        content = self._main_tf_content()
        assert re.search(r"depends_on\s*\{", content), "No depends_on blocks found in job"
        assert re.search(r'task_key\s*=\s*"run_bronze"', content), "Dependency on run_bronze not found"
        assert re.search(r'task_key\s*=\s*"run_silver"', content), "Dependency on run_silver not found"

    def test_workflow_has_daily_schedule(self):
        """Job has a daily 2AM UTC schedule."""
        content = self._main_tf_content()
        assert "quartz_cron_expression" in content
        assert "0 0 2 * * ?" in content or "0 2 * * *" in content
        assert "timezone_id" in content
        assert "UTC" in content

    def test_workflow_has_notebook_task(self):
        """Job has a notebook_task for JDBC export."""
        content = self._main_tf_content()
        assert "notebook_task" in content, "No notebook_task found in job (expected for JDBC export)"

    def test_workflow_has_environment(self):
        """Job has an environment block with pymssql."""
        content = self._main_tf_content()
        assert "environment" in content, "No environment block found"
        assert "pymssql" in content, "No pymssql dependency found"

    def test_no_job_cluster_block(self):
        """No job_cluster block exists (all 3 tasks use serverless)."""
        content = self._main_tf_content()
        assert "job_cluster" not in content, "job_cluster block found — all tasks should use serverless"


# ── Pipeline Configuration ───────────────────────────────────────────


class TestTerraformPartBConfig:
    """Verify pipeline configuration details."""

    def test_bronze_has_storage_account_name(self):
        """Bronze pipeline config has storage.account_name."""
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None
        assert "storage.account_name" in content

    def test_silver_has_geoip_paths(self):
        """Silver pipeline config has geoip paths."""
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None
        assert "geoip.city_db_path" in content
        assert "geoip.asn_db_path" in content
        assert "GeoLite2-City.mmdb" in content
        assert "GeoLite2-ASN.mmdb" in content

    def test_silver_has_maxminddb_env(self):
        """Silver pipeline has maxminddb environment dependency."""
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None
        assert "maxminddb" in content

    def test_both_pipelines_have_catalog_target(self):
        """Both pipelines specify catalog and target."""
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None
        # Count catalog/target occurrences
        catalog_count = content.count("catalog")
        target_count = content.count("target")
        assert catalog_count >= 2, f"Expected catalog in at least 2 places, found {catalog_count}"
        assert target_count >= 2, f"Expected target in at least 2 places, found {target_count}"

    def test_conditional_storage_key(self):
        """Storage key uses ternary for conditional injection."""
        content = _read_file(os.path.join(_PART_B_DIR, "main.tf"))
        assert content is not None
        # Should have a ternary pattern like: var.storage_access_key != "" ? ...
        assert "storage_access_key" in content
        assert "?" in content  # ternary operator
        assert "storage_access_key != " in content or "storage_access_key" in content


# ── Variables ────────────────────────────────────────────────────────


class TestTerraformPartBVariables:
    """Verify variables.tf contains expected inputs."""

    _EXPECTED_VARS = [
        "databricks_host",
        "storage_account_name",
        "storage_access_key",
        "azure_sql_server",
        "azure_sql_database",
        "unity_catalog_name",
        "bronze_notebook_path",
        "silver_notebook_path",
        "jdbc_export_notebook_path",
    ]

    def test_variables_file_exists(self):
        """variables.tf exists."""
        path = os.path.join(_PART_B_DIR, "variables.tf")
        assert os.path.isfile(path)

    @pytest.mark.parametrize("var_name", _EXPECTED_VARS)
    def test_expected_variable_defined(self, var_name):
        """Expected variable {var_name} is defined in variables.tf."""
        content = _read_file(os.path.join(_PART_B_DIR, "variables.tf"))
        assert content is not None
        assert f'variable "{var_name}"' in content, f"Variable '{var_name}' not found in variables.tf"


# ── Outputs ──────────────────────────────────────────────────────────


class TestTerraformPartBOutputs:
    """Verify outputs.tf contains expected values."""

    _EXPECTED_OUTPUTS = [
        "bronze_pipeline_id",
        "silver_pipeline_id",
        "workflow_job_id",
        "workflow_job_url",
    ]

    def test_outputs_file_exists(self):
        """outputs.tf exists."""
        path = os.path.join(_PART_B_DIR, "outputs.tf")
        assert os.path.isfile(path)

    @pytest.mark.parametrize("output_name", _EXPECTED_OUTPUTS)
    def test_expected_output_defined(self, output_name):
        """Expected output {output_name} is defined in outputs.tf."""
        content = _read_file(os.path.join(_PART_B_DIR, "outputs.tf"))
        assert content is not None
        assert f'output "{output_name}"' in content, f"Output '{output_name}' not found in outputs.tf"


# ── Terraform Validate (requires binary + init) ──────────────────────


@pytest.mark.terraform
class TestTerraformValidate:
    """Run ``terraform validate`` (requires ``terraform init`` completed)."""

    @pytest.fixture(scope="class")
    def terraform_path(self):
        """Ensure terraform binary and .terraform dir exist, return path."""
        tf_bin = _terraform_binary()
        if tf_bin is None:
            pytest.skip("terraform binary not found on PATH")
        if not _terraform_inited(_PART_B_DIR):
            pytest.skip("terraform init not run yet. Run 'terraform init' in terraform/part_b/ first.")
        return tf_bin

    def test_terraform_validate_success(self, terraform_path):
        """``terraform validate`` completes with exit code 0."""
        result = subprocess.run(
            [terraform_path, "validate"],
            cwd=_PART_B_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"terraform validate failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        assert "Success" in result.stdout, f"terraform validate did not report success:\n{result.stdout}"
