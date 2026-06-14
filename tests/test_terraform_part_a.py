"""
Terraform Part A validation tests.

Verifies that ``terraform/part_a/`` directory structure, HCL files, and
module definitions are syntactically correct and match expected patterns.

Running
-------
These tests require the ``terraform`` binary on ``PATH`` and a successful
``terraform init`` in the Part A directory. They are marked
``@pytest.mark.terraform`` and are skipped by default:

    # Run tests excluding terraform (default):
    pytest tests/ -v

    # Run terraform tests explicitly:
    pytest tests/test_terraform_part_a.py -v -m terraform
"""

import os
import re
import subprocess

import pytest

_PART_A_DIR = os.path.join(os.path.dirname(__file__), "..", "terraform", "part_a")

_EXPECTED_FILES = [
    "main.tf",
    "variables.tf",
    "outputs.tf",
    "backend.tf",
    "environments/dev/terraform.tfvars",
]

_EXPECTED_MODULES = ["networking", "datalake", "databricks", "warehouse"]

_RESOURCE_PATTERNS = {
    "module.networking": r'module\s+"networking"',
    "module.datalake": r'module\s+"datalake"',
    "module.databricks": r'module\s+"databricks"',
    "module.warehouse": r'module\s+"warehouse"',
}


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


@pytest.mark.terraform
class TestTerraformPartADirectory:
    """Verify the Part A directory and expected files exist."""

    def test_part_a_dir_exists(self):
        """terraform/part_a/ directory exists."""
        assert os.path.isdir(_PART_A_DIR), f"Expected directory {_PART_A_DIR} not found"

    @pytest.mark.parametrize("filename", _EXPECTED_FILES)
    def test_expected_file_exists(self, filename):
        """Expected file {filename} exists."""
        path = os.path.join(_PART_A_DIR, filename)
        assert os.path.isfile(path), f"Expected file {filename} not found at {path}"

    def test_has_terraform_init(self):
        """terraform init has been run (.terraform directory present)."""
        assert _terraform_inited(_PART_A_DIR), (
            f"No .terraform directory in {_PART_A_DIR}. Run 'terraform init' in terraform/part_a/ first."
        )


# ── Module Structure ─────────────────────────────────────────────────


@pytest.mark.terraform
class TestTerraformPartAModules:
    """Verify expected module directories and definitions exist."""

    @pytest.mark.parametrize("module_name", _EXPECTED_MODULES)
    def test_module_directory_exists(self, module_name):
        """Module directory modules/{module_name}/ exists."""
        path = os.path.join(_PART_A_DIR, "modules", module_name)
        assert os.path.isdir(path), f"Expected module directory {path} not found"

    @pytest.mark.parametrize(
        "module_name,pattern",
        _RESOURCE_PATTERNS.items(),
        ids=list(_RESOURCE_PATTERNS.keys()),
    )
    def test_module_defined_in_main(self, module_name, pattern):
        """Expected module {module_name} is defined in main.tf."""
        content = _read_file(os.path.join(_PART_A_DIR, "main.tf"))
        assert content is not None, "main.tf not found"
        assert re.search(pattern, content), f"Module '{module_name}' not found in main.tf"

    def test_four_modules_defined(self):
        """main.tf defines exactly 4 module blocks."""
        content = _read_file(os.path.join(_PART_A_DIR, "main.tf"))
        assert content is not None
        matches = re.findall(r'module\s+"(\w+)"', content)
        assert len(matches) == 4, f"Expected 4 modules, found {len(matches)}: {matches}"
        for m in _EXPECTED_MODULES:
            assert m in matches, f"Module '{m}' missing from main.tf"

    def test_each_module_has_three_files(self):
        """Each module directory has main.tf, variables.tf, outputs.tf."""
        for mod in _EXPECTED_MODULES:
            mod_dir = os.path.join(_PART_A_DIR, "modules", mod)
            for fname in ["main.tf", "variables.tf", "outputs.tf"]:
                assert os.path.isfile(os.path.join(mod_dir, fname)), f"Module '{mod}' missing {fname}"


# ── Variables ────────────────────────────────────────────────────────


@pytest.mark.terraform
class TestTerraformPartAVariables:
    """Verify variables.tf contains expected inputs."""

    _EXPECTED_VARS = [
        "subscription_id",
        "tenant_id",
        "client_id",
        "resource_group_name",
        "location",
        "storage_account_name",
        "containers",
        "databricks_workspace_name",
        "databricks_sku",
        "sql_server_name",
        "sql_database_name",
        "sql_administrator_login",
        "sql_administrator_password",
        "enable_private_endpoints",
        "sql_allowed_ips",
    ]

    def test_variables_file_exists(self):
        """variables.tf exists."""
        path = os.path.join(_PART_A_DIR, "variables.tf")
        assert os.path.isfile(path)

    @pytest.mark.parametrize("var_name", _EXPECTED_VARS)
    def test_expected_variable_defined(self, var_name):
        """Expected variable {var_name} is defined in variables.tf."""
        content = _read_file(os.path.join(_PART_A_DIR, "variables.tf"))
        assert content is not None
        assert f'variable "{var_name}"' in content, f"Variable '{var_name}' not found in variables.tf"

    def test_enable_private_endpoints_is_bool(self):
        """enable_private_endpoints variable has type bool."""
        content = _read_file(os.path.join(_PART_A_DIR, "variables.tf"))
        assert content is not None
        block = _extract_variable_block(content, "enable_private_endpoints")
        assert block is not None
        assert "type" in block and "bool" in block


def _extract_variable_block(content, var_name):
    """Extract the full variable block for *var_name* from HCL content."""
    pattern = rf'variable\s+"{re.escape(var_name)}"\s*{{(.*?)}}'
    match = re.search(pattern, content, re.DOTALL)
    return match.group(1) if match else None


# ── Outputs ──────────────────────────────────────────────────────────


@pytest.mark.terraform
class TestTerraformPartAOutputs:
    """Verify outputs.tf contains expected values."""

    _EXPECTED_OUTPUTS = [
        "storage_account_name",
        "databricks_workspace_url",
        "databricks_workspace_id",
        "managed_identity_id",
        "server_fqdn",
        "database_name",
        "resource_group_name",
        "location",
        "vnet_id",
        "databricks_subnet_id",
        "sql_subnet_id",
    ]

    def test_outputs_file_exists(self):
        """outputs.tf exists."""
        path = os.path.join(_PART_A_DIR, "outputs.tf")
        assert os.path.isfile(path)

    @pytest.mark.parametrize("output_name", _EXPECTED_OUTPUTS)
    def test_expected_output_defined(self, output_name):
        """Expected output {output_name} is defined in outputs.tf."""
        content = _read_file(os.path.join(_PART_A_DIR, "outputs.tf"))
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
        if not _terraform_inited(_PART_A_DIR):
            pytest.skip("terraform init not run yet. Run 'terraform init' in terraform/part_a/ first.")
        return tf_bin

    def test_terraform_validate_success(self, terraform_path):
        """``terraform validate`` completes with exit code 0."""
        result = subprocess.run(
            [terraform_path, "validate"],
            cwd=_PART_A_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"terraform validate failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        assert "Success" in result.stdout, f"terraform validate did not report success:\n{result.stdout}"

    def test_terraform_fmt_check(self, terraform_path):
        """``terraform fmt --check`` passes (no formatting issues)."""
        result = subprocess.run(
            [terraform_path, "fmt", "--check", "-recursive"],
            cwd=_PART_A_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"terraform fmt --check found formatting issues:\n{result.stdout}"
