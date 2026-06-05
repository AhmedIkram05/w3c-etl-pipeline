# Shared Credentials

The same Azure service principal is used for:
1. Terraform remote state backend access (ARM_* env vars)
2. Tier 2 GitHub Actions CI (ARM_* secrets)

Service Principal: w3c-etl-pipeline-sp
Role: Contributor (subscription-level)
Additional Role: Storage Blob Data Contributor (on tfstate storage account)

This credential reuse simplifies management but requires:
- Regular rotation (quarterly)
- Monitoring for unusual activity
- Immediate revocation if compromised
