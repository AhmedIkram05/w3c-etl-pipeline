# Security policy

## Reporting a vulnerability

Open a private security advisory on this repository (Security → Report a vulnerability). Please do not file a public issue for anything that exposes credentials or data.

## Known credential exposure (open)

An **Azure SQL password** was committed in plaintext to `pipeline/.env.example`, added in commit `f6e2c0d` and removed from the working tree in `4cfe68d`. The same file also carried the real server hostname, and the Airflow DAG carried a real Databricks job ID as a fallback value.

Current state:

| | status |
| --- | --- |
| Files at `HEAD` | scrubbed — placeholders only |
| Git history | **still contains the plaintext secret** |
| Anyone with a clone or a fork | has the secret |
| Rotation | **not done — this is the only real fix** |

Scrubbing `HEAD` does not un-leak the value. A history rewrite (filter-repo/BFG + force-push) removes it from *future* clones and is worth doing, but it cannot recall copies that already exist, and it breaks every existing clone. **Rotation is therefore the required remediation; the rewrite is optional cleanup.**

### Rotation runbook (repo owner)

1. Rotate the Azure SQL login password (Portal → SQL database → Connection strings, or `ALTER LOGIN <user> WITH PASSWORD = ...`). If the SQL server is deleted, confirm the credential is dead — that is a valid outcome.
2. Rotate the Databricks personal access token if one was ever pasted into an env file, and confirm the job ID is not itself a secret (it is not, but the workspace URL plus token is).
3. Update the secret store the deployment reads from, then `pipeline/.env` locally. Never commit the new value.
4. Verify: the pipeline's connection check must succeed with the new credential and fail with the old one. Record the date here.

### Until rotation is confirmed

Treat the Azure SQL credential as compromised: no new database may reuse that password, and anything reachable with it should be considered readable.

## Prevention

- `pipeline/.env` is gitignored; only `*.example` / `*.template` files are tracked, and they must contain placeholders (`<change-me>`, `your-...`) rather than values.
- `tests/test_secret_hygiene.py` fails the build when a tracked file assigns a value that looks like a real secret to a secret-named variable.
- Secret scanning: enable GitHub push protection in repository settings — it rejects a commit containing a recognised credential before it reaches history.
