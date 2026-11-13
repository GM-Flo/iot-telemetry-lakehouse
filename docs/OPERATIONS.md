# Operations Runbook

Everything needed to build the platform from nothing, run it, check it, and take it down.
Commands are shown for bash; PowerShell equivalents differ only in how environment variables
are set (`$env:NAME = "value"`).

## 1. Prerequisites

| Tool | Used for |
|---|---|
| An Azure subscription | Everything; built and tested on Azure for Students |
| Terraform ≥ 1.9 | `infra/` (Azure Cloud Shell has it, and works where the tenant blocks app registrations) |
| Databricks CLI, the version in `.github/workflows/ci.yml` | Bundles, secrets, files |
| Python 3.11 | Tests and the producer |

On an Azure for Students subscription, run the checks in [DAY-ONE.md](DAY-ONE.md) before the
first `terraform apply`: region policy, VM family quota and whether a workspace deploys.

## 2. Build the platform

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars    # subscription, region, budget email
# first apply with deploy_databricks = false to prove the subscription allows the rest,
# then set it to true and databricks_sku = "premium" (row filters need Premium)
terraform init
terraform apply
terraform output                                 # names and URLs; secrets stay hidden
```

Then, once, in the Databricks workspace:

1. **Storage credential.** Catalog → External data → Credentials → create `iot_lake_cred`
   from the access connector ID (`terraform output access_connector_id`). Credentials for
   managed identities cannot be created from SQL.
2. **Secret scope** with the listen-only connection strings:
   ```bash
   databricks secrets create-scope iot
   terraform output -json consumer_connection_strings     # read the two values
   databricks secrets put-secret iot eventhub-listen-conn-dev  --string-value "<dev listen string>"
   databricks secrets put-secret iot eventhub-listen-conn-prod --string-value "<prod listen string>"
   ```

## 3. Dev environment

```bash
pip install -r requirements-dev.txt
pytest -m "not slow"
databricks bundle deploy                                      # target dev, owned by you
databricks bundle run setup_environment --params catalog=iot_dev
```

`setup_environment` creates the external locations, the catalog, the medallion schemas, the
checkpoint and landing volumes, the gold access table with its row filter and VIN mask, and
makes you the platform admin. The first time, pass `storage_account=<name>` as well; later
environments find it from the existing external location.

## 4. Prod environment

1. **Service principal.** Workspace settings → Identity and access → Service principals → add
   a Databricks-managed principal named `iot-lakehouse-cicd`. Give it workspace access,
   Databricks SQL access and unrestricted cluster creation; generate an OAuth secret with
   the `all-apis` scope.
2. **GitHub.** Repository variable `DATABRICKS_HOST`; repository secrets
   `DATABRICKS_CLIENT_ID` (the application ID) and `DATABRICKS_CLIENT_SECRET`. Create the
   `prod` environment, deployable from `main` only.
3. **Catalog and grants**, run by an admin:
   ```bash
   databricks bundle run setup_environment --params catalog=iot_prod
   ```
   This also grants the service principal all privileges on `iot_prod` and read on the secret
   scope, adds it as platform admin, and gives you read-only access.
4. **Deploy.** GitHub → Actions → CI/CD → Run workflow → `main`.

## 5. Load data

```bash
# Registry: one file per day, the date in the name is the snapshot version
python src/producer/produce.py --env prod --export-registry fleet_registry_2026-11-09.json
databricks fs mkdir dbfs:/Volumes/iot_prod/bronze/landing/registry
databricks fs cp fleet_registry_2026-11-09.json \
  dbfs:/Volumes/iot_prod/bronze/landing/registry/fleet_registry_2026-11-09.json

# Telemetry: the send-only string for the same environment
export EVENTHUB_CONN_STR="<prod send string>"           # ends with EntityPath=vehicle-telemetry-prod
python src/producer/produce.py --env prod --mode backfill --backfill-hours 168
```

| Producer mode | Command | Notes |
|---|---|---|
| Backfill | `--mode backfill --backfill-hours 168` | History up to now at 30-second steps, sent in batches of 120 steps. A 7-day backfill is about 790,000 messages and took about an hour from a laptop. |
| Resume a backfill | `--mode backfill --resume-backfill` | Continues an interrupted backfill from the saved state, with no gap and no repeats |
| Live | `--mode live` | Real time until Ctrl+C |
| Schema rollout | `--fpl-v2-share 0.3` | 30% of FleetPulse devices send the v2 contract |
| Late registration | `--export-registry … --include-late-registrations` | Includes the device that was not registered |

The fleet's state is saved in `producer_state_<env>.json`, so every run continues the same
fleet. `--fresh-fleet` starts over.

## 6. Run

```bash
databricks bundle run lakehouse_refresh            # dev
```

In prod, run the `lakehouse_refresh` job from the Jobs page. It runs the two bronze jobs and
the three pipelines in order, each after the one before succeeds.

| Run | Duration on one single-node cluster |
|---|---|
| First prod run, 788,055 messages | 1 h 21 min |
| Later runs, a few tens of thousands of messages | About 1 h, most of it cluster start-up and full recomputes |

A failed or cancelled run continues with **Repair run** from the failed task; streaming steps
resume from their checkpoints. Event Hubs keeps messages 3 days, so a run must happen at
least that often or unread messages expire.

## 7. Check

```sql
-- Nothing lost, nothing invented
SELECT lost_readings, unexpected_readings, expected_readings, stored_readings, checked_at
FROM iot_prod.operations.silver_reconciliation;

-- Recent pipeline runs
SELECT started_at, final_state, duration_seconds, rows_to_vehicle_measurements,
       rows_quarantined, failed_flows, expectation_failures
FROM iot_prod.operations.pipeline_runs ORDER BY started_at DESC;

-- What was quarantined, and why
SELECT quarantine_stage, quarantine_reason, vendor_id, SUM(quarantined) AS quarantined
FROM iot_prod.operations.quarantine_summary_by_hour
GROUP BY ALL ORDER BY quarantined DESC;
```

`pipeline_runs` lists finished updates only, and is built during an update, so a run appears
there after the next run. The Platform Health dashboard shows the same three views.

## 8. Change something

| Change | How |
|---|---|
| Code, pipelines, dashboards | Commit, push, green CI, then Run workflow on `main` for prod |
| A new vendor contract version | A schema in `contracts/`, a row in `vendor_contract_versions.csv`, rows in `vendor_field_mappings.csv` |
| A threshold or validity limit | A row in `reference/anomaly_thresholds.csv` or `validity_limits.csv` |
| A dashboard | Layout in `src/iot/dashboards.py`, query in `dashboards/*.sql`, then `python -m src.iot.dashboards` and commit both |
| A new fleet manager | A row in `gold.client_access` |
| A schema change that breaks a checkpoint | A full refresh of only the affected tables: `--full-refresh a,b`, then a normal run so the operations views catch up |

## 9. Pause

Nothing runs unless started: clusters stop when their job ends and the SQL warehouse stops
after its auto-stop delay. While paused, the platform still costs about $1 to $2 a day: the
NAT gateway of the Databricks workspace's managed network and the Event Hubs namespace's
throughput unit are billed by the hour whether or not data flows.

## 10. Tear down

1. Capture anything still needed: screenshots, query results.
2. From the folder that holds `terraform.tfstate`: `terraform plan -destroy -out destroy.tfplan`,
   check the list, then `terraform apply destroy.tfplan`. The state is local, so keep a copy
   until the teardown is confirmed; Cloud Shell without a file share loses it when the session ends.
3. `az group list -o table`: the project resource group and the workspace's managed resource
   group should both be gone. If the managed group remains with only its root storage account,
   the Unity Catalog access connector and an Event Grid topic, and
   `az resource list --resource-type Microsoft.Databricks/workspaces` is empty, delete it with
   `az group delete --name <managed resource group>`.
4. GitHub: delete `DATABRICKS_HOST` and the two secrets. `validate` and `deploy-prod` then skip
   themselves; lint and tests keep running.
5. Azure Cost analysis, a day or two later: confirm the daily cost has dropped to zero.
