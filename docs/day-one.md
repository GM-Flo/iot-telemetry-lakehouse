# Day One: Environment Verification

This project targets an **Azure for Students** subscription (`MS-AZR-0170P`, $100 credit,
no credit card). That subscription type imposes restrictions that change the architecture,
and none of them can be determined from documentation alone. Run these six checks before
writing pipeline code.

Record the result of each one in the table at the bottom. The answers decide which
compute path the project takes.

---

## Why this file exists

Azure for Students is **not** a smaller Azure Free Trial. The differences that matter here:

| | Free Trial | Azure for Students |
|---|---|---|
| Credit | $200 / 30 days | $100 / 12 months |
| Credit card | Required | Not required |
| Upgrade to Pay-As-You-Go | Supported, keeps remaining credit | Loses student benefits |
| vCPU quota | 4, no increases | 4, no increases |
| Region allowlist policy | No | **Yes** |
| VM SKU restrictions | Minimal | **Steered to B-series** |

The last two rows are the problem. Databricks classic clusters run on D/E/F-series node
types. It does not offer B-series. So even a single-node cluster that fits inside 4 vCPUs
can fail with `SkuNotAvailable` rather than a quota error.

Published reports on this are contradictory: some say Azure for Students cannot create a
Databricks workspace at all because DBUs require Pay-As-You-Go billing, while others show
a workspace deploying successfully but exposing only SQL Warehouses in the Compute menu,
with no all-purpose Spark cluster option. Both outcomes are plausible. Check 3 settles it
for your subscription.

---

## Check 1 — Allowed regions

Azure for Students applies an "Allowed resource deployment regions" policy. Students
sometimes see a policy whose permitted regions do not overlap the regions offered in the
portal's VM creation blade.

```bash
az login
az account show --query "{name:name, id:id, type:user.type}" -o table

az policy assignment list \
  --query "[].{name:displayName, policy:policyDefinitionId}" -o table

az account list-locations --query "[].name" -o tsv | sort
```

**Pass:** `eastus` appears, or you identify a permitted alternative.
**If it fails:** set `location` in `terraform.tfvars` to a region the policy permits.
Latency is irrelevant for this project; quota and policy availability are the only criteria.

---

## Check 2 — vCPU quota, per family

Total regional cores being ≥ 4 is not sufficient. The specific VM family quota must also
be non-zero, and Databricks needs a D/E/F-series family.

```bash
az vm list-usage --location eastus -o table | grep -Ei "total regional|DSv[2-5]|Dv[3-5]|Esv|Fsv"
```

**Pass:** `Standard DSv3 Family vCPUs` (or DSv2/Dv3) shows a limit ≥ 4.
**If it shows 0:** that family cannot start a VM at all. Databricks classic compute is out,
and the project depends entirely on serverless (Check 4) or a non-Databricks fallback.

Do **not** file a quota increase request. Free, student and sponsored subscriptions are not
eligible, and the request will be closed.

---

## Check 3 — Does the Databricks workspace deploy? (the gating unknown)

Terraform answers this faster than the portal, and leaves you with real infrastructure
either way.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# edit subscription_id, location, budget_alert_emails; leave deploy_databricks = false

terraform init
terraform validate
terraform apply
```

That first apply creates storage, containers, Event Hubs and the budget — all PaaS, zero
vCPU footprint. It will succeed on a student subscription.

Then flip the flag and try the workspace on its own:

```bash
terraform apply -var="deploy_databricks=true"
```

**Pass:** apply completes and `databricks_workspace_url` is populated.
**If it fails:** capture the exact ARM error code. `SubscriptionNotRegistered` is fixable
with `az provider register --namespace Microsoft.Databricks`. A billing or offer-restriction
error is not fixable and means the fallback decision in the README applies.

---

## Check 4 — Does any compute start?

Serverless runs in Databricks' own subscription, so it bypasses your regional core quota
entirely. This single answer decides roughly a third of the architecture.

Open the workspace from the Azure portal **using the identity that created it** — portal
launch is what assigns workspace admin. Then:

1. `Settings → Identity and access → Users → your account`. Confirm **Admin access**, or
   **Workspace access** plus **Allow unrestricted cluster creation**. If only SQL Warehouses
   appear in Compute, missing entitlements are the more likely cause than an offer
   restriction — sign out and back in after fixing them.
2. `Compute → Create compute`. Single node, smallest available node type, autotermination
   **10 minutes**. Try to start it.
3. `SQL Warehouses → Create`. Choose **Serverless**. Try to start it.
4. `Compute → Serverless` notebook attach. Try to run `spark.range(5).count()`.

**Record which of the three started.** If serverless works and classic does not, the project
runs serverless-only: use Lakeflow Declarative Pipelines for silver/gold and skip the
hand-rolled checkpoint work, since serverless manages it.

> Trial accounts registered with personal email addresses can face limited scaling of
> serverless resources. If you have a university or custom-domain address, prefer it.

---

## Check 5 — Unity Catalog metastore

Creating a metastore needs Databricks **account admin**. If you signed up with a personal
email you are the admin. On a university tenant where you are not, this is blocked.

Go to the Databricks **account console** (`accounts.azuredatabricks.net`) →
`Catalog`. Newly created workspaces are usually Unity Catalog enabled by default with a
metastore pre-provisioned in the region.

**Pass:** a metastore exists in your workspace's region.
**If it fails:** the project falls back to `hive_metastore` with `abfss://` paths. You lose
column-level lineage, which is the single most valuable screenshot in the README — so retry
this before giving up on it.

Either way: **never use `dbutils.fs.mount()` or `/mnt/` paths.** Mounts are the clearest
signal of a dated project. Use `abfss://` URIs directly, which the `abfss_paths` Terraform
output gives you.

---

## Check 6 — Entra app registration

Needed for the Terraform service principal and GitHub Actions OIDC. Many tenants restrict
app registration to administrators.

```bash
az ad app create --display-name "iot-lakehouse-cicd" --query appId -o tsv
```

**Pass:** an application ID comes back.
**If it fails** with an authorization error: CI/CD authenticates with a Databricks personal
access token in a GitHub secret instead of OIDC. This is a real downgrade — "no credentials
in the repo" is a selling point — so note it in the README rather than hiding it.

---

## Record your results

| # | Check | Result | Notes |
|---|-------|--------|-------|
| 1 | `eastus` permitted by policy | ⬜ | |
| 2 | D/E/F-series family quota ≥ 4 | ⬜ | |
| 3 | Databricks workspace deploys | ⬜ | ARM error code if not: |
| 4a | Classic single-node cluster starts | ⬜ | |
| 4b | Serverless SQL Warehouse starts | ⬜ | |
| 4c | Serverless notebook compute runs | ⬜ | |
| 5 | Unity Catalog metastore exists | ⬜ | |
| 6 | Entra app registration permitted | ⬜ | |

---

## What each outcome means

| Checks 4a / 4c | Path |
|---|---|
| Both work | Full architecture as designed. Hand-roll bronze on classic, Lakeflow pipelines for silver/gold. |
| Serverless only | Serverless-only build. Lakeflow pipelines throughout. You lose the checkpoint/watermark internals that streaming interviews probe, so compensate by covering them in the README. |
| Neither works | Databricks is out on this subscription. Event Hubs, ADLS Gen2, Delta, the medallion layers, the transform tests and the CI/CD half all survive — run Spark locally in Docker against the same Event Hubs Kafka endpoint and the same `abfss://` storage. Unity Catalog lineage is the only real casualty. |

Everything in `src/`, `tests/` and `.github/` is independent of this outcome. That is
deliberate — it is also the half that most clearly demonstrates engineering practice, so it
gets built first regardless of what the portal says.