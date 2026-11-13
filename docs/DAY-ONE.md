# Day One: Azure for Students Checks

This project was built on an **Azure for Students** subscription: $100 of credit, no credit
card, no quota increases. That kind of subscription restricts regions, VM families and
identity, and documentation alone does not say how far. These six checks were run before any
pipeline code was written, because their answers decide the architecture.

Run them again before building this on another restricted subscription.

## Results

| # | Check | Result | What it changed |
|---|---|---|---|
| 1 | Which regions the subscription's policy allows | `uaenorth` permitted; the usual US regions were not | Every resource is in `uaenorth`, set through `location` in `terraform.tfvars` |
| 2 | vCPU quota for a VM family Databricks can use | 4 vCPUs for the D-series family | One `Standard_D4ds_v4` single-node cluster at a time ([ADR-024](DECISIONS.md#adr-024-single-node-clusters-one-at-a-time)) |
| 3 | Does a Databricks workspace deploy | Yes, Premium tier | Unity Catalog, row filters and Lakeflow are all available |
| 4 | Which compute starts: classic cluster, serverless SQL warehouse, serverless notebooks | All three | Classic job clusters for the pipelines, the serverless warehouse for dashboards, serverless for the one-off setup job |
| 5 | Is there a Unity Catalog metastore in the region | Yes, provisioned with the workspace | Catalogs per environment, lineage, row filters and column masks |
| 6 | Can the account create an Entra ID app registration | **No**: the university tenant blocks it | Terraform runs from Azure Cloud Shell; CI/CD uses a Databricks-managed service principal with OAuth instead of an Entra one ([ADR-017](DECISIONS.md#adr-017-a-databricks-managed-service-principal-owns-prod)) |

## How to run each check

### 1. Allowed regions

```bash
az policy assignment list --query "[].{name:displayName, policy:policyDefinitionId}" -o table
```

Open the "Allowed resource deployment regions" assignment and read its parameter list. Pick a
permitted region; latency does not matter for this project, availability and quota do.

### 2. vCPU quota per family

Total regional cores are not enough: the specific family must have quota, and Databricks runs
on D, E and F series, not the B series student subscriptions are steered towards.

```bash
az vm list-usage --location <region> -o table | grep -Ei "total regional|Dv[3-5]|Ddsv|Esv|Fsv"
```

Do not file a quota increase request: student subscriptions are not eligible.

### 3. Workspace deployment

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars    # deploy_databricks = false
terraform init && terraform apply                # storage, Event Hubs, budget: no vCPUs used
terraform apply -var="deploy_databricks=true"    # then the workspace on its own
```

`SubscriptionNotRegistered` is fixed with
`az provider register --namespace Microsoft.Databricks`; a billing or offer error is not.

### 4. Compute

Open the workspace from the Azure portal with the identity that created it, which makes it
workspace admin. Then try, in turn: a single-node cluster with a 10-minute auto-termination,
a serverless SQL warehouse, and `spark.range(5).count()` on serverless notebook compute.

### 5. Unity Catalog

Catalog → check that the workspace has a metastore attached. New workspaces in most regions
get one automatically. Without it the project would fall back to `hive_metastore` and lose
lineage, row filters and column masks.

### 6. App registration

```bash
az ad app create --display-name "iot-lakehouse-cicd" --query appId -o tsv
```

An authorization error means the tenant restricts app registrations; use a
Databricks-managed service principal for CI/CD, as this project does.
