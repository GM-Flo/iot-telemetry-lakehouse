# Costs

Built on an Azure for Students subscription: $100 of credit, no credit card, no quota
increases. Cost was a design input from the first day.

## What it cost

Azure Cost analysis, grouped by meter, for the whole build up to the first production load
(2026-11-09):

| Meter | Cost (USD) | Share |
|---|---|---|
| Azure Databricks (DBUs: jobs, pipelines, SQL warehouse) | 33.50 | 80% |
| NAT gateway (Databricks managed network) | 3.26 | 8% |
| Event Hubs (Standard, 1 throughput unit) | 2.81 | 7% |
| Virtual machines (cluster nodes) | 1.90 | 5% |
| Virtual network | 0.36 | 1% |
| Storage | 0.11 | 0% |
| **Total** | **41.94** | |

**The whole project, from the first `terraform apply` to the end of the production runs,
cost $75.62 of the $100 credit.** The second half of that, about $34, covered the 7-day
production backfill, live runs, several refresh and rebuild runs, and the workspace, network
and Event Hubs charges that run by the hour. The screenshot is in
[EVIDENCE.md](EVIDENCE.md#azure-cost).

## What drove the cost

| Driver | What happened | What changed |
|---|---|---|
| The serverless SQL warehouse | The largest single spike: dashboard development left the default warehouse running at its default size | Resized to 2X-Small with the shortest auto-stop; queries run in batches rather than one by one |
| Cluster start-ups | Every job and pipeline starts its own cluster, about 5 minutes each, 25 minutes per refresh | Accepted: an instance pool would keep VMs warm but bill them while idle |
| Full recomputes | Event tables, gold and the reconciliation rebuild over all history each run | Documented as the first thing to change at scale; see [LIMITATIONS.md](LIMITATIONS.md) |
| Always-on network | The NAT gateway and the Event Hubs throughput unit bill by the hour, data or not | About $1 to $2 a day while idle; the reason to tear the platform down when done |

## Choices made for cost

| Choice | Saving |
|---|---|
| Triggered streaming (`availableNow`) instead of continuous | No always-on cluster |
| Single-node `Standard_D4ds_v4` clusters, Photon off | The smallest compute that runs Unity Catalog pipelines |
| Job clusters only, no all-purpose cluster | Nothing runs between jobs |
| `pipelines.clusterShutdown.delay: 60s` | Pipeline clusters stop a minute after an update, not after the default |
| Local Spark for 416 tests | Development and CI run no Databricks compute |
| A subscription budget with alerts, in Terraform | Early warning before the credit runs out |
| Tear down after the project | Stops the hourly network and Event Hubs charges |

## Per refresh, at this scale

One `lakehouse_refresh` runs five single-node clusters in sequence for about an hour: roughly
$0.50 to $0.70 per cluster-hour for the VM and the DBUs, so about $1 to $3 per run including
start-ups.
