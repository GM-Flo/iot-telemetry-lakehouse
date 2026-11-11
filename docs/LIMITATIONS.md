# Limitations and How This Would Scale

What this build does not do, why, and what the next step would be. Most limits come from the
subscription: one 4-vCPU cluster at a time and $100 of credit.

## 1. Full recomputes

| What | Now | At scale |
|---|---|---|
| The 10 event tables and gold | Materialized views rebuilt from all history on every run; classic compute cannot refresh them incrementally | Serverless pipelines, which refresh materialized views incrementally; or event tables kept as streaming tables with a reprocessing window of the last few days |
| The reconciliation | Re-parses and re-checks all of bronze in batch every run, about 20 minutes of the silver pipeline's 28 | A separate daily audit job, so the refresh stays fast and the audit still runs |

Measured on prod, a refresh with no new data took about an hour: 25 minutes of cluster
start-up, about 20 minutes of reconciliation, the rest rebuilding events and gold.

## 2. Start-up time

Each of the five refresh steps starts its own cluster, about 5 minutes each. Instance pools
would keep VMs warm, at the price of paying for them while idle; serverless compute removes
start-up altogether. Merging the events and gold pipelines would save one start, but would
drop and rebuild every gold table, including its security, for 5 minutes.

## 3. Latency

Data is as fresh as the last refresh, because bronze runs with `availableNow`. The ingest code
is the same as a continuous stream's: changing the trigger to processing time and running the
pipelines in continuous mode makes it real-time, at the cost of always-on compute.

## 4. Quarantine replay is designed, not built

The 20,852 readings of the device registered late are still in quarantine
([ADR-022](DECISIONS.md#adr-022-late-registration-is-fixed-forward-not-replayed)). Recovering
them would take:

1. A replay request, for example a row in a reviewed `reference/quarantine_replays.csv`: an ID,
   the reason and the device.
2. A one-time AUTO CDC flow into `vehicle_measurements` (`once=True`, which Lakeflow supports
   for SCD1 targets alongside the main flow) that reads the quarantined readings' messages
   back from bronze and runs them through parse, contract, normalize, validity and matching
   against the corrected registry.
3. Readings that pass are stored; deduplication on `(device_id, event_ts)` makes the replay
   safe to repeat. Readings that still fail stay where they are.
4. A `recovered_readings` count in the reconciliation: quarantined, but now stored.

## 5. One workspace for dev and prod

Environments are separated by catalog, hub, secret and identity, but share a workspace and its
admins ([ADR-016](DECISIONS.md#adr-016-dev-and-prod-in-one-workspace)). At scale, prod would be
its own workspace, ideally its own subscription, with no human admins in daily use.

## 6. Smaller gaps

| Gap | Note |
|---|---|
| Row counts for SCD2 master data in `pipeline_flow_runs` | Snapshot CDC flows report their changes under different metrics; the tables are correct, only the run log's totals miss them |
| The quarantine chart groups by arrival day | A backfill arrives in one day, so a week of history shows as one bar. Correct, but worth knowing when reading it |
| Odometer-based sequence checks | A vehicle with a broken odometer would need another signal, such as GPS distance |
| A UDF for contract validation | The most expensive step in silver; a compiled validator or validating only changed contracts would help at volume |
| No alerting | Expectations and the reconciliation record failures; a SQL alert on `lost_readings > 0` or a failed run would page someone |
| Fixed UTC offsets per region | Correct for the simulated regions; real fleets need IANA time zones and daylight saving |
