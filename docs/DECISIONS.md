# Architecture Decision Records

Each record states the situation, what was decided, what else was considered, and what the
decision costs. Several decisions were revised after a real failure; those records say so,
because the failure is the reason.

| # | Decision | Area |
|---|---|---|
| [001](#adr-001-bronze-stores-messages-unparsed) | Bronze stores messages unparsed | Ingestion |
| [002](#adr-002-event-hubs-through-its-kafka-endpoint-triggered-not-continuous) | Event Hubs through its Kafka endpoint, triggered, not continuous | Ingestion |
| [003](#adr-003-a-json-schema-contract-per-vendor-format-version) | A JSON Schema contract per vendor format version | Quality |
| [004](#adr-004-normalization-driven-by-a-mapping-table-with-a-fixed-vocabulary) | Normalization driven by a mapping table, with a fixed vocabulary | Silver |
| [005](#adr-005-four-quarantine-tables-with-one-shape) | Four quarantine tables with one shape | Quality |
| [006](#adr-006-one-query-shape-for-every-contract) | One query shape for every contract | Silver |
| [007](#adr-007-the-registry-decides-identity-as-of-the-readings-date) | The registry decides identity, as of the reading's date | Silver |
| [008](#adr-008-deduplicate-on-the-natural-key-with-auto-cdc) | Deduplicate on the natural key with AUTO CDC | Silver |
| [009](#adr-009-reconcile-by-identity-not-by-count) | Reconcile by identity, not by count | Quality |
| [010](#adr-010-event-tables-as-materialized-views) | Event tables as materialized views | Silver |
| [011](#adr-011-find-out-of-sequence-readings-by-the-odometer) | Find out-of-sequence readings by the odometer | Silver |
| [012](#adr-012-hash-surrogate-keys-and-as-was-facts) | Hash surrogate keys and "as was" facts | Gold |
| [013](#adr-013-local-days-per-region) | Local days per region | Gold |
| [014](#adr-014-row-filters-driven-by-an-access-table) | Row filters driven by an access table | Security |
| [015](#adr-015-row-level-dashboard-datasets-defined-as-code) | Row-level dashboard datasets, defined as code | Serving |
| [016](#adr-016-dev-and-prod-in-one-workspace) | Dev and prod in one workspace | Platform |
| [017](#adr-017-a-databricks-managed-service-principal-owns-prod) | A Databricks-managed service principal owns prod | CI/CD |
| [018](#adr-018-prod-deploys-only-on-a-manual-run) | Prod deploys only on a manual run | CI/CD |
| [019](#adr-019-environment-bootstrap-as-a-parameterized-job) | Environment bootstrap as a parameterized job | Platform |
| [020](#adr-020-a-stateful-simulator-with-planted-faults) | A stateful simulator with planted faults | Testing |
| [021](#adr-021-fast-tests-on-every-push-the-full-suite-on-main) | Fast tests on every push, the full suite on main | CI/CD |
| [022](#adr-022-late-registration-is-fixed-forward-not-replayed) | Late registration is fixed forward, not replayed | Operations |
| [023](#adr-023-a-silent-day-is-no_data-not-healthy) | A silent day is `no_data`, not healthy | Gold |
| [024](#adr-024-single-node-clusters-one-at-a-time) | Single-node clusters, one at a time | Platform |
| [025](#adr-025-a-trip-is-one-drivers-journey) | A trip is one driver's journey | Silver |

---

## ADR-001: Bronze stores messages unparsed

**Context.** Vendors send four formats and change them. Parsing at ingest ties bronze to
today's understanding of each format.

**Decision.** Bronze keeps each message as raw text with its Kafka position and timestamps,
and nothing else.

**Alternatives.** Parsing into typed columns at ingest; Auto Loader schema inference.
Both lose data the moment a format changes, and a parsing bug would corrupt the only copy.

**Consequences.** Every later layer can be rebuilt from bronze: a parser fix is a silver
rebuild, not a re-ingest. Bronze is less useful to query directly, which is acceptable
because nobody should.

## ADR-002: Event Hubs through its Kafka endpoint, triggered, not continuous

**Context.** The platform needs streaming semantics (exactly-once offsets, checkpoints) but
runs on $100 of credit.

**Decision.** A hand-written Structured Streaming job reads the Event Hubs Kafka endpoint
with `trigger(availableNow=True)`: it processes everything waiting, commits, and stops.

**Alternatives.** A continuously running stream (always-on cluster, unaffordable); the Event
Hubs Spark connector (a second protocol to learn for no gain); ingesting inside the Lakeflow
pipeline (hides checkpoints and offsets, which were part of the learning goal).

**Consequences.** Data latency equals the refresh interval, not seconds. The code is the
same as a continuous stream's: changing the trigger makes it real-time. Event Hubs retains
messages for 3 days, so the refresh must run at least that often.

## ADR-003: A JSON Schema contract per vendor format version

**Context.** "Never assume clean data": a vendor can drop a field, change a type or
rename a key at any time.

**Decision.** Each vendor format version has a JSON Schema in `contracts/`. A message is
checked against the contract its envelope claims before any field is read. Which contracts
a vendor may send is configuration (`vendor_contract_versions.csv`).

**Alternatives.** Schema inference with rescued-data columns (silently accepts drift);
checking only the fields that are used (misses a renamed field until it becomes null);
Lakeflow expectations alone (can say a row failed, not which rule of a nested schema).

**Consequences.** A violating message is quarantined with the first error in readable form,
for example `$.speedMph: 'fast' is not of type 'number'`, which is what a vendor
needs to fix it. The check runs in a Python UDF, the most expensive step in silver.

## ADR-004: Normalization driven by a mapping table, with a fixed vocabulary

**Context.** Four formats, 21 standard fields, unit conversions, nested objects and batched
arrays. New contract versions must not need new code.

**Decision.** `vendor_field_mappings.csv` maps each vendor field to a standard field and
names a conversion. The code holds a closed set of conversions; the CSV can choose one but
not define one.

**Alternatives.** A function per vendor (a code change per version); SQL expressions in the
CSV (configuration that can run arbitrary code, and is untestable).

**Consequences.** FleetPulse v2 was supported with 21 new CSV rows and a schema file, and was
live without a code change. Mappings are validated at pipeline start: an unknown conversion
or standard field fails the run before any data is touched.

## ADR-005: Four quarantine tables with one shape

**Context.** Bad data must never be dropped, and must be easy to count, explain and
recover.

**Decision.** One quarantine table per stage (parse, contract, validity, matching), all with
the same columns: identity, stage, reason code, readable detail, Kafka position.

**Alternatives.** One shared quarantine table (each stage's useful context differs, and a
single table needs nullable everything); Lakeflow `expect_or_drop` (drops silently, keeps
only counts); failing the pipeline (one bad message would stop every client).

**Consequences.** Each table holds what its stage needs: parse and contract failures keep
the raw payload, validity and matching failures keep the normalized reading. The common
columns let `quarantine_summary_by_hour` union all four. The tables are append-only, so a
reading delivered twice and rejected twice appears twice; the reconciliation accounts for
that (ADR-009).

## ADR-006: One query shape for every contract

**Context.** The first normalize built one branch per contract and unioned them. When the
FleetPulse v2 contract was added, the pipeline failed: *"There are [4] sources in the
checkpoint offsets and now there are [5]"*. A streaming checkpoint records the shape of its
query, and a new branch is a new source.

**Decision.** Normalize is a single stream in and a single stream out. Every standard field
is a `CASE` on the contract, and each contract's payload is parsed with its own `from_json`
inside the same projection. A test counts the streaming sources and fails if adding a
contract changes the number.

**Alternatives.** One branch per contract (breaks every downstream checkpoint per new
version); one pipeline per vendor (multiplies clusters on a 4-vCPU quota).

**Consequences.** Adding a contract never touches a checkpoint. Recovering from the incident
took a full refresh of the three affected tables; planning time of the new design is close
to the old one.

## ADR-007: The registry decides identity, as of the reading's date

**Context.** Devices are installed, swapped and retired. A device's claim about its VIN or
account can be wrong.

**Decision.** Readings are matched to `device_assignments` by device and the reading's date
(`valid_from` inclusive, `valid_to` exclusive). A passing reading takes vehicle, client and
driver from the registry. An unknown driver fob is a warning, not a failure: it does not make
engine data wrong.

**Alternatives.** Trusting the device's fields (four of the planted faults would reach
gold); matching against today's registry (a device swapped last month would put old readings
on the new vehicle).

**Consequences.** All four planted integration faults were quarantined with the right reason,
none reached gold. The first version of each client and driver is open from the beginning of
time, so readings older than the first snapshot still match.

## ADR-008: Deduplicate on the natural key with AUTO CDC

**Context.** Event Hubs is at-least-once, vendors re-send readings in new messages, and a
replay would deliver readings again.

**Decision.** `vehicle_measurements` is an SCD type 1 AUTO CDC target keyed on
`(device_id, event_ts)`, sequenced by Event Hubs enqueue time and offset: the copy received
last wins.

**Alternatives.** Deduplicating on `message_id` (a re-sent reading has a new message ID);
`dropDuplicates` with a watermark (forgets keys after the watermark and drops late
duplicates' updates); merging in a separate job (Lakeflow owns the table).

**Consequences.** Duplicates of any origin collapse into one row however late they arrive.
GeoTrak sends timestamps rounded to the second, so two different readings can share a key;
those are counted as conflicting duplicates (321 in prod) rather than hidden.

## ADR-009: Reconcile by identity, not by count

**Context.** The first reconciliation compared counts: bronze readings against stored plus
quarantined plus duplicates. It reported −3 unaccounted readings while nothing was lost:
duplicates rejected twice sat twice in the append-only quarantine, while the stored side
collapsed them.

**Decision.** A batch recount of bronze, run through parse, contract and normalize again,
independent of checkpoints, joins and AUTO CDC, is compared by identity with every stored
and quarantined reading. `lost_readings` (expected, found nowhere) and `unexpected_readings`
(found, never expected) must be 0, enforced by pipeline expectations. Duplicates are reported
separately, and copies whose content differs are counted as conflicts.

**Alternatives.** Row counts (the −3 above); trusting the pipeline's own metrics (they come
from the same code that would have the bug).

**Consequences.** The check is exact and explains itself. It also recomputes all of bronze on
every run, the largest share of the silver pipeline's time; see
[LIMITATIONS.md](LIMITATIONS.md).

## ADR-010: Event tables as materialized views

**Context.** Trips, idling and speeding are sequences across readings. Readings arrive late
and out of order.

**Decision.** Event tables are materialized views over all of `vehicle_measurements`, built
with window functions: change detection, gaps and islands, and bucket counting.

**Alternatives.** Stateful streaming with watermarks (drops readings later than the
watermark, and a trip split across two micro-batches is hard to get right).

**Consequences.** A late reading is sorted into place on the next run and every event is
exact. The cost is a full recompute on classic compute, which grows with history.

## ADR-011: Find out-of-sequence readings by the odometer

**Context.** 5% of readings come from devices whose clock runs behind, so they carry an
older timestamp than their values. They made trips appear to go backwards.

**Decision.** Per vehicle and day, keep the longest non-decreasing subsequence of odometer
values, an O(n log n) algorithm, and flag the rest. Every event table leaves flagged readings
out; `vehicle_measurements` keeps them.

**Alternatives.** Comparing each reading with its neighbours (the first design; it missed
two bad readings in a row); fixing the producer (the fault is realistic, so the platform must
handle it).

**Consequences.** On simulator data, 99.8% of shifted readings are found with no false flags,
and no trip has a negative distance. A vehicle with a broken odometer would need another
signal.

## ADR-012: Hash surrogate keys and "as was" facts

**Context.** Gold is rebuilt on every run. Dimensions are SCD2.

**Decision.** Surrogate keys are SHA-256 of the natural key and version start; each dimension
has an `unknown` member. Facts point at the dimension version in force when the event
happened.

**Alternatives.** Identity columns or `monotonically_increasing_id` (keys change on rebuild,
breaking anything that stored them); "as is" facts pointing at the current version
(re-attributes history when a driver changes client).

**Consequences.** Keys are stable across rebuilds and computable in Python. A trip taken
before a driver moved to another client stays with the old client.

## ADR-013: Local days per region

**Context.** "Per day" for a fleet manager in Texas means their day, not UTC's.

**Decision.** Every daily fact uses the local day of the vehicle's region. Fuel is priced at
the region's price for that local day.

**Consequences.** Daily numbers match what a manager sees on their clock. UTC-based
operational views (`vendor_data_quality`) stay in UTC, and say so.

## ADR-014: Row filters driven by an access table

**Context.** Each fleet company may see only its own rows, in the same tables.

**Decision.** `gold.client_access` maps principals to clients, `*` meaning all. A Unity
Catalog row filter on `client_id` checks it; a column mask shows the VIN only to platform
admins, others see the last four characters. Both are attached to the data products in the
pipeline definition.

**Alternatives.** A view or schema per client (multiplies objects per new client); one
account group per client checked in the filter (works, but the access rules then live
outside the data and cannot be queried or audited with it); filtering in the dashboard
(bypassed by any direct query).

**Consequences.** Onboarding a client is one row. Verified: a user mapped only to C001 sees
C001's six vehicles with masked VINs and nothing else.

## ADR-015: Row-level dashboard datasets, defined as code

**Context.** The first dashboards used widget definitions written from memory over
pre-aggregated datasets: tables failed to render and filters had nothing to filter. They were
rebuilt from the documented Lakeview format.

**Decision.** Layouts live in Python, queries in SQL files, and a generator writes the
Lakeview JSON that the bundle deploys. Datasets are row level, so dashboard filters filter
rows and widgets aggregate; scores and flags a widget cannot express are computed in SQL.
CI fails if the generated files differ from their source.

**Consequences.** Dashboards are reviewed, versioned and deployed like code. A period safety
score is recomputed from totals rather than averaged from daily scores, so a 3 km day does
not weigh as much as a 300 km day.

## ADR-016: Dev and prod in one workspace

**Context.** One subscription, one region, a 4-vCPU quota.

**Decision.** One workspace; environments separated by catalog, Event Hub, secret, bundle
target and identity. Prod is owned and run by a service principal and is view-only for
people.

**Alternatives.** A workspace per environment, the standard at scale, which the quota and
budget could not support.

**Consequences.** A workspace admin can still reach prod. On an unconstrained platform, prod
would be its own workspace with no human admins in daily use.

## ADR-017: A Databricks-managed service principal owns prod

**Context.** CI/CD must deploy without a person's credentials. The university tenant blocks
Entra ID app registrations.

**Decision.** A Databricks-managed service principal with an OAuth secret (machine-to-machine)
deploys prod and owns every prod resource. It has all privileges on `iot_prod` only, and
read access to the Event Hubs secrets.

**Alternatives.** A personal access token (tied to a person, long-lived); an Entra ID service
principal (blocked by the tenant).

**Consequences.** Nothing in prod depends on a person. People get read-only rights on the
catalog through the setup job.

## ADR-018: Prod deploys only on a manual run

**Context.** Required reviewers on a GitHub environment need a paid plan for private
repositories.

**Decision.** Every push runs lint, tests and bundle validation. Prod deploys only when the
workflow is started by hand on `main`; the `prod` environment accepts only `main`. Dev is
each developer's sandbox, deployed by them.

**Alternatives.** Deploying on every merge (no deliberate step); required reviewers (not
available on the free plan for a private repository; planned once the repository is
public).

**Consequences.** Deploying prod is a recorded, deliberate act, and every deploy is a tested
commit on `main`.

## ADR-019: Environment bootstrap as a parameterized job

**Context.** Creating a catalog, granting the service principal and setting up row security
needs an admin, and was done by filling notebook widgets by hand.

**Decision.** A dev-only job, `setup_environment`, runs the setup notebooks in order. The only
required parameter is the catalog; the storage account is read from the existing external
location and the service principal is looked up by name.

**Consequences.** Setting up an environment is one command, repeatable and recorded in git.

## ADR-020: A stateful simulator with planted faults

**Context.** No real vendor feed was available, and random data has no signal for gold to
find.

**Decision.** A simulator with persistent fleet state, driver styles and planted problems,
from malformed JSON to a device nobody registered; see
[ARCHITECTURE.md](ARCHITECTURE.md#33-the-simulator). Faults are realistic; simulation flaws are
fixed. Message IDs are random UUIDs, and the fleet state is saved so a restart continues the
same fleet.

**Consequences.** Every check can be verified against a known answer. Two simulator flaws were
found by the pipeline and fixed: odometers reset on restart, and seeded message IDs repeated
across runs.

## ADR-021: Fast tests on every push, the full suite on main

**Context.** The suite runs on local Spark; 92 of 449 tests take most of the time.

**Decision.** The three slowest modules are marked `slow`. Pushes and pull requests run the
rest; `main` runs everything.

**Consequences.** A few minutes of feedback per push, with full coverage before anything can
be deployed. On Windows, local Spark starts a Python process per task, so the suite runs
far slower there than on the Linux CI runners.

## ADR-022: Late registration is fixed forward, not replayed

**Context.** C006-V001's device sent data before it was registered; 20,852 readings were
quarantined as `unknown_device`.

**Decision.** Register the device in the next snapshot and let new readings flow. The backlog
stays in quarantine, counted and recoverable.

**Alternatives.** A one-time AUTO CDC flow that reprocesses the backlog from bronze against
the corrected registry, which Lakeflow supports for SCD1 targets.

**Consequences.** The quarantine remains as evidence that the registry check works; the
replay design is documented in [LIMITATIONS.md](LIMITATIONS.md).

## ADR-023: A silent day is `no_data`, not healthy

**Context.** A vehicle-day exists when the vehicle reported or was expected to. With no
readings, no rule fired, so the status was `ok`: a vehicle that went dark counted as healthy.

**Decision.** A fourth status, checked first: no readings that day means `no_data`. The
dashboard counts vehicles not reporting, and the workshop list holds only `critical` and
`attention`.

**Consequences.** A dead device, a theft or an accident shows up as something to act on.

## ADR-024: Single-node clusters, one at a time

**Context.** A 4-vCPU family quota with no increases.

**Decision.** Every job and pipeline runs on one single-node `Standard_D4ds_v4` cluster, and
the refresh job runs its five steps strictly in sequence. Photon is off.

**Consequences.** About 25 minutes of each refresh is cluster start-up. Instance pools or
serverless compute would remove most of it; see [LIMITATIONS.md](LIMITATIONS.md).

## ADR-025: A trip is one driver's journey

**Context.** A trip ran from ignition on to ignition off, and belonged to the driver who
started it. At a shift handover the engine can keep running while drivers swap, and the
24x7 fleets never switched off at all. In prod, single "trips" ran for four days and over
5,000 km with four different drivers. Harsh events were counted for whoever was driving,
but distance only for the driver who started the trip, so second-shift drivers showed
hundreds of harsh events per 100 km and safety scores of 0.

**Decision.** A trip also ends when a different driver's fob takes over. The fob is carried
forward over readings that report none, so a missing fob is not a change; an unregistered
fob is a driver of its own, with no registered driver ID. A trip's driver is the first one to
log in during it.

**Alternatives.** Keeping ignition-based trips and computing each driver's distance from the
odometer while their fob was in: it fixes the scores, but after-hours trips and every other
per-trip measure would still belong to the wrong driver.

**Consequences.** In prod, trips went from 377 to 916, each with the right driver. The worst
rate fell from 1,062 harsh events per 100 km to 18, and every regular driver now shows a
realistic week of 1,600 to 4,100 km. Distance, scores and after-hours trips are attributed
correctly; vehicle totals do not change. The step between the
last reading of one driver and the first of the next, one reading interval, belongs to
neither trip.
