# Naming and Tagging Conventions

One scheme, applied everywhere. Names are written for a reader who has never seen this
project: descriptive snake_case, full words, no abbreviations a newcomer would have to decode.
Length is not a concern; clarity is.

## Environments

| Code | Meaning |
|---|---|
| `dev` | Development. Small fleet, sporadic data, deployed by an individual from a branch. |
| `prod` | Production. Full fleet, continuous data, deployed only by CI/CD from `main`. |

## Azure resources

Azure enforces length limits and character rules, so these follow Microsoft's abbreviation
standard: `<type>-<project>-<scope>[-<suffix>]`.

| Resource | Name | Notes |
|---|---|---|
| Resource group | `rg-iot-dev-lakehouse` | Shared platform. Named `dev` for historical reasons; renaming would recreate everything. |
| Event Hubs namespace | `evhns-iot-dev-<random>` | Shared by both environments. |
| Event Hub | `vehicle-telemetry-<env>` | One per environment. |
| Hub access policy | `producer-send`, `databricks-listen` | Least privilege: one send-only and one listen-only per hub. |
| Storage account | `iotdevlake<random>` | Globally unique, lowercase, no dashes (Azure rule). |
| Databricks workspace | `dbw-iot-dev` | Shared by both environments. |
| Access connector | `dbac-iot-dev` | Unity Catalog identity for the storage account. |

## Databricks catalogs and schemas

| Object | Name | Purpose |
|---|---|---|
| Catalog | `iot_<env>` | One per environment: `iot_dev`, `iot_prod`. |
| Schema | `bronze` | Raw messages exactly as received. |
| Schema | `silver` | Validated, normalized, one row per reading. Plus quarantine tables. |
| Schema | `gold` | Business-ready facts, dimensions and aggregates. |
| Schema | `reference` | Lookup and master data maintained outside the stream. |
| Schema | `operations` | Pipeline run logs and audit tables. |

## Tables

The full table catalogue is in [ARCHITECTURE.md](ARCHITECTURE.md). Naming rules:

| Rule | Example |
|---|---|
| Plural snake_case, describing what the rows are | `vehicle_trips`, `driver_behavior_events` |
| Bronze tables start with `raw_` | `raw_telemetry_messages` |
| Things that happen end in `_events` | `geofence_events` |
| Quarantine tables start with `quarantine_` | `quarantine_registry_mismatches` |
| Gold dimensions and facts start with `dim_` and `fact_` | `dim_devices`, `fact_vehicle_trips` |
| Gold data products are named for what they answer, with their grain | `driver_safety_daily` |
| US spelling | `behavior`, not `behaviour` |

## Other Databricks objects

| Object | Pattern | Example |
|---|---|---|
| Volume | purpose | `checkpoints` |
| Checkpoint path | `/Volumes/<catalog>/<schema>/checkpoints/<table>` | one directory per stream |
| Job | `<layer>_<what it does>` | `bronze_ingest_vendor_messages`, `silver_validate_and_normalize` |
| Notebook | `<NN>_<layer>_<what it does>.py` | `01_bronze_ingest_vendor_messages.py` |
| Pipeline | `<layers>_<what it does>` | `silver_gold_telemetry` |
| Secret scope | `iot` | |
| Secret key | `eventhub-<access>-conn-<env>` | `eventhub-listen-conn-dev` |

Snake case for every Databricks object. Lowercase with dashes for every Azure resource.

## Columns

| Kind | Convention | Example |
|---|---|---|
| Event time | ends in `_ts` | `event_ts` |
| System time | ends in `_at` | `received_at`, `quarantined_at` |
| Units | included in the name | `engine_temperature_celsius`, `speed_kph`, `distance_km` |
| Booleans | `is_` or `has_` | `is_late_arrival`, `has_fault_code` |
| Surrogate keys | ends in `_key` | `vehicle_key` |
| Hashes | ends in `_hash` | `record_hash`, `attribute_hash` |

## Tags

Applied to every job (and, through it, every job cluster, so they appear on the Azure bill).

| Tag | Values | Set by |
|---|---|---|
| `project` | `iot-telemetry-lakehouse` | job definition |
| `layer` | `bronze`, `silver`, `gold`, `operations` | job definition |
| `environment` | `dev`, `prod` | bundle target preset |
| `managed_by` | `databricks-bundle`, `terraform` | job definition / Terraform |

## Data identifiers

| Thing | Convention |
|---|---|
| Vendor ID | 3-letter lowercase code: `tmx`, `fpl`, `gtk`, `ats` |
| Client ID | `C` + 3 digits: `C001` |
| Vehicle ID | `<client_id>-V<NNN>`: `C003-V002` |
| Device ID | `<VENDOR>-<6 hex>`: `FPL-3A9C1E` |
| Contract name | `<vendor_id>.telemetry.v<N>` |
| Timestamps | UTC everywhere. |
| Units | Silver and gold are metric: km/h, °C, kPa, km, litres. Vendor units are converted in silver. |

## Never hardcoded

Catalog names, Event Hub names, storage account names, workspace URLs, secrets and paths come
from bundle variables, widgets, secrets or Terraform outputs. The repository is public.