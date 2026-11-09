-- Platform Health dashboard: one dataset per block below. For the platform team only.
--
-- Row level where the dashboard filters on it, as in Fleet Operations. Table
-- names are never catalog-qualified; the bundle's dataset_catalog points them at
-- each environment's catalog. Embedded credentials are fine: everyone who may
-- open this dashboard may see every client anyway.

-- dataset: reconciliation
SELECT
  expected_readings, stored_readings, invalid_readings, mismatched_readings,
  duplicate_readings, conflicting_duplicates, lost_readings, unexpected_readings, checked_at
FROM operations.silver_reconciliation;

-- dataset: pipeline_runs
SELECT
  started_at, final_state, duration_seconds,
  rows_to_vehicle_measurements, rows_quarantined, expectation_failures
FROM operations.pipeline_runs
ORDER BY started_at DESC;

-- dataset: quarantine_days
SELECT
  CAST(arrival_hour AS DATE) AS arrival_date,
  vendor_id,
  quarantine_stage,
  quarantine_reason,
  SUM(quarantined)           AS quarantined
FROM operations.quarantine_summary_by_hour
GROUP BY ALL;

-- dataset: vendor_days
SELECT
  utc_date,
  vendor_id,
  messages_received,
  messages_quarantined,
  readings_stored,
  readings_quarantined,
  message_quarantine_rate,
  reading_quarantine_rate
FROM vendor_data_quality;

-- dataset: vendor_totals
-- Rates over the whole history, recomputed from totals, never averaged per day.
SELECT
  vendor_id,
  SUM(messages_received)                                                  AS messages_received,
  SUM(messages_quarantined)                                               AS messages_quarantined,
  ROUND(SUM(messages_quarantined) / NULLIF(SUM(messages_received), 0), 4) AS message_quarantine_rate,
  SUM(readings_stored)                                                    AS readings_stored,
  SUM(readings_quarantined)                                               AS readings_quarantined,
  ROUND(SUM(readings_quarantined)
        / NULLIF(SUM(readings_stored) + SUM(readings_quarantined), 0), 4) AS reading_quarantine_rate
FROM vendor_data_quality
GROUP BY vendor_id
ORDER BY reading_quarantine_rate DESC;

-- dataset: billing_days
SELECT local_date, client_id, client_name, vendor_id, billable_devices, messages_received
FROM platform_billing
ORDER BY local_date DESC, client_id;