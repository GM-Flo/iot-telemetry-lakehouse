-- Fleet Operations dashboard: one dataset per block below.
--
-- Datasets are row level (one row per vehicle per day, per driver per day ...):
-- the dashboard's filters work by filtering rows, and its widgets do the summing.
-- Logic a widget cannot express, such as a status flag or a score, is computed here.
--
-- Table names are never catalog-qualified: the bundle's dataset_catalog points
-- them at each environment's catalog, and the schema defaults to gold.
--
-- Published WITHOUT embedded credentials: each viewer's own identity runs the
-- queries, so the row filters show every fleet manager only their own client.

-- dataset: vehicle_days
SELECT
  local_date,
  client_id,
  vehicle_id,
  make,
  model,
  health_status,
  CASE WHEN health_status = 'critical'  THEN 1 ELSE 0 END AS is_critical,
  CASE WHEN health_status = 'attention' THEN 1 ELSE 0 END AS is_attention,
  CASE WHEN health_status = 'ok'        THEN 1 ELSE 0 END AS is_ok,
  minutes_above_warning_temperature,
  minutes_above_critical_temperature,
  distinct_fault_codes,
  distinct_high_severity_codes,
  max_engine_temperature_celsius
FROM vehicle_health;

-- dataset: vehicles_needing_action
SELECT
  local_date,
  client_id,
  vehicle_id,
  vin,
  make,
  model,
  health_status,
  minutes_above_critical_temperature,
  minutes_above_warning_temperature,
  distinct_high_severity_codes,
  distinct_fault_codes,
  max_engine_temperature_celsius
FROM vehicle_health
WHERE health_status <> 'ok'
ORDER BY local_date DESC,
         CASE health_status WHEN 'critical' THEN 1 ELSE 2 END,
         minutes_above_critical_temperature DESC,
         minutes_above_warning_temperature DESC;

-- dataset: driver_days
SELECT
  local_date,
  client_id,
  driver_id,
  COALESCE(driver_name, 'No registered driver') AS driver_name,
  is_unknown_driver,
  CASE WHEN is_unknown_driver THEN distance_km ELSE 0 END AS km_without_registered_driver,
  trips,
  distance_km,
  driving_hours,
  harsh_brakes + harsh_accelerations AS harsh_events,
  harsh_brakes,
  harsh_accelerations,
  speeding_minutes,
  after_hours_trips
FROM driver_safety;

-- dataset: driver_scores
-- One row per driver over the whole history. The score is recomputed from the
-- totals, never averaged from daily scores: a 3 km day must not weigh as much as
-- a 300 km day. Weights come from the thresholds table the pipeline uses.
WITH weights AS (
  SELECT
    MAX(CASE WHEN threshold_name = 'safety_harsh_event_weight'     THEN value END) AS harsh_weight,
    MAX(CASE WHEN threshold_name = 'safety_speeding_minute_weight' THEN value END) AS speeding_weight,
    MAX(CASE WHEN threshold_name = 'safety_minimum_distance'       THEN value END) AS minimum_distance_km
  FROM reference.anomaly_thresholds
),
totals AS (
  SELECT
    driver_id,
    client_id,
    COALESCE(MAX(driver_name), 'No registered driver') AS driver_name,
    BOOL_OR(is_unknown_driver)                         AS is_unknown_driver,
    SUM(trips)                                         AS trips,
    SUM(distance_km)                                   AS distance_km,
    SUM(harsh_brakes + harsh_accelerations)            AS harsh_events,
    SUM(speeding_minutes)                              AS speeding_minutes,
    SUM(after_hours_trips)                             AS after_hours_trips
  FROM driver_safety
  GROUP BY driver_id, client_id
),
scored AS (
  SELECT
    t.*,
    ROUND(t.harsh_events / NULLIF(t.distance_km, 0) * 100, 2)     AS harsh_events_per_100_km,
    ROUND(t.speeding_minutes / NULLIF(t.distance_km, 0) * 100, 2) AS speeding_minutes_per_100_km,
    CASE WHEN NOT t.is_unknown_driver AND t.distance_km >= w.minimum_distance_km THEN ROUND(GREATEST(0,
      100 - w.harsh_weight * t.harsh_events / t.distance_km * 100
          - w.speeding_weight * t.speeding_minutes / t.distance_km * 100), 1) END AS safety_score
  FROM totals t
  CROSS JOIN weights w
)
SELECT
  *,
  CASE WHEN safety_score < 60 THEN 1 ELSE 0 END AS is_below_60
FROM scored
ORDER BY safety_score ASC NULLS LAST;

-- dataset: client_safety_scores
-- Each client's score: its drivers' scores weighted by distance, over the whole
-- history. A fleet manager sees their own client only, so the counter is exact.
WITH weights AS (
  SELECT
    MAX(CASE WHEN threshold_name = 'safety_harsh_event_weight'     THEN value END) AS harsh_weight,
    MAX(CASE WHEN threshold_name = 'safety_speeding_minute_weight' THEN value END) AS speeding_weight,
    MAX(CASE WHEN threshold_name = 'safety_minimum_distance'       THEN value END) AS minimum_distance_km
  FROM reference.anomaly_thresholds
),
totals AS (
  SELECT
    driver_id,
    client_id,
    SUM(distance_km)                        AS distance_km,
    SUM(harsh_brakes + harsh_accelerations) AS harsh_events,
    SUM(speeding_minutes)                   AS speeding_minutes
  FROM driver_safety
  WHERE NOT is_unknown_driver
  GROUP BY driver_id, client_id
),
scored AS (
  SELECT
    t.client_id,
    t.distance_km,
    GREATEST(0,
      100 - w.harsh_weight * t.harsh_events / t.distance_km * 100
          - w.speeding_weight * t.speeding_minutes / t.distance_km * 100) AS safety_score
  FROM totals t
  CROSS JOIN weights w
  WHERE t.distance_km >= w.minimum_distance_km
)
SELECT
  client_id,
  ROUND(SUM(safety_score * distance_km) / NULLIF(SUM(distance_km), 0), 1) AS fleet_safety_score
FROM scored
GROUP BY client_id;

-- dataset: client_cost_days
SELECT
  local_date,
  client_id,
  fuel_cost_usd,
  ROUND(fuel_cost_usd - idle_cost_usd, 2) AS driving_fuel_cost_usd,
  idle_cost_usd,
  distance_km,
  driving_hours,
  idle_hours,
  vehicles_active,
  vehicles_reporting,
  fuel_cost_per_km_usd,
  idle_cost_share
FROM fleet_cost_utilization
ORDER BY local_date DESC, client_id;

-- dataset: misuse_days
SELECT
  local_date,
  client_id,
  vehicle_id,
  after_hours_trips,
  after_hours_distance_km,
  geofence_exits,
  trips_without_registered_driver
FROM security_misuse
ORDER BY local_date DESC, after_hours_trips + geofence_exits + trips_without_registered_driver DESC;

-- dataset: misuse_by_kind
-- The three kinds of case as rows, so one chart can colour them.
SELECT local_date, client_id, vehicle_id, kind, cases
FROM security_misuse
LATERAL VIEW STACK(3,
  'After-hours trip',     after_hours_trips,
  'Geofence exit',        geofence_exits,
  'No registered driver', trips_without_registered_driver
) k AS kind, cases
WHERE cases > 0;