"""Every dashboard query runs against tables with the real gold schemas.

The tables are built by the real gold functions, so a renamed column breaks this
test, not a fleet manager's dashboard. The period safety score, the one formula
written in SQL as well as Python, is checked against the Python version.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src.iot import dashboards as dash
from src.iot import operations
from src.iot import reference as ref
from src.iot.gold import products
from src.iot.gold.dimensions import UNKNOWN, UNKNOWN_KEY

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = ROOT / "dashboards"
UTC = timezone.utc
DAY1, DAY2 = date(2026, 11, 7), date(2026, 11, 8)


def datasets(file_name: str) -> dict[str, str]:
    return dash.read_datasets(DASHBOARDS / file_name)


def local(query: str) -> str:
    """Point schema-qualified names at the test's temporary views: reference.x -> reference__x."""
    return re.sub(r"\b(reference|operations)\.", r"\1__", query)


VEHICLE_DAY_SCHEMA = (
    "vehicle_key string, client_key string, date_key int, vehicle_id string, client_id string, local_date date, "
    "seconds_above_warning_temperature bigint, seconds_above_critical_temperature bigint, "
    "distinct_fault_codes bigint, distinct_high_severity_codes bigint, fault_raise_events bigint, "
    "silent_minutes bigint, readings bigint, max_engine_temperature_celsius double, trips bigint, "
    "distance_km double, driving_seconds bigint, idle_seconds bigint, fuel_used_l double, fuel_cost_usd double, "
    "idle_cost_usd double, refuel_cost_usd double, after_hours_trips bigint, after_hours_distance_km double, "
    "geofence_exits bigint"
)
DRIVER_DAY_SCHEMA = (
    "driver_key string, client_key string, date_key int, driver_id string, client_id string, local_date date, "
    "trips bigint, distance_km double, driving_seconds bigint, idle_seconds bigint, after_hours_trips bigint, "
    "harsh_brakes bigint, harsh_accelerations bigint, speeding_events bigint, speeding_seconds bigint"
)


def vehicle_day(vehicle, day, critical_seconds=0, after_hours=0):
    key = 20261107 if day == DAY1 else 20261108
    return (f"vk-{vehicle}", "ck1", key, vehicle, "C001", day, 0, critical_seconds, 0, 0, 0, 0, 100, 90.0, 1,
            100.0, 3600, 300, 10.0, 20.0, 2.0, 0.0, after_hours, 0.0, 0)


def driver_day(driver, key, day, distance, harsh):
    return (key, "ck1", 20261108, driver, "C001", day, 1, distance, 3600, 0, 0, harsh, 0, 0, 0)


@pytest.fixture(scope="module")
def catalog(spark):
    """Real-shaped gold, reference and operations tables, as the dashboards see them.

    Held as temporary views, not saved tables: nothing touches the disk, so the
    test needs no Hadoop setup on Windows. Gold tables keep their bare names,
    the way the dashboards name them; the rest are renamed by local().
    """
    views = []

    def save(df, name):
        schema, table = name.split(".")
        view = table if schema == "gold" else f"{schema}__{table}"
        df.localCheckpoint().createOrReplaceTempView(view)
        views.append(view)

    vehicle_days = spark.createDataFrame(
        [vehicle_day("V1", DAY1), vehicle_day("V1", DAY2, critical_seconds=600, after_hours=1),
         vehicle_day("V2", DAY2)],
        VEHICLE_DAY_SCHEMA,
    )
    dim_vehicle = spark.createDataFrame(
        [("vk-V1", "VIN00000000000001", "Ford", "Transit", 2021), ("vk-V2", "VIN00000000000002", "Ford", "F150", 2022)],
        "vehicle_key string, vin string, make string, model string, model_year int",
    )
    save(products.vehicle_health(vehicle_days, dim_vehicle, 300, 600), "gold.vehicle_health")
    save(products.fleet_cost_utilization(vehicle_days), "gold.fleet_cost_utilization")
    trips = spark.createDataFrame(
        [("V1", datetime(2026, 11, 8, 10, tzinfo=UTC), UNKNOWN_KEY)],
        "vehicle_id string, local_start_ts timestamp, driver_key string",
    )
    save(products.security_misuse(vehicle_days, trips), "gold.security_misuse")

    driver_days = spark.createDataFrame(
        [driver_day("D1", "dk1", DAY1, 20.0, 2), driver_day("D1", "dk1", DAY2, 300.0, 0),
         driver_day("D2", "dk2", DAY2, 100.0, 1), driver_day(UNKNOWN, UNKNOWN_KEY, DAY2, 15.0, 1)],
        DRIVER_DAY_SCHEMA,
    )
    names = spark.createDataFrame([("dk1", "Ana"), ("dk2", "Bo")], "driver_key string, driver_name string")
    save(products.driver_safety(driver_days, names, 5, 2, 10), "gold.driver_safety")

    beats = spark.createDataFrame(
        [("DEV-1", "V1", "C001", datetime(2026, 11, 8, 16, tzinfo=UTC), 10, "reporting")],
        "device_id string, vehicle_id string, client_id string, bucket_start timestamp, messages_received bigint, "
        "status string",
    )
    zones = spark.createDataFrame([("V1", "houston", True)], "vehicle_id string, region string, is_current boolean")
    regions = spark.createDataFrame([("houston", -6)], "region string, utc_offset_hours int")
    clients = spark.createDataFrame(
        [("ck1", "C001", "Acme", "tmx", True)],
        "client_key string, client_id string, client_name string, vendor_id string, is_current boolean",
    )
    save(products.platform_billing(beats, zones, regions, clients), "gold.platform_billing")

    t = datetime(2026, 11, 8, 12, tzinfo=UTC)
    message = spark.createDataFrame([('{"vendor_id": "tmx"}', t)], "raw_payload string, eventhub_enqueued_at timestamp")
    reading = spark.createDataFrame([("tmx", t)], "vendor_id string, eventhub_enqueued_at timestamp")
    save(products.vendor_data_quality(message, reading, [reading.limit(0)], [reading]), "gold.vendor_data_quality")

    save(ref.load_reference_table(spark, ROOT / "reference", "anomaly_thresholds"), "reference.anomaly_thresholds")

    quarantine = spark.createDataFrame(
        [("m", None, None, None, None, "tmx", "x", "parse", "malformed_json", None, 0, 1, t, t, t, False)],
        "message_id string, measurement_id string, reading_index int, device_id string, event_ts timestamp, "
        "vendor_id string, raw_payload string, quarantine_stage string, quarantine_reason string, "
        "quarantine_detail string, kafka_partition int, kafka_offset long, eventhub_enqueued_at timestamp, "
        "ingested_at timestamp, quarantined_at timestamp, replayed boolean",
    )
    save(operations.quarantine_summary_by_hour(quarantine), "operations.quarantine_summary_by_hour")
    keyed = spark.createDataFrame([("D", t, 1.0)], "device_id string, event_ts timestamp, speed double")
    save(operations.silver_reconciliation(message, keyed, quarantine, quarantine, keyed, keyed.limit(0),
                                          keyed.limit(0), content_columns=["speed"]),
         "operations.silver_reconciliation")
    event_log = spark.createDataFrame(
        [(("u1", None), "update_progress", t, '{"update_progress": {"state": "COMPLETED"}}')],
        "origin struct<update_id:string,flow_name:string>, event_type string, timestamp timestamp, details string",
    )
    save(operations.pipeline_runs(event_log), "operations.pipeline_runs")

    yield spark
    for view in views:
        spark.catalog.dropTempView(view)


ALL_DATASETS = [(f, name) for f in ("fleet_operations.sql", "platform_health.sql") for name in datasets(f)]


@pytest.mark.parametrize("file_name, name", ALL_DATASETS, ids=[n for _, n in ALL_DATASETS])
def test_every_dataset_runs_on_the_real_schemas(catalog, file_name, name):
    catalog.sql(local(datasets(file_name)[name])).collect()


def run(catalog, name):
    return catalog.sql(local(datasets("fleet_operations.sql")[name])).collect()


def test_each_vehicle_day_has_exactly_one_status_flag(catalog):
    for row in run(catalog, "vehicle_days"):
        assert row["is_critical"] + row["is_attention"] + row["is_ok"] == 1


def test_action_list_holds_only_vehicles_that_need_action(catalog):
    assert [(r["vehicle_id"], r["health_status"]) for r in run(catalog, "vehicles_needing_action")] == [
        ("V1", "critical")]


def test_period_score_matches_the_pipeline_formula_for_one_day(catalog):
    """D2 drove one day: the SQL period score must equal the pipeline's daily score."""
    sql = {r["driver_id"]: r for r in run(catalog, "driver_scores")}["D2"]
    python = catalog.sql("SELECT safety_score FROM driver_safety WHERE driver_id = 'D2'").first()[0]
    assert sql["safety_score"] == python == 95.0


def test_period_score_is_weighted_by_distance_not_averaged(catalog):
    """D1: 20 km with 2 harsh events (daily score 50), then 300 km with none (100).
    Averaging days would say 75; over 320 km it is 2 events per 320 km."""
    d1 = {r["driver_id"]: r for r in run(catalog, "driver_scores")}["D1"]
    assert d1["safety_score"] == 96.9  # 100 - 5 x 0.625
    assert d1["distance_km"] == 320.0
    assert d1["is_below_60"] == 0


def test_unknown_driver_gets_no_score(catalog):
    unknown = {r["driver_id"]: r for r in run(catalog, "driver_scores")}[UNKNOWN]
    assert unknown["safety_score"] is None
    assert unknown["is_below_60"] == 0


def test_client_score_is_its_drivers_weighted_by_distance(catalog):
    row = {r["client_id"]: r for r in run(catalog, "client_safety_scores")}["C001"]
    assert row["fleet_safety_score"] == round((96.9 * 320 + 95.0 * 100) / 420, 1)


def test_km_with_nobody_logged_in_comes_only_from_the_unknown_driver(catalog):
    rows = run(catalog, "driver_days")
    assert sum(r["km_without_registered_driver"] for r in rows) == 15.0


def test_misuse_kinds_become_rows(catalog):
    rows = run(catalog, "misuse_by_kind")
    kinds = {(r["vehicle_id"], r["kind"]): r["cases"] for r in rows}
    assert kinds[("V1", "After-hours trip")] == 1
    assert kinds[("V1", "No registered driver")] == 1
    assert all(r["cases"] > 0 for r in rows)


class TestDashboardFiles:
    @pytest.mark.parametrize("name", list(dash.DASHBOARD_LAYOUTS))
    def test_generated_files_are_up_to_date(self, name):
        """Run `python -m src.iot.dashboards` after changing a layout or a query."""
        expected = dash.build(dash.DASHBOARD_LAYOUTS[name], datasets(f"{name}.sql"))
        assert json.loads((DASHBOARDS / f"{name}.lvdash.json").read_text(encoding="utf-8")) == expected

    @pytest.mark.parametrize("name", list(dash.DASHBOARD_LAYOUTS))
    def test_every_widget_reads_columns_its_dataset_returns(self, catalog, name):
        queries = datasets(f"{name}.sql")
        document = dash.build(dash.DASHBOARD_LAYOUTS[name], queries)
        returned = {d: set(catalog.sql(local(queries[d])).columns) for d in {ds for _, ds, _ in dash.widget_fields(document)}}
        for widget, dataset, columns in dash.widget_fields(document):
            assert columns <= returned[dataset], (widget, columns - returned[dataset])

    def test_a_row_wider_than_the_grid_is_refused(self):
        too_wide = [dash.Page("p", "subtitle", [[dash.Text("a", width=4), dash.Text("b", width=4)]])]
        with pytest.raises(ValueError, match="wider"):
            dash.build(too_wide, {})

    def test_a_widget_on_an_unknown_dataset_is_refused(self):
        page = [dash.Page("p", "subtitle", [[dash.Counter("nope", "t", 2, 3, value=dash.Field("x", "x"))]])]
        with pytest.raises(ValueError, match="unknown dataset"):
            dash.build(page, {"other": "SELECT 1"})

    def test_a_filter_on_an_unknown_dataset_is_refused(self):
        page = [dash.Page("p", "subtitle", [[dash.Filter("multi-select", "client_id", "Client", ["nope"])]])]
        with pytest.raises(ValueError, match="unknown dataset"):
            dash.build(page, {"other": "SELECT 1"})

    @pytest.mark.parametrize("name", list(dash.DASHBOARD_LAYOUTS))
    def test_filters_query_without_aggregation_and_counters_with_trends_aggregate(self, name):
        """Per the dashboard specification: filter queries are not disaggregated, and a counter
        is disaggregated only when its dataset already returns the single value."""
        document = dash.build(dash.DASHBOARD_LAYOUTS[name], datasets(f"{name}.sql"))
        for page in document["pages"]:
            for item in page["layout"]:
                spec = item["widget"].get("spec", {})
                queries = item["widget"].get("queries", [])
                if spec.get("widgetType", "").startswith("filter-"):
                    assert all(q["query"]["disaggregated"] is False for q in queries)
                if spec.get("widgetType") == "counter":
                    aggregates = any("(" in f["expression"] for f in queries[0]["query"]["fields"])
                    assert queries[0]["query"]["disaggregated"] is (not aggregates)

    def test_queries_never_name_a_catalog(self):
        """dataset_catalog points them at each environment's catalog."""
        for name in dash.DASHBOARD_LAYOUTS:
            for query in datasets(f"{name}.sql").values():
                assert "iot_dev" not in query and "iot_prod" not in query