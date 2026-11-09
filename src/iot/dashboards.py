"""Dashboards as code: layouts described here, datasets read from dashboards/*.sql.

Run `python -m src.iot.dashboards` after changing a layout or a query. It writes
dashboards/<name>.lvdash.json, the file `databricks bundle deploy` publishes.

Every page reads top to bottom the way a manager works: filters, then counters
with a trend line for "how are we doing", a table for "what do I do now", and
charts for "where and when". Widget formats follow the Lakeview dashboard
specification: counter and table version 2, bar and line version 3, filters
version 2 with one query per filtered dataset.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

DASHBOARDS = Path(__file__).resolve().parents[2] / "dashboards"
GRID_WIDTH = 6

STATUS_COLORS = {"critical": "#E5484D", "attention": "#F5A524", "ok": "#30A46C"}
SERIES_COLORS = {"After-hours trip": "#8E4EC6", "Geofence exit": "#F5A524", "No registered driver": "#E5484D"}

USD = {"type": "number-currency", "currencyCode": "USD", "decimalPlaces": {"type": "max", "places": 2}}
PERCENT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
WHOLE = {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}
ONE_DECIMAL = {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}


def read_datasets(sql_file: Path) -> dict[str, str]:
    """name -> query, from the '-- dataset: name' blocks of a dashboard's SQL file."""
    text = sql_file.read_text(encoding="utf-8")
    blocks = re.split(r"^-- dataset: (\w+)\s*$", text, flags=re.MULTILINE)[1:]
    return {name: query.strip().rstrip(";").strip() for name, query in zip(blocks[::2], blocks[1::2], strict=True)}


@dataclass
class Field:
    column: str
    label: str
    aggregate: str | None = None  # SUM, AVG, MAX, COUNT ... or None for the raw column
    format: dict | None = None
    daily: bool = False  # truncated to the day, for a counter's trend line

    @property
    def name(self) -> str:
        if self.daily:
            return f"daily({self.column})"
        return f"{self.aggregate.lower()}({self.column})" if self.aggregate else self.column

    @property
    def expression(self) -> str:
        if self.daily:
            return f'DATE_TRUNC("DAY", `{self.column}`)'
        return f"{self.aggregate}(`{self.column}`)" if self.aggregate else f"`{self.column}`"


def daily(column: str) -> Field:
    """A day-level time field, for a counter's trend line."""
    return Field(column, "Day", daily=True)


@dataclass
class Widget:
    dataset: str
    title: str
    width: int
    height: int


@dataclass
class Counter(Widget):
    value: Field = None
    trend: Field | None = None  # a daily() field: draws a trend line, shows the latest day


@dataclass
class Table(Widget):
    columns: list[Field] = field(default_factory=list)


@dataclass
class Chart(Widget):
    kind: str = "bar"  # bar or line
    x: Field = None
    y: list[Field] = field(default_factory=list)
    color: Field | None = None
    x_scale: str = "temporal"
    y_scale: str = "quantitative"
    color_mappings: dict[str, str] | None = None
    grouped: bool = False


@dataclass
class Filter:
    """A filter on `column`, applied to every listed dataset that has it."""
    kind: str  # date-range, multi-select
    column: str
    title: str
    datasets: list[str]
    width: int = 2
    height: int = 2


@dataclass
class Text:
    markdown: str
    width: int = GRID_WIDTH
    height: int = 1


@dataclass
class Page:
    title: str
    subtitle: str
    rows: list[list]


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _query(dataset: str, fields: list[Field], disaggregated: bool) -> list[dict]:
    return [{
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [{"name": f.name, "expression": f.expression} for f in fields],
            "disaggregated": disaggregated,
        },
    }]


def _frame(title: str) -> dict:
    return {"showTitle": True, "title": title}


def _encoding(f: Field, **extra) -> dict:
    out = {"fieldName": f.name, "displayName": f.label, **extra}
    if f.format:
        out["format"] = f.format
    return out


def _counter(w: Counter) -> dict:
    fields = [w.value] + ([w.trend] if w.trend else [])
    encodings = {"value": _encoding(w.value)}
    if w.trend:
        encodings["period"] = {"fieldName": w.trend.name}
    return {
        # Rows are summed by the widget unless the dataset already returns the one value.
        "queries": _query(w.dataset, fields, disaggregated=w.trend is None and w.value.aggregate is None),
        "spec": {"version": 2, "widgetType": "counter", "encodings": encodings, "frame": _frame(w.title)},
    }


def _table(w: Table) -> dict:
    return {
        "queries": _query(w.dataset, w.columns, disaggregated=True),
        "spec": {
            "version": 2,
            "widgetType": "table",
            "encodings": {"columns": [_encoding(c) for c in w.columns]},
            "frame": _frame(w.title),
        },
    }


def _chart(w: Chart) -> dict:
    fields = [w.x, *w.y] + ([w.color] if w.color else [])
    x = _encoding(w.x, scale={"type": w.x_scale})
    if len(w.y) == 1:
        y = _encoding(w.y[0], scale={"type": w.y_scale})
    else:
        y = {"scale": {"type": w.y_scale}, "fields": [_encoding(f) for f in w.y]}
    encodings = {"x": x, "y": y}
    if w.color:
        scale = {"type": "categorical"}
        if w.color_mappings:
            scale["mappings"] = [{"value": v, "color": c} for v, c in w.color_mappings.items()]
        encodings["color"] = _encoding(w.color, scale=scale)
    spec = {"version": 3, "widgetType": w.kind, "encodings": encodings, "frame": _frame(w.title)}
    if w.grouped:
        spec["mark"] = {"layout": "group"}
    aggregated = any(f.aggregate for f in fields)
    return {"queries": _query(w.dataset, fields, disaggregated=not aggregated), "spec": spec}


def _filter(w: Filter) -> dict:
    widget_type = {"date-range": "filter-date-range-picker", "multi-select": "filter-multi-select"}[w.kind]
    queries, encodings = [], []
    for dataset in w.datasets:
        name = f"{dataset}_{w.column}"
        queries.append({
            "name": name,
            "query": {
                "datasetName": dataset,
                "fields": [{"name": w.column, "expression": f"`{w.column}`"}],
                "disaggregated": False,
            },
        })
        encodings.append({"fieldName": w.column, "displayName": w.title, "queryName": name})
    return {
        "queries": queries,
        "spec": {"version": 2, "widgetType": widget_type, "encodings": {"fields": encodings}, "frame": _frame(w.title)},
    }


def _widget_json(widget, name: str) -> dict:
    if isinstance(widget, Text):
        return {"name": name, "multilineTextboxSpec": {"lines": [widget.markdown]}}
    build_one = {Counter: _counter, Table: _table, Chart: _chart, Filter: _filter}[type(widget)]
    return {"name": name, **build_one(widget)}


def _datasets_of(widget) -> list[str]:
    if isinstance(widget, Filter):
        return widget.datasets
    if isinstance(widget, Widget):
        return [widget.dataset]
    return []


def build(pages: list[Page], datasets: dict[str, str]) -> dict:
    """The Lakeview dashboard document: datasets, then pages laid out row by row."""
    used = set()
    out_pages = []
    for page in pages:
        rows = [[Text(f"# {page.title}")], [Text(page.subtitle)], *page.rows]
        layout, y = [], 0
        for r, row in enumerate(rows):
            if sum(w.width for w in row) > GRID_WIDTH:
                raise ValueError(f"page {page.title!r} row {r} is wider than {GRID_WIDTH} columns")
            x = 0
            for c, widget in enumerate(row):
                for dataset in _datasets_of(widget):
                    if dataset not in datasets:
                        raise ValueError(f"page {page.title!r}: unknown dataset {dataset!r}")
                    used.add(dataset)
                layout.append({
                    "widget": _widget_json(widget, f"{_slug(page.title)}_{r}_{c}"),
                    "position": {"x": x, "y": y, "width": widget.width, "height": widget.height},
                })
                x += widget.width
            y += max(w.height for w in row)
        out_pages.append({
            "name": _slug(page.title), "displayName": page.title, "pageType": "PAGE_TYPE_CANVAS", "layout": layout,
        })

    return {
        "datasets": [
            {"name": name, "displayName": name, "queryLines": [line + "\n" for line in datasets[name].splitlines()]}
            for name in datasets
            if name in used
        ],
        "pages": out_pages,
    }


def widget_fields(document: dict) -> list[tuple[str, str, set[str]]]:
    """(widget, dataset, columns it reads) for every query of every widget: what the tests check."""
    out = []
    for page in document["pages"]:
        for item in page["layout"]:
            widget = item["widget"]
            for query in widget.get("queries", []):
                columns = {c for f in query["query"]["fields"] for c in re.findall(r"`([^`]+)`", f["expression"])}
                out.append((widget["name"], query["query"]["datasetName"], columns))
    return out


# ------------------------------------------------------------------------------- layouts


def _trend_counter(dataset: str, column: str, title: str, fmt: dict = WHOLE, day: str = "local_date") -> Counter:
    return Counter(dataset, title, width=2, height=3, value=Field(column, title, "SUM", fmt), trend=daily(day))


VEHICLE_DATASETS = ["vehicle_days", "vehicles_needing_action"]
DRIVER_DATASETS = ["driver_days", "driver_scores", "client_safety_scores"]
COST_DATASETS = ["client_cost_days"]
MISUSE_DATASETS = ["misuse_days", "misuse_by_kind"]

FLEET_OPERATIONS = [
    Page(
        "Vehicle health",
        "Which vehicles need the workshop, and is the fleet getting better or worse? **Critical**: five minutes or "
        "more above the critical engine temperature, or a high-severity fault code. **Attention**: ten minutes or "
        "more above the warning temperature, or any fault code. Counters show the latest day; the line under each "
        "is its trend.",
        [
            [Filter("date-range", "local_date", "Days", VEHICLE_DATASETS),
             Filter("multi-select", "client_id", "Client", VEHICLE_DATASETS),
             Filter("multi-select", "vehicle_id", "Vehicle", VEHICLE_DATASETS)],
            [_trend_counter("vehicle_days", "is_critical", "Critical vehicles"),
             _trend_counter("vehicle_days", "is_attention", "Vehicles needing attention"),
             _trend_counter("vehicle_days", "is_ok", "Healthy vehicles")],
            [Table("vehicles_needing_action", "Send to the workshop: newest day first, worst first", 6, 7, columns=[
                Field("local_date", "Day"), Field("vehicle_id", "Vehicle"), Field("vin", "VIN"),
                Field("make", "Make"), Field("model", "Model"), Field("health_status", "Status"),
                Field("minutes_above_critical_temperature", "Minutes above critical", format=ONE_DECIMAL),
                Field("minutes_above_warning_temperature", "Minutes above warning", format=ONE_DECIMAL),
                Field("distinct_high_severity_codes", "High-severity codes", format=WHOLE),
                Field("distinct_fault_codes", "Fault codes", format=WHOLE),
                Field("max_engine_temperature_celsius", "Hottest engine °C", format=ONE_DECIMAL),
            ])],
            [Chart("vehicle_days", "Vehicles per day by health status", 3, 6, kind="bar",
                   x=Field("local_date", "Day"), y=[Field("vehicle_id", "Vehicles", "COUNT")],
                   color=Field("health_status", "Status"), color_mappings=STATUS_COLORS),
             Chart("vehicle_days", "Hottest engine per day (pick vehicles in the filter above)", 3, 6, kind="line",
                   x=Field("local_date", "Day"), y=[Field("max_engine_temperature_celsius", "Hottest engine °C", "MAX")],
                   color=Field("vehicle_id", "Vehicle"))],
        ],
    ),
    Page(
        "Driver safety",
        "Harsh braking, harsh acceleration and sustained speeding, measured **per 100 km** so that long-distance "
        "drivers are not penalised for driving more. Scores run from 0 to 100 over each driver's whole history; "
        "below 10 km there is too little driving to judge.",
        [
            [Filter("date-range", "local_date", "Days (daily charts)", ["driver_days"]),
             Filter("multi-select", "client_id", "Client", DRIVER_DATASETS),
             Filter("multi-select", "driver_name", "Driver", ["driver_days", "driver_scores"])],
            [Counter("client_safety_scores", "Fleet safety score (0–100)", 2, 3,
                     value=Field("fleet_safety_score", "Score", "AVG", ONE_DECIMAL)),
             Counter("driver_scores", "Drivers scoring below 60", 2, 3,
                     value=Field("is_below_60", "Drivers", "SUM", WHOLE)),
             _trend_counter("driver_days", "km_without_registered_driver", "km driven with nobody logged in",
                            ONE_DECIMAL)],
            [Table("driver_scores", "Drivers, lowest score first", 6, 7, columns=[
                Field("driver_name", "Driver"), Field("driver_id", "Driver id"), Field("client_id", "Client"),
                Field("safety_score", "Safety score", format=ONE_DECIMAL),
                Field("distance_km", "km driven", format=ONE_DECIMAL), Field("trips", "Trips", format=WHOLE),
                Field("harsh_events_per_100_km", "Harsh events per 100 km", format=ONE_DECIMAL),
                Field("speeding_minutes_per_100_km", "Speeding minutes per 100 km", format=ONE_DECIMAL),
                Field("after_hours_trips", "After-hours trips", format=WHOLE),
            ])],
            [Chart("driver_days", "Harsh events per day", 3, 6, kind="bar",
                   x=Field("local_date", "Day"),
                   y=[Field("harsh_brakes", "Harsh brakes", "SUM"), Field("harsh_accelerations", "Harsh accelerations", "SUM")]),
             Chart("driver_days", "Distance driven per day (km)", 3, 6, kind="bar",
                   x=Field("local_date", "Day"), y=[Field("distance_km", "km", "SUM", ONE_DECIMAL)])],
        ],
    ),
    Page(
        "Cost & utilization",
        "What the fleet's fuel costs, how much of it is burned standing still with the engine running, and how "
        "much the fleet is actually driven. Fuel is priced at each region's price on the day.",
        [
            [Filter("date-range", "local_date", "Days", COST_DATASETS),
             Filter("multi-select", "client_id", "Client", COST_DATASETS)],
            [_trend_counter("client_cost_days", "fuel_cost_usd", "Fuel cost", USD),
             _trend_counter("client_cost_days", "idle_cost_usd", "Fuel burned idling", USD),
             _trend_counter("client_cost_days", "distance_km", "km driven", ONE_DECIMAL)],
            [Chart("client_cost_days", "Fuel cost per day: driving vs idling", 3, 6, kind="bar",
                   x=Field("local_date", "Day"),
                   y=[Field("driving_fuel_cost_usd", "Driving", "SUM", USD), Field("idle_cost_usd", "Idling", "SUM", USD)]),
             Chart("client_cost_days", "Hours per day: driving vs idling", 3, 6, kind="bar",
                   x=Field("local_date", "Day"),
                   y=[Field("driving_hours", "Driving hours", "SUM"), Field("idle_hours", "Idle hours", "SUM")],
                   grouped=True)],
            [Table("client_cost_days", "Cost and use per client per day", 6, 6, columns=[
                Field("local_date", "Day"), Field("client_id", "Client"),
                Field("fuel_cost_usd", "Fuel cost", format=USD),
                Field("fuel_cost_per_km_usd", "Cost per km", format=USD),
                Field("idle_cost_share", "Share burned idling", format=PERCENT),
                Field("distance_km", "km driven", format=ONE_DECIMAL),
                Field("vehicles_active", "Vehicles driven", format=WHOLE),
                Field("vehicles_reporting", "Vehicles reporting", format=WHOLE),
            ])],
        ],
    ),
    Page(
        "Misuse",
        "Vehicle days worth a security review: driving outside the client's operating hours, leaving the region's "
        "geofence, or trips with nobody logged in on a registered fob. A day with nothing to review is not listed.",
        [
            [Filter("date-range", "local_date", "Days", MISUSE_DATASETS),
             Filter("multi-select", "client_id", "Client", MISUSE_DATASETS),
             Filter("multi-select", "vehicle_id", "Vehicle", MISUSE_DATASETS)],
            [_trend_counter("misuse_days", "after_hours_trips", "After-hours trips"),
             _trend_counter("misuse_days", "geofence_exits", "Geofence exits"),
             _trend_counter("misuse_days", "trips_without_registered_driver", "Trips with nobody logged in")],
            [Chart("misuse_by_kind", "Cases per day, by kind", 6, 5, kind="bar",
                   x=Field("local_date", "Day"), y=[Field("cases", "Cases", "SUM")],
                   color=Field("kind", "Kind"), color_mappings=SERIES_COLORS)],
            [Table("misuse_days", "Vehicle days to review, newest first", 6, 6, columns=[
                Field("local_date", "Day"), Field("client_id", "Client"), Field("vehicle_id", "Vehicle"),
                Field("after_hours_trips", "After-hours trips", format=WHOLE),
                Field("after_hours_distance_km", "After-hours km", format=ONE_DECIMAL),
                Field("geofence_exits", "Geofence exits", format=WHOLE),
                Field("trips_without_registered_driver", "Trips with nobody logged in", format=WHOLE),
            ])],
        ],
    ),
]


PLATFORM_HEALTH = [
    Page(
        "Pipeline",
        "Every reading must end up stored or quarantined: **lost** and **unexpected** must be 0. Conflicting "
        "duplicates are two different readings claiming the same device and instant, usually a vendor sending "
        "timestamps rounded to the second.",
        [
            [Counter("reconciliation", "Lost readings", 2, 3, value=Field("lost_readings", "Lost", format=WHOLE)),
             Counter("reconciliation", "Unexpected readings", 2, 3,
                     value=Field("unexpected_readings", "Unexpected", format=WHOLE)),
             Counter("reconciliation", "Conflicting duplicates", 2, 3,
                     value=Field("conflicting_duplicates", "Conflicting", format=WHOLE))],
            [Counter("reconciliation", "Readings stored", 2, 3, value=Field("stored_readings", "Stored", format=WHOLE)),
             Counter("reconciliation", "Readings with impossible values", 2, 3,
                     value=Field("invalid_readings", "Invalid", format=WHOLE)),
             Counter("reconciliation", "Readings the registry rejects", 2, 3,
                     value=Field("mismatched_readings", "Mismatched", format=WHOLE))],
            [Filter("date-range", "arrival_date", "Days", ["quarantine_days"]),
             Filter("multi-select", "vendor_id", "Vendor", ["quarantine_days"]),
             Filter("multi-select", "quarantine_stage", "Stage", ["quarantine_days"])],
            [Chart("quarantine_days", "Quarantined per day, by reason", 6, 6, kind="bar",
                   x=Field("arrival_date", "Day"), y=[Field("quarantined", "Quarantined", "SUM")],
                   color=Field("quarantine_reason", "Reason"))],
            [Table("pipeline_runs", "Silver pipeline runs, newest first", 6, 6, columns=[
                Field("started_at", "Started"), Field("final_state", "State"),
                Field("duration_seconds", "Seconds", format=WHOLE),
                Field("rows_to_vehicle_measurements", "Readings stored", format=WHOLE),
                Field("rows_quarantined", "Rows quarantined", format=WHOLE),
                Field("expectation_failures", "Expectation failures", format=WHOLE),
            ])],
        ],
    ),
    Page(
        "Vendors & billing",
        "Which vendor sends data we cannot use, and how many devices each client is billed for: a device is "
        "billable on a day it sent at least one message.",
        [
            [Filter("date-range", "utc_date", "Days (vendors, UTC)", ["vendor_days"]),
             Filter("date-range", "local_date", "Days (billing)", ["billing_days"]),
             Filter("multi-select", "vendor_id", "Vendor", ["vendor_days", "vendor_totals", "billing_days"])],
            [Table("vendor_totals", "Data quality per vendor, worst first", 6, 4, columns=[
                Field("vendor_id", "Vendor"),
                Field("messages_received", "Messages received", format=WHOLE),
                Field("message_quarantine_rate", "Messages quarantined", format=PERCENT),
                Field("readings_stored", "Readings stored", format=WHOLE),
                Field("reading_quarantine_rate", "Readings quarantined", format=PERCENT),
            ])],
            [Chart("vendor_days", "Messages per day, by vendor", 3, 6, kind="bar",
                   x=Field("utc_date", "Day"), y=[Field("messages_received", "Messages", "SUM")],
                   color=Field("vendor_id", "Vendor")),
             Chart("billing_days", "Billable devices per day, by client", 3, 6, kind="bar",
                   x=Field("local_date", "Day"), y=[Field("billable_devices", "Devices", "SUM")],
                   color=Field("client_id", "Client"))],
            [Table("billing_days", "Billing per client per day", 6, 5, columns=[
                Field("local_date", "Day"), Field("client_id", "Client"), Field("client_name", "Name"),
                Field("vendor_id", "Vendor"), Field("billable_devices", "Billable devices", format=WHOLE),
                Field("messages_received", "Messages", format=WHOLE),
            ])],
        ],
    ),
]

DASHBOARD_LAYOUTS = {
    "fleet_operations": FLEET_OPERATIONS,
    "platform_health": PLATFORM_HEALTH,
}


def write_all() -> list[Path]:
    written = []
    for name, pages in DASHBOARD_LAYOUTS.items():
        document = build(pages, read_datasets(DASHBOARDS / f"{name}.sql"))
        path = DASHBOARDS / f"{name}.lvdash.json"
        path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    for path in write_all():
        print(f"wrote {path}")