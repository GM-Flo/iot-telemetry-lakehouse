"""Runtime configuration helpers.

The Event Hubs connection string is the single source of truth for where to
read from: the namespace, and therefore the Kafka bootstrap server, and the
topic are both parsed out of it. Nothing environment-specific is hardcoded in
notebooks or bundle config, so switching dev to prod is just a different secret.
"""

from __future__ import annotations

KAFKA_PORT = 9093
JAAS_LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"


def parse_eventhub_connection_string(conn: str) -> dict:
    """Split an Event Hubs connection string into its parts.

    Raises ValueError for the common mistake of storing only the access key
    rather than the full connection string.
    """
    if not conn or not conn.strip().startswith("Endpoint="):
        raise ValueError(
            "Not an Event Hubs connection string. Copy 'Connection string-primary key', "
            "which starts with 'Endpoint=sb://', not 'Primary key'."
        )

    parts = {}
    for segment in conn.strip().strip(";").split(";"):
        if "=" in segment:
            key, value = segment.split("=", 1)
            parts[key.strip()] = value.strip()

    for required in ("Endpoint", "SharedAccessKeyName", "SharedAccessKey"):
        if not parts.get(required):
            raise ValueError(f"Connection string is missing {required}.")

    host = parts["Endpoint"].removeprefix("sb://").rstrip("/")

    return {
        "host": host,
        "namespace": host.split(".", 1)[0],
        "bootstrap_servers": f"{host}:{KAFKA_PORT}",
        "topic": parts.get("EntityPath"),
        "key_name": parts["SharedAccessKeyName"],
    }


def kafka_source_options(
    conn: str,
    topic: str | None = None,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int | None = None,
    fail_on_data_loss: bool = False,
) -> dict:
    """Spark Kafka source options for reading Event Hubs.

    fail_on_data_loss defaults to False: if a checkpoint points at offsets that
    Event Hubs has already expired (retention is 3 days here), the stream skips
    ahead instead of failing. Right for a dev environment that sits idle between
    sessions; a production pipeline should usually fail loudly instead.
    """
    parsed = parse_eventhub_connection_string(conn)
    resolved_topic = topic or parsed["topic"]
    if not resolved_topic:
        raise ValueError("No topic given and the connection string has no EntityPath.")

    jaas = f'{JAAS_LOGIN_MODULE} required username="$ConnectionString" password="{conn}";'

    options = {
        "kafka.bootstrap.servers": parsed["bootstrap_servers"],
        "subscribe": resolved_topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
        "startingOffsets": starting_offsets,
        "failOnDataLoss": str(fail_on_data_loss).lower(),
    }
    if max_offsets_per_trigger:
        options["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)

    return options


def bronze_paths(catalog: str, table: str = "telemetry_raw") -> dict:
    """Table name and checkpoint location for a bronze stream."""
    return {
        "table": f"{catalog}.bronze.{table}",
        "checkpoint": f"/Volumes/{catalog}/bronze/checkpoints/{table}",
    }