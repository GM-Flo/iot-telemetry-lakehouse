"""Tests for connection-string parsing and Kafka options. Pure Python, no Spark."""

from __future__ import annotations

import pytest

from src.iot import config as C

CONN = (
    "Endpoint=sb://evhns-iot-dev-abc123.servicebus.windows.net/;"
    "SharedAccessKeyName=databricks-listen;"
    "SharedAccessKey=c2VjcmV0a2V5PQ==;"
    "EntityPath=vehicle-telemetry"
)


class TestParseConnectionString:
    def test_extracts_bootstrap_and_topic(self):
        parsed = C.parse_eventhub_connection_string(CONN)

        assert parsed["bootstrap_servers"] == "evhns-iot-dev-abc123.servicebus.windows.net:9093"
        assert parsed["namespace"] == "evhns-iot-dev-abc123"
        assert parsed["topic"] == "vehicle-telemetry"
        assert parsed["key_name"] == "databricks-listen"

    def test_key_containing_equals_signs_survives(self):
        """Base64 keys end in '=', which a naive split would break."""
        parsed = C.parse_eventhub_connection_string(CONN)
        assert parsed["host"].endswith(".servicebus.windows.net")

    def test_bare_access_key_is_rejected_with_a_helpful_message(self):
        """The exact mistake made during setup: pasting 'Primary key'."""
        with pytest.raises(ValueError, match="Connection string-primary key"):
            C.parse_eventhub_connection_string("f85ctRnzZW65+ua9HUP0ipARysUKo8MQh+AEhGzaPxk=")

    def test_empty_string_is_rejected(self):
        with pytest.raises(ValueError):
            C.parse_eventhub_connection_string("")

    def test_missing_key_is_rejected(self):
        broken = "Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=listen"
        with pytest.raises(ValueError, match="SharedAccessKey"):
            C.parse_eventhub_connection_string(broken)

    def test_namespace_level_string_has_no_topic(self):
        namespace_conn = CONN.rsplit(";EntityPath", 1)[0]
        assert C.parse_eventhub_connection_string(namespace_conn)["topic"] is None


class TestKafkaOptions:
    def test_core_options_present(self):
        opts = C.kafka_source_options(CONN)

        assert opts["subscribe"] == "vehicle-telemetry"
        assert opts["kafka.security.protocol"] == "SASL_SSL"
        assert opts["kafka.sasl.mechanism"] == "PLAIN"
        assert opts["startingOffsets"] == "earliest"

    def test_jaas_uses_shaded_module_and_full_connection_string(self):
        jaas = C.kafka_source_options(CONN)["kafka.sasl.jaas.config"]

        assert jaas.startswith("kafkashaded.")
        assert 'username="$ConnectionString"' in jaas
        assert CONN in jaas

    def test_explicit_topic_overrides_entity_path(self):
        assert C.kafka_source_options(CONN, topic="other")["subscribe"] == "other"

    def test_topic_required_when_no_entity_path(self):
        namespace_conn = CONN.rsplit(";EntityPath", 1)[0]
        with pytest.raises(ValueError, match="topic"):
            C.kafka_source_options(namespace_conn)

    def test_rate_limit_only_set_when_requested(self):
        assert "maxOffsetsPerTrigger" not in C.kafka_source_options(CONN)
        assert C.kafka_source_options(CONN, max_offsets_per_trigger=5000)["maxOffsetsPerTrigger"] == "5000"

    def test_fail_on_data_loss_serialized_as_spark_expects(self):
        assert C.kafka_source_options(CONN)["failOnDataLoss"] == "false"
        assert C.kafka_source_options(CONN, fail_on_data_loss=True)["failOnDataLoss"] == "true"


class TestBronzePaths:
    def test_paths_follow_catalog(self):
        dev = C.bronze_paths("iot_dev")
        prod = C.bronze_paths("iot_prod")

        assert dev["table"] == "iot_dev.bronze.raw_telemetry_messages"
        assert dev["checkpoint"] == "/Volumes/iot_dev/bronze/checkpoints/raw_telemetry_messages"
        assert prod["table"].startswith("iot_prod.")
        assert dev["checkpoint"] != prod["checkpoint"]
