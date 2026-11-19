import re

from config.config import load_kafka_config

DEFAULT_TOPIC = "customers_review"

TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")


def _escape_jaas_value(value: str) -> str:
    """Escape a value for inclusion in a JAAS quoted string."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_jaas_config(cfg: dict | None = None) -> str:
    """
    Build the JAAS configuration required by Kafka SASL authentication.
    """
    if cfg is None:
        cfg = load_kafka_config()

    username = _escape_jaas_value(cfg["sasl.username"])
    password = _escape_jaas_value(cfg["sasl.password"])

    return (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{username}" '
        f'password="{password}";'
    )


def _validate_topic(topic: str) -> str:
    """Validate a Kafka topic name."""
    if not isinstance(topic, str) or not topic:
        raise ValueError("Kafka topic must be a non-empty string.")

    if len(topic) > 249:
        raise ValueError("Kafka topic cannot exceed 249 characters.")

    if topic in {".", ".."}:
        raise ValueError("Kafka topic cannot be '.' or '..'.")

    if not TOPIC_PATTERN.fullmatch(topic):
        raise ValueError(f"Invalid Kafka topic name: {topic!r}")

    return topic


def build_kafka_options(
    cfg: dict | None = None,
    topic: str = DEFAULT_TOPIC,
) -> dict:
    """
    Build the options required by Spark's Kafka data source.
    """
    if cfg is None:
        cfg = load_kafka_config()

    topic = _validate_topic(topic)

    return {
        "kafka.bootstrap.servers": cfg["bootstrap.servers"],
        "kafka.security.protocol": cfg["security.protocol"],
        "kafka.sasl.mechanism": cfg["sasl.mechanism"],
        "kafka.sasl.jaas.config": build_jaas_config(cfg),
        "kafka.acks": str(cfg["acks"]),
        "kafka.enable.idempotence": str(cfg["enable.idempotence"]).lower(),
        "kafka.retries": str(cfg["retries"]),
        "kafka.max.in.flight.requests.per.connection": str(
            cfg["max.in.flight.requests.per.connection"]
        ),
        "kafka.delivery.timeout.ms": str(cfg["delivery.timeout.ms"]),
        "kafka.request.timeout.ms": str(cfg["request.timeout.ms"]),
        "topic": topic,
    }
