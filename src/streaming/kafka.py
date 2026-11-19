from config.config import load_config


def build_jaas_config(cfg: dict | None = None) -> str:
    if cfg is None:
        cfg = load_config()

    return (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{cfg["kafka"]["sasl.username"]}" '
        f'password="{cfg["kafka"]["sasl.password"]}";'
    )


def build_kafka_options(
    cfg: dict | None = None,
    topic: str = "customers_review",
) -> dict:
    if cfg is None:
        cfg = load_config()

    return {
        "kafka.bootstrap.servers": cfg["kafka"]["bootstrap.servers"],
        "kafka.security.protocol": cfg["kafka"]["security.protocol"],
        "kafka.sasl.mechanism": cfg["kafka"]["sasl.mechanism"],
        "kafka.sasl.jaas.config": build_jaas_config(cfg),
        "kafka.acks": cfg["kafka"]["acks"],
        "kafka.enable.idempotence": str(cfg["kafka"]["enable.idempotence"]).lower(),
        "kafka.retries": str(cfg["kafka"]["retries"]),
        "kafka.max.in.flight.requests.per.connection": str(
            cfg["kafka"]["max.in.flight.requests.per.connection"]
        ),
        "kafka.delivery.timeout.ms": str(cfg["kafka"]["delivery.timeout.ms"]),
        "kafka.request.timeout.ms": str(cfg["kafka"]["request.timeout.ms"]),
        "topic": topic,
    }
