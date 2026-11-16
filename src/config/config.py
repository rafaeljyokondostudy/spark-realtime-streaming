import os
from pathlib import Path


def _load_dotenv_file() -> None:
    candidates = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path(__file__).resolve().parents[2] / ".env",
        Path.cwd() / ".env",
    ]

    for env_file in candidates:
        if not env_file.exists():
            continue

        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        return


_load_dotenv_file()


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config() -> dict:
    _load_dotenv_file()

    config = {
        "openai": {
            "api_key": _require_env("OPENAI_API_KEY")
        },
        "kafka": {
            "sasl.username": _require_env("KAFKA_SASL_USERNAME"),
            "sasl.password": _require_env("KAFKA_SASL_PASSWORD"),
            "bootstrap.servers": _require_env("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "50000")),
            "acks": os.getenv("KAFKA_ACKS", "all"),
            "enable.idempotence": os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true",
            "retries": int(os.getenv("KAFKA_RETRIES", "5")),
            "max.in.flight.requests.per.connection": int(os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5")),
            "delivery.timeout.ms": int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "120000")),
            "request.timeout.ms": int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000")),
        },
    }
    return config


config = {
    "openai": {"api_key": os.getenv("OPENAI_API_KEY", "")},
    "kafka": {
        "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "50000")),
        "acks": os.getenv("KAFKA_ACKS", "all"),
        "enable.idempotence": os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true",
        "retries": int(os.getenv("KAFKA_RETRIES", "5")),
        "max.in.flight.requests.per.connection": int(os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5")),
        "delivery.timeout.ms": int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "120000")),
        "request.timeout.ms": int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000")),
    },
}