import os
from pathlib import Path

from dotenv import load_dotenv


def load_environment() -> None:
    """Load environment variables from the project root."""
    env_file = Path(__file__).resolve().parents[2] / ".env"

    load_dotenv(env_file, override=False)


def require_env(name: str) -> str:
    """Retrieve a required environment variable."""
    value = os.getenv(name, "").strip()

    if not value:
        raise ValueError(
            f"Missing required environment variable: {name}"
        )

    return value


def get_bool(name: str, default: bool) -> bool:
    """Convert an environment variable into a boolean."""
    value = os.getenv(name, str(default)).lower()

    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")

    return value == "true"


def load_config() -> dict:
    """Load and validate application configuration."""

    return {
        "openai": {
            "api_key": require_env("OPENAI_API_KEY"),
        },
        "kafka": {
            "sasl.username": require_env("KAFKA_SASL_USERNAME"),
            "sasl.password": require_env("KAFKA_SASL_PASSWORD"),
            "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": os.getenv(
                "KAFKA_SECURITY_PROTOCOL", "SASL_SSL"
            ),
            "sasl.mechanism": os.getenv(
                "KAFKA_SASL_MECHANISM", "PLAIN"
            ),
            "session.timeout.ms": int(
                os.getenv("KAFKA_SESSION_TIMEOUT_MS", "50000")
            ),
            "acks": os.getenv("KAFKA_ACKS", "all"),
            "enable.idempotence": get_bool(
                "KAFKA_ENABLE_IDEMPOTENCE", True
            ),
            "retries": int(os.getenv("KAFKA_RETRIES", "5")),
            "max.in.flight.requests.per.connection": int(
                os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5")
            ),
            "delivery.timeout.ms": int(
                os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "120000")
            ),
            "request.timeout.ms": int(
                os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000")
            ),
        },
    }


load_environment()