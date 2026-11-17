import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config.config import load_config


class ConfigSecurityTests(unittest.TestCase):
    def setUp(self):
        for key in [
            "OPENAI_API_KEY",
            "KAFKA_SASL_USERNAME",
            "KAFKA_SASL_PASSWORD",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_SECURITY_PROTOCOL",
            "KAFKA_SASL_MECHANISM",
        ]:
            os.environ.pop(key, None)

    def test_load_config_reads_from_environment(self):
        os.environ["OPENAI_API_KEY"] = "test-openai-key"
        os.environ["KAFKA_SASL_USERNAME"] = "test-kafka-user"
        os.environ["KAFKA_SASL_PASSWORD"] = "test-kafka-pass"
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "broker:9092"
        os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
        os.environ["KAFKA_SASL_MECHANISM"] = "PLAIN"

        cfg = load_config()

        self.assertEqual(cfg["openai"]["api_key"], "test-openai-key")
        self.assertEqual(cfg["kafka"]["sasl.username"], "test-kafka-user")
        self.assertEqual(cfg["kafka"]["sasl.password"], "test-kafka-pass")
        self.assertEqual(cfg["kafka"]["bootstrap.servers"], "broker:9092")

    def test_load_config_requires_missing_secrets_to_be_explicit(self):
        with self.assertRaises(ValueError):
            load_config()


if __name__ == "__main__":
    unittest.main()
