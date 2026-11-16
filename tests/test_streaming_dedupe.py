import importlib.util
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

MODULE_PATH = SRC / "jobs" / "spark-streaming.py"
SPEC = importlib.util.spec_from_file_location("spark_streaming", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

mark_processed = module.mark_processed
write_dead_letter = module.write_dead_letter


class StreamingIdempotencyTests(unittest.TestCase):
    def test_mark_processed_rejects_duplicate_ids(self):
        path = Path("/tmp/spark-streaming-test-processed.txt")
        if path.exists():
            path.unlink()

        self.assertFalse(mark_processed({"review_id": "r-1"}, path))
        self.assertTrue(mark_processed({"review_id": "r-1"}, path))
        self.assertFalse(mark_processed({"review_id": "r-2"}, path))

    def test_write_dead_letter_creates_json_entry(self):
        path = Path("/tmp/spark-streaming-test-dead-letter.jsonl")
        if path.exists():
            path.unlink()

        payload = {"review_id": "r-99", "text": "bad review", "error": "timeout"}
        write_dead_letter(payload, "OPENAI_TIMEOUT", path)

        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        parsed = json.loads(lines[0])
        self.assertEqual(parsed["review_id"], "r-99")
        self.assertEqual(parsed["reason"], "OPENAI_TIMEOUT")


if __name__ == "__main__":
    unittest.main()
