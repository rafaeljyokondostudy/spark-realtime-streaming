import json
from datetime import datetime, timezone
from pathlib import Path


def get_processed_ids_path(checkpoint_dir: Path) -> Path:
    return checkpoint_dir.parent / "processed-review-ids.txt"


def mark_processed(record: dict, processed_path: Path | None = None) -> bool:
    review_id = str(record.get("review_id", "")).strip()
    if not review_id:
        return False

    processed_path = processed_path or Path("/tmp/processed-review-ids.txt")
    processed_path.parent.mkdir(parents=True, exist_ok=True)

    seen_ids = set()
    if processed_path.exists():
        seen_ids = {line.strip() for line in processed_path.read_text(encoding="utf-8").splitlines() if line.strip()}

    if review_id in seen_ids:
        return True

    with processed_path.open("a", encoding="utf-8") as fp:
        fp.write(f"{review_id}\n")
    return False


def write_dead_letter(record: dict, reason: str, dead_letter_path: Path | None = None) -> None:
    payload = dict(record)
    payload["reason"] = reason
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()

    dead_letter_path = dead_letter_path or Path("/tmp/dead-letter.jsonl")
    dead_letter_path.parent.mkdir(parents=True, exist_ok=True)

    with dead_letter_path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, default=str) + "\n")
