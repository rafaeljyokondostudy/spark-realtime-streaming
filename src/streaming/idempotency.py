import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def get_processed_ids_path(checkpoint_dir: Path) -> Path:
    """
    Return the path of the local SQLite database used for deduplication.
    """
    return checkpoint_dir.parent / "processed-review-ids.sqlite3"


def _initialize_database(db_path: Path) -> None:
    """Create the processed IDs table if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_reviews (
                review_id TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            )
        """)


def is_processed(
    record: dict,
    processed_path: Path | None = None,
) -> bool:
    """
    Check whether a review ID has already been processed.
    """
    review_id = str(record.get("review_id", "")).strip()

    if not review_id:
        return False

    processed_path = (
        Path(processed_path)
        if processed_path is not None
        else Path("/tmp/processed-review-ids.sqlite3")
    )

    _initialize_database(processed_path)

    with sqlite3.connect(processed_path, timeout=30) as conn:
        result = conn.execute(
            """
            SELECT 1
            FROM processed_reviews
            WHERE review_id = ?
            """,
            (review_id,),
        ).fetchone()

    return result is not None


def mark_processed(
    record: dict,
    processed_path: Path | None = None,
) -> bool:
    """
    Record a successfully processed review.

    Returns:
        True: The review ID was already present.
        False: The review ID was newly inserted.

    IMPORTANT:
        Call this only after the downstream operation succeeds.
    """
    review_id = str(record.get("review_id", "")).strip()

    if not review_id:
        return False

    processed_path = (
        Path(processed_path)
        if processed_path is not None
        else Path("/tmp/processed-review-ids.sqlite3")
    )

    _initialize_database(processed_path)

    processed_at = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(processed_path, timeout=30) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO processed_reviews (
                review_id,
                processed_at
            )
            VALUES (?, ?)
            """,
            (review_id, processed_at),
        )

        return cursor.rowcount == 0


def write_dead_letter(
    record: dict,
    reason: str,
    dead_letter_path: Path | None = None,
) -> None:
    """
    Append an invalid or failed record to a JSON Lines dead-letter file.
    """
    payload = dict(record)
    payload["reason"] = reason
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()

    dead_letter_path = (
        Path(dead_letter_path)
        if dead_letter_path is not None
        else Path("/tmp/dead-letter.jsonl")
    )

    dead_letter_path.parent.mkdir(parents=True, exist_ok=True)

    with dead_letter_path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, default=str) + "\n")
