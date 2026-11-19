import json
import logging
import socket
import time
from pathlib import Path
from typing import Iterator

# The server listens on all network interfaces.
# Spark connects using the Docker service hostname, such as "spark-master".
SOCKET_HOST = "0.0.0.0"
SOCKET_PORT = 8522

DEFAULT_CHUNK_SIZE = 2
DEFAULT_DELAY_SECONDS = 5
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 2.0

logger = logging.getLogger(__name__)


def resolve_file_path(file_path: str | Path) -> Path:
    """
    Resolve the dataset path.

    Relative paths are resolved from the src directory.
    Absolute paths are used directly.
    """
    file_path = Path(file_path)

    if not file_path.is_absolute():
        file_path = Path(__file__).resolve().parents[1] / file_path

    if not file_path.is_file():
        raise FileNotFoundError(f"Dataset file not found: {file_path}")

    return file_path


def iter_jsonl_chunks(
    file_path: Path,
    chunk_size: int,
) -> Iterator[list[dict]]:
    """
    Read a JSONL file and yield records in chunks.

    Each non-empty line must contain one JSON object.
    """
    records: list[dict] = []

    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc}") from exc

            if not isinstance(record, dict):
                raise ValueError(
                    f"Expected a JSON object on line {line_number}, "
                    f"received {type(record).__name__}."
                )

            records.append(record)

            if len(records) >= chunk_size:
                yield records
                records = []

    if records:
        yield records


def emit_chunk(
    conn: socket.socket,
    records: list[dict],
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> None:
    """
    Send JSON records through the TCP connection.

    Each record is serialized as JSON followed by a newline.
    This creates the JSON Lines format expected by the Spark
    socket source.
    """
    for record in records:
        payload = json.dumps(record).encode("utf-8") + b"\n"

        conn.sendall(payload)

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    logger.info("Sent %d records.", len(records))


def send_data_over_socket(
    file_path: str | Path,
    host: str = SOCKET_HOST,
    port: int = SOCKET_PORT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> None:
    """
    Start a TCP server and continuously send the dataset to clients.

    Behaviour:
        1. Start listening for a client connection.
        2. Accept a connection from Spark.
        3. Read the dataset and send its records.
        4. Close the connection after the dataset is sent.
        5. Wait for another connection and replay the dataset.

    The dataset always starts from the beginning for each connection.
    """

    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1.")

    if max_retries < 1:
        raise ValueError("max_retries must be at least 1.")

    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535.")

    if delay_seconds < 0 or retry_delay_seconds < 0:
        raise ValueError("Delay values cannot be negative.")

    file_path = resolve_file_path(file_path)

    consecutive_failures = 0

    logger.info("Dataset: %s", file_path)
    logger.info("Starting socket server on %s:%s", host, port)

    while True:
        try:
            with socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            ) as server_socket:

                server_socket.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_REUSEADDR,
                    1,
                )

                server_socket.bind((host, port))
                server_socket.listen(1)

                logger.info(
                    "Waiting for a client connection on %s:%s...",
                    host,
                    port,
                )

                conn, address = server_socket.accept()

                logger.info("Client connected: %s", address)

                with conn:
                    total_sent = 0

                    for records in iter_jsonl_chunks(
                        file_path,
                        chunk_size,
                    ):
                        emit_chunk(
                            conn,
                            records,
                            delay_seconds,
                        )

                        total_sent += len(records)

                    logger.info(
                        "Dataset transmission completed. " "Total records sent: %d",
                        total_sent,
                    )

            # A complete transmission resets the consecutive failure count.
            consecutive_failures = 0

            logger.info(
                "Waiting for the next connection. "
                "The dataset will be replayed from the beginning."
            )

        except (BrokenPipeError, ConnectionResetError) as exc:
            consecutive_failures += 1

            logger.warning(
                "Client disconnected unexpectedly. " "Failure %d/%d. Error: %s",
                consecutive_failures,
                max_retries,
                exc,
            )

        except OSError as exc:
            consecutive_failures += 1

            logger.error(
                "Socket error. Failure %d/%d. Error: %s",
                consecutive_failures,
                max_retries,
                exc,
            )

        except KeyboardInterrupt:
            logger.info("Producer interrupted by user.")
            break

        if consecutive_failures >= max_retries:
            raise RuntimeError(
                f"Producer stopped after {max_retries} " "consecutive socket failures."
            )

        if consecutive_failures > 0:
            logger.info(
                "Retrying in %.1f seconds...",
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    send_data_over_socket(
        file_path="datasets/yelp_academic_dataset_review.json",
    )
