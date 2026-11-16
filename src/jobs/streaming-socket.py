import json
import socket
import time
from pathlib import Path

import pandas as pd

# Bind to all interfaces so the producer is reachable from the Spark driver/executors.
# The Spark socket source connects to this endpoint, so 0.0.0.0 is the correct bind address.
SOCKET_HOST = "0.0.0.0"
SOCKET_PORT = 8522


def emit_chunk(conn, records):
    chunk = pd.DataFrame(records)
    print(chunk)
    for record in chunk.to_dict(orient="records"):
        payload = json.dumps(record).encode("utf-8") + b"\n"
        conn.sendall(payload)
        time.sleep(0.5)


def send_data_over_socket(file_path, host=SOCKET_HOST, port=SOCKET_PORT, chunk_size=2, max_retries=3):
    file_path = Path(file_path)
    if not file_path.is_absolute():
        file_path = Path(__file__).resolve().parents[1] / file_path

    retries = 0
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((host, port))
                s.listen(1)
                print(f"Listening for connections on {host}:{port}")

                conn, addr = s.accept()
                print(f"Connection from {addr}")
                with conn:
                    with open(file_path, "r", encoding="utf-8") as file:
                        records = []
                        for line in file:
                            if not line.strip():
                                continue
                            records.append(json.loads(line))
                            if len(records) == chunk_size:
                                emit_chunk(conn, records)
                                records = []
                        if records:
                            emit_chunk(conn, records)
        except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected; waiting to reconnect.")
            retries += 1
            if retries >= max_retries:
                print("Maximum retries reached; stopping the producer.")
                break
            time.sleep(2)
        except KeyboardInterrupt:
            print("Producer interrupted by user.")
            break
        except OSError as exc:
            print(f"Socket error: {exc}; retrying.")
            retries += 1
            if retries >= max_retries:
                raise
            time.sleep(2)


if __name__ == "__main__":
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")