# Spark Realtime Streaming

Real-time sentiment analysis for review data using Apache Spark Structured Streaming, Kafka, and OpenAI.

This project is built for data engineers, ML practitioners, and developers who want a practical example of streaming data enrichment in a production-like architecture without needing a full enterprise setup.

[![Python](https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.3.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Confluent%20Cloud-231F20?logo=apachekafka&logoColor=white)](https://www.confluent.io/cloud/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

## Why this project?

Many streaming demos stop at reading data from a socket and writing it somewhere else. This project goes one step further by combining live ingestion, schema validation, AI-driven enrichment, and event publishing into a small end-to-end pipeline that is easy to run locally.

It is designed to help you understand how real-time data systems are assembled and how to think about reliability, observability, and fault tolerance in streaming workflows.

## Demo / pipeline preview

```mermaid
flowchart LR
    A["Yelp review dataset"] --> B["Socket producer"]
    B --> C["Spark Structured Streaming"]
    C --> D["Schema validation"]
    D --> E["OpenAI sentiment analysis"]
    E --> F["Kafka topic"]
```

## What this project does

- Reads review records from a JSON dataset
- Streams them over a TCP socket
- Parses each payload with a known schema
- Uses OpenAI to classify sentiment as POSITIVE, NEGATIVE, or NEUTRAL
- Publishes the enriched record to Kafka
- Runs in Docker with Spark master and worker services

## Project structure

```text
spark-realtime-streaming/
├── README.md
├── LICENSE
├── .env.example
├── pyproject.toml
├── src/
│   ├── Dockerfile.spark
│   ├── docker-compose.yml
│   ├── requirements.txt
│   ├── config/
│   │   └── config.py
│   ├── jobs/
│   │   ├── spark-streaming.py
│   │   └── streaming-socket.py
│   ├── datasets/
│   │   └── yelp_academic_dataset_review.json
│   └── schemas/
│       └── reviews.schema.avsc.py
├── tests/
│   └── test_config.py
├── tmp/
│   └── spark-checkpoints/
│       └── kafka-stream/
└── .gitignore
```

## Prerequisites

Before you begin, make sure you have:

- Docker Desktop or Docker Engine
- Python 3.14 or compatible version
- Access to a Kafka broker or Confluent Cloud cluster
- An OpenAI API key
- A terminal with git installed
- Optional: Windows `winutils.exe` if you run Spark locally on Windows

## Installation

### 1) Clone the repository

```bash
git clone https://github.com/<your-username>/spark-realtime-streaming.git
cd spark-realtime-streaming
```

### 2) Create your environment file

Copy the example file and fill in your real values:

```bash
cp .env.example .env
```

Then set the required variables in `.env`:

```bash
OPENAI_API_KEY=your-openai-api-key
KAFKA_SASL_USERNAME=your-kafka-api-key
KAFKA_SASL_PASSWORD=your-kafka-secret
KAFKA_BOOTSTRAP_SERVERS=your-bootstrap-server:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_TOPIC=customers_review
SOCKET_HOST=spark-master
SOCKET_PORT=8522
```

### 3) Install Python dependencies

```bash
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r src/requirements.txt
```

### 4) Start the Spark services

```bash
docker compose -f src/docker-compose.yml up --build -d
```

This starts:

- Spark master on `http://localhost:9090`
- Spark worker connected to the cluster

## Quick start

### 1) Start the socket producer

Open a new terminal and run:

```bash
python src/jobs/streaming-socket.py
```

This script listens on port `8522` and sends Yelp review records to the Spark socket source.

### 2) Run the Spark consumer

In another terminal, submit the application to the Spark master:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  /opt/spark/jobs/spark-streaming.py
```

The consumer will:

- read the socket stream
- parse JSON records using a strict schema
- run sentiment classification with OpenAI
- write enriched records to the Kafka topic

## Configuration

The project reads required secrets from environment variables and does not hardcode credentials in source files.

Key settings are loaded from `.env` and can also be overridden directly in the shell environment when needed.

### Recommended settings

```bash
OPENAI_API_KEY=your-openai-api-key
KAFKA_SASL_USERNAME=your-kafka-user
KAFKA_SASL_PASSWORD=your-kafka-secret
KAFKA_BOOTSTRAP_SERVERS=your-broker:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_TOPIC=customers_review
```

This pattern is safer than storing secrets in source control and makes deployment more portable across local, Docker, and cloud environments.


## Windows notes

If you run Spark locally on Windows, make sure Hadoop binaries are available for checkpointing:

- `HADOOP_HOME` must point to your Hadoop installation
- `winutils.exe` and `hadoop.dll` must exist in the `bin` folder

Example:

```powershell
setx HADOOP_HOME C:\hadoop
```

## Contributing

Contributions are welcome. If you want to improve the project:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the relevant tests and validation steps
5. Open a pull request with a clear summary

For bug reports or feature requests, open an issue in the repository.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

The project is intended for learning, experimentation, and practical data engineering exploration. You are free to use, modify, and extend it as long as the license terms are respected.

## Contact

For questions, suggestions, or collaboration opportunities, open an issue or reach out through the project repository.
