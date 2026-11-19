import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)

from config.config import load_kafka_config
from streaming.idempotency import (
    get_processed_ids_path,
    is_processed,
    mark_processed,
    write_dead_letter,
)
from streaming.kafka import build_kafka_options
from streaming.sentiment import sentiment_analysis_with_retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DEFAULT_SOCKET_HOST = "spark-master"
DEFAULT_SOCKET_PORT = 8522

DEFAULT_KAFKA_TOPIC = "customers_review"
DEFAULT_KAFKA_BATCH_SIZE = 100

PROCESSING_TIME = "5 seconds"


# ---------------------------------------------------------
# Schemas
# ---------------------------------------------------------

REVIEW_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", FloatType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
    ]
)


OUTPUT_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", FloatType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("feedback", StringType(), True),
    ]
)


# ---------------------------------------------------------
# Windows Hadoop Configuration
# ---------------------------------------------------------


def ensure_windows_hadoop_setup() -> None:
    """
    Configure Hadoop environment variables when running Spark on Windows.
    """

    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")

    if not hadoop_home:
        raise RuntimeError(
            "Spark on Windows requires Hadoop binaries for streaming checkpoints. "
            "Install winutils.exe and set HADOOP_HOME to the Hadoop directory."
        )

    hadoop_home = Path(hadoop_home).expanduser().resolve()

    winutils = hadoop_home / "bin" / "winutils.exe"
    hadoop_dll = hadoop_home / "bin" / "hadoop.dll"

    if not winutils.exists() or not hadoop_dll.exists():
        raise RuntimeError(
            "HADOOP_HOME is configured, but winutils.exe or hadoop.dll "
            "is missing from the bin directory."
        )

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)

    os.environ["PATH"] = (
        str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")
    )


# ---------------------------------------------------------
# Kafka Publishing
# ---------------------------------------------------------


def publish_to_kafka(
    spark: SparkSession,
    records: list[dict],
    kafka_config: dict,
    topic: str,
) -> None:
    """
    Publish a collection of enriched records to Kafka.

    The review_id is used as the Kafka message key.
    """

    if not records:
        return

    kafka_df = spark.createDataFrame(
        records,
        schema=OUTPUT_SCHEMA,
    )

    kafka_df = kafka_df.selectExpr(
        "CAST(review_id AS STRING) AS key",
        "to_json(struct(*)) AS value",
    )

    (
        kafka_df.write.format("kafka")
        .options(
            **build_kafka_options(
                kafka_config,
                topic,
            )
        )
        .save()
    )

    logger.info(
        "Successfully published %d records to Kafka topic '%s'.",
        len(records),
        topic,
    )


# ---------------------------------------------------------
# Micro-batch Processing
# ---------------------------------------------------------


def process_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
    checkpoint_dir: Path,
) -> None:
    """
    Process one Spark micro-batch.

    Processing steps:
        1. Validate incoming records.
        2. Check whether records were already processed.
        3. Classify sentiment using OpenAI.
        4. Publish enriched records to Kafka.
        5. Mark successfully published records as processed.

    Records are published in chunks to avoid collecting the entire
    micro-batch into driver memory.
    """

    if batch_df.isEmpty():
        logger.info("Batch %s is empty.", batch_id)
        return

    logger.info("Processing batch %s.", batch_id)

    kafka_config = load_kafka_config()

    topic = os.getenv(
        "KAFKA_TOPIC",
        DEFAULT_KAFKA_TOPIC,
    )

    chunk_size = int(
        os.getenv(
            "KAFKA_BATCH_SIZE",
            str(DEFAULT_KAFKA_BATCH_SIZE),
        )
    )

    if chunk_size < 1:
        raise ValueError("KAFKA_BATCH_SIZE must be at least 1.")

    processed_path = get_processed_ids_path(checkpoint_dir)

    dead_letter_path = checkpoint_dir.parent / "dead-letter.jsonl"

    pending_records: list[dict] = []
    pending_ids: list[str] = []

    # Prevent duplicate processing inside the current uncommitted chunk.
    seen_in_batch: set[str] = set()

    def flush_pending_records() -> None:
        """
        Publish pending records and mark them as processed only
        after Kafka confirms the write operation completed.
        """

        if not pending_records:
            return

        publish_to_kafka(
            spark,
            pending_records,
            kafka_config,
            topic,
        )

        # Only mark records after the Kafka write succeeds.
        for review_id in pending_ids:
            mark_processed(
                {"review_id": review_id},
                processed_path,
            )

        logger.info(
            "Batch %s: committed %d processed IDs.",
            batch_id,
            len(pending_ids),
        )

        pending_records.clear()
        pending_ids.clear()

    for spark_row in batch_df.toLocalIterator():

        row = spark_row.asDict(recursive=True)

        # -------------------------------------------------
        # 1. JSON validation
        # -------------------------------------------------

        if row.get("_parse_failed"):
            write_dead_letter(
                row,
                "INVALID_JSON",
                dead_letter_path,
            )
            continue

        # -------------------------------------------------
        # 2. Required field validation
        # -------------------------------------------------

        raw_review_id = row.get("review_id")

        review_id = str(raw_review_id).strip() if raw_review_id is not None else ""

        if not review_id:
            write_dead_letter(
                row,
                "MISSING_REVIEW_ID",
                dead_letter_path,
            )
            continue

        text = row.get("text")

        if text is None or not str(text).strip():
            write_dead_letter(
                row,
                "MISSING_TEXT",
                dead_letter_path,
            )
            continue

        # -------------------------------------------------
        # 3. Duplicate detection
        # -------------------------------------------------

        if review_id in seen_in_batch:
            write_dead_letter(
                row,
                "DUPLICATE_EVENT_IN_BATCH",
                dead_letter_path,
            )
            continue

        seen_in_batch.add(review_id)

        if is_processed(row, processed_path):
            write_dead_letter(
                row,
                "DUPLICATE_EVENT",
                dead_letter_path,
            )
            continue

        # -------------------------------------------------
        # 4. OpenAI sentiment analysis
        # -------------------------------------------------

        try:
            sentiment = sentiment_analysis_with_retry(str(text))

        except Exception as exc:
            logger.exception(
                "Sentiment analysis failed for review %s.",
                review_id,
            )

            write_dead_letter(
                row,
                f"OPENAI_FAILURE:{type(exc).__name__}",
                dead_letter_path,
            )

            continue

        # -------------------------------------------------
        # 5. Prepare enriched Kafka record
        # -------------------------------------------------

        enriched_record = {
            "review_id": review_id,
            "user_id": row.get("user_id"),
            "business_id": row.get("business_id"),
            "stars": row.get("stars"),
            "date": row.get("date"),
            "text": text,
            "feedback": sentiment,
        }

        pending_records.append(enriched_record)
        pending_ids.append(review_id)

        # -------------------------------------------------
        # 6. Publish when the chunk reaches its limit
        # -------------------------------------------------

        if len(pending_records) >= chunk_size:
            flush_pending_records()

    # Publish any remaining records.
    flush_pending_records()

    logger.info(
        "Finished processing batch %s.",
        batch_id,
    )


# ---------------------------------------------------------
# Streaming Initialization
# ---------------------------------------------------------


def start_streaming(
    spark: SparkSession,
    checkpoint_dir: Path,
):
    """
    Initialize the Spark Structured Streaming query.
    """

    socket_host = os.getenv(
        "SOCKET_HOST",
        DEFAULT_SOCKET_HOST,
    )

    socket_port = int(
        os.getenv(
            "SOCKET_PORT",
            str(DEFAULT_SOCKET_PORT),
        )
    )

    stream_df = (
        spark.readStream.format("socket")
        .option("host", socket_host)
        .option("port", socket_port)
        .load()
    )

    # Keep the original message so malformed JSON can be
    # written to the dead-letter file.
    parsed_df = stream_df.select(
        col("value").cast("string").alias("raw_value"),
        from_json(
            col("value").cast("string"),
            REVIEW_SCHEMA,
        ).alias("parsed"),
    ).select(
        col("raw_value"),
        col("parsed").isNull().alias("_parse_failed"),
        col("parsed.*"),
    )

    query = (
        parsed_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                spark,
                batch_df,
                batch_id,
                checkpoint_dir,
            )
        )
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TIME)
        .option(
            "checkpointLocation",
            str(checkpoint_dir.resolve()),
        )
        .start()
    )

    return query


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    ensure_windows_hadoop_setup()

    project_root = Path(__file__).resolve().parents[2]

    checkpoint_dir = project_root / "tmp" / "spark-checkpoints" / "kafka-stream"

    checkpoint_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = (
        SparkSession.builder.appName("SocketStreamConsumer")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.streaming.checkpointLocation",
            str(checkpoint_dir.resolve()),
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        query = start_streaming(
            spark,
            checkpoint_dir,
        )

        logger.info("Spark streaming query started.")

        query.awaitTermination()

    finally:
        spark.stop()
