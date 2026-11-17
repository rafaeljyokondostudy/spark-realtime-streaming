import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, when
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from config.config import load_config
from streaming.idempotency import get_processed_ids_path, mark_processed, write_dead_letter
from streaming.kafka import build_kafka_options
from streaming.sentiment import sentiment_analysis_with_retry


def ensure_windows_hadoop_setup() -> None:
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if not hadoop_home:
        raise RuntimeError(
            "Spark on Windows requires Hadoop binaries for streaming checkpoints. "
            "Install winutils.exe and set HADOOP_HOME (or hadoop.home.dir) to the Hadoop folder. "
            "Example: C:\\hadoop and HADOOP_HOME=C:\\hadoop"
        )

    hadoop_home = Path(hadoop_home).expanduser().resolve()
    winutils = hadoop_home / "bin" / "winutils.exe"
    hadoop_dll = hadoop_home / "bin" / "hadoop.dll"

    if not winutils.exists() or not hadoop_dll.exists():
        raise RuntimeError(
            "HADOOP_HOME is set, but the required Windows Hadoop runtime files are missing. "
            "This project uses PySpark 4.3.0, which expects Hadoop 3.5.0 binaries. "
            "Download the Hadoop 3.5.0 winutils package, extract it to C:\\hadoop\\bin, "
            "and make sure both winutils.exe and hadoop.dll exist there. "
            "Then restart VS Code and run the script again."
        )

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")


def get_runtime_config() -> dict:
    return load_config()


def process_batch(spark: SparkSession, batch_df, batch_id: str, checkpoint_dir: Path):
    cfg = get_runtime_config()
    topic = os.getenv("KAFKA_TOPIC", "customers_review")
    processed_path = get_processed_ids_path(checkpoint_dir)
    dead_letter_path = checkpoint_dir.parent / "dead-letter.jsonl"

    rows = batch_df.toPandas().to_dict(orient="records")
    valid_rows = []

    for row in rows:
        review_id = str(row.get("review_id", "")).strip()
        text = row.get("text")

        if not review_id:
            write_dead_letter(row, "MISSING_REVIEW_ID", dead_letter_path)
            continue

        if not text or not str(text).strip():
            write_dead_letter(row, "MISSING_TEXT", dead_letter_path)
            continue

        if mark_processed(row, processed_path):
            write_dead_letter(row, "DUPLICATE_EVENT", dead_letter_path)
            continue

        try:
            sentiment = sentiment_analysis_with_retry(str(text))
        except Exception as exc:  # pragma: no cover - external API behavior
            write_dead_letter(row, f"OPENAI_FAILURE:{type(exc).__name__}", dead_letter_path)
            continue

        row["feedback"] = sentiment
        valid_rows.append(row)

    if valid_rows:
        output_schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType()),
            StructField("feedback", StringType()),
        ])
        kafka_df = spark.createDataFrame(valid_rows, output_schema)
        kafka_df.selectExpr(
            "CAST(review_id AS STRING) AS key",
            "to_json(struct(*)) AS value",
        ).write.format("kafka").options(**build_kafka_options(cfg, topic)).save()


def start_streaming(spark: SparkSession, checkpoint_dir: Path):
    stream_df = (
        spark.readStream.format("socket")
        .option("host", os.getenv("SOCKET_HOST", "spark-master"))
        .option("port", int(os.getenv("SOCKET_PORT", "8522")))
        .load()
    )

    schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        StructField("date", StringType()),
        StructField("text", StringType()),
    ])

    parsed_df = stream_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    parsed_df = parsed_df.dropDuplicates(["review_id"])

    safe_sentiment_udf = udf(lambda text: sentiment_analysis_with_retry(str(text)) if text and str(text).strip() else None, StringType())
    enriched_df = parsed_df.withColumn(
        "feedback",
        when(col("text").isNotNull(), safe_sentiment_udf(col("text"))).otherwise(None),
    )

    dead_letter_df = enriched_df.filter(col("feedback").isNull())
    valid_df = enriched_df.filter(col("feedback").isNotNull())

    dead_letter_df.writeStream.format("json").option("path", str((checkpoint_dir.parent / "dead-letter"))).option("checkpointLocation", str(checkpoint_dir.parent / "dead-letter-checkpoint")).start()

    return (
        valid_df.writeStream
        .foreachBatch(lambda batch_df, batch_id: process_batch(spark, batch_df, batch_id, checkpoint_dir))
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", str(checkpoint_dir.as_posix()))
        .start()
    )


if __name__ == "__main__":
    ensure_windows_hadoop_setup()

    checkpoint_dir = Path(__file__).resolve().parents[2] / "tmp" / "spark-checkpoints" / "kafka-stream"
    dead_letter_checkpoint_dir = checkpoint_dir.parent / "dead-letter-checkpoint"
    processed_ids_path = checkpoint_dir.parent / "processed-review-ids.txt"

    for stale_path in [checkpoint_dir, dead_letter_checkpoint_dir, processed_ids_path]:
        if stale_path.exists():
            if stale_path.is_dir():
                shutil.rmtree(stale_path)
            else:
                stale_path.unlink()

    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("SocketStreamConsumer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.checkpointLocation", str(checkpoint_dir.as_posix()))
        .getOrCreate()
    )

    query = start_streaming(spark, checkpoint_dir)
    query.awaitTermination()