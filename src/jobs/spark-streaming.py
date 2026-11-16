import os
import time
from pathlib import Path

from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, udf, when
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from config.config import config, load_config


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


def sentiment_analysis(comment: str) -> str:
    if not comment or not str(comment).strip():
        return "EMPTY"

    cfg = get_runtime_config()
    client = OpenAI(api_key=cfg["openai"]["api_key"])
    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a sentiment classifier. Return exactly one word: POSITIVE, "
                    "NEGATIVE, or NEUTRAL. Do not add punctuation or explanations."
                ),
            },
            {"role": "user", "content": str(comment)},
        ],
        timeout=30,
    )
    return response.choices[0].message.content.strip().upper()


def sentiment_analysis_with_retry(comment: str, retries: int = 3, backoff_seconds: float = 1.0) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return sentiment_analysis(comment)
        except Exception as exc:  # pragma: no cover - actual OpenAI failures are external
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Sentiment analysis failed after {retries} attempts") from last_error


def build_jaas_config() -> str:
    cfg = get_runtime_config()
    return (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{cfg["kafka"]["sasl.username"]}" '
        f'password="{cfg["kafka"]["sasl.password"]}";'
    )


def start_streaming(spark: SparkSession, checkpoint_dir: Path):
    cfg = get_runtime_config()
    topic = os.getenv("KAFKA_TOPIC", "customers_review")
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
    parsed_df = parsed_df.filter(col("review_id").isNotNull() & col("text").isNotNull())

    sentiment_udf = udf(lambda text: sentiment_analysis_with_retry(text), StringType())
    enriched_df = parsed_df.withColumn(
        "feedback",
        when(col("text").isNotNull(), sentiment_udf(col("text"))).otherwise(None),
    )

    kafka_df = enriched_df.selectExpr(
        "CAST(review_id AS STRING) AS key",
        "to_json(struct(*)) AS value",
    )

    kafka_options = {
        "kafka.bootstrap.servers": cfg["kafka"]["bootstrap.servers"],
        "kafka.security.protocol": cfg["kafka"]["security.protocol"],
        "kafka.sasl.mechanism": cfg["kafka"]["sasl.mechanism"],
        "kafka.sasl.jaas.config": build_jaas_config(),
        "kafka.acks": cfg["kafka"]["acks"],
        "kafka.enable.idempotence": str(cfg["kafka"]["enable.idempotence"]).lower(),
        "kafka.retries": str(cfg["kafka"]["retries"]),
        "kafka.max.in.flight.requests.per.connection": str(cfg["kafka"]["max.in.flight.requests.per.connection"]),
        "kafka.delivery.timeout.ms": str(cfg["kafka"]["delivery.timeout.ms"]),
        "kafka.request.timeout.ms": str(cfg["kafka"]["request.timeout.ms"]),
        "checkpointLocation": str(checkpoint_dir.as_posix()),
        "topic": topic,
    }

    return (
        kafka_df.writeStream
        .format("kafka")
        .options(**kafka_options)
        .trigger(processingTime="5 seconds")
        .start()
    )


if __name__ == "__main__":
    ensure_windows_hadoop_setup()

    checkpoint_dir = Path(__file__).resolve().parents[2] / "tmp" / "spark-checkpoints" / "kafka-stream"
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