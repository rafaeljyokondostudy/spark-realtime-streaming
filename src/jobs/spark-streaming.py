import os
import shutil
from pathlib import Path

from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.functions import col, from_json, to_json, struct, udf, when, lit
from pyspark.sql import SparkSession
from openai import OpenAI
from config.config import config

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

def sentiment_analysis(comment) -> str:
    if not comment:
        return "EMPTY"

    client = OpenAI(api_key=config["openai"]["api_key"])
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
            {"role": "user", "content": comment},
        ],
    )
    return response.choices[0].message.content.strip().upper()

def start_streaming(spark: SparkSession, checkpoint_dir: Path):
    topic = "customers_review"
    stream_df = (
        spark.readStream.format("socket")
        .option("host", "spark-master")
        .option("port", 8522)
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

    stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))
    sentiment_analysis_udf = udf(sentiment_analysis, StringType())
    stream_df = stream_df.withColumn("feedback", when(col("text").isNotNull(), sentiment_analysis_udf(col("text"))).otherwise(None))
    
    kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

    jaas_config = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{config["kafka"]["sasl.username"]}" '
        f'password="{config["kafka"]["sasl.password"]}";'
    )

    query = (
        kafka_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
        .option("kafka.security.protocol", config["kafka"]["security.protocol"])
        .option("kafka.sasl.mechanism", config["kafka"]["sasl.mechanism"])
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("checkpointLocation", str(checkpoint_dir.as_posix()))
        .option("topic", topic)
        .trigger(processingTime="5 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    ensure_windows_hadoop_setup()

    checkpoint_dir = Path(__file__).resolve().parents[2] / "tmp" / "spark-checkpoints" / "kafka-stream"
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("SocketStreamConsumer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.checkpointLocation", str(checkpoint_dir.as_posix()))
        .getOrCreate()
    )

    start_streaming(spark, checkpoint_dir)