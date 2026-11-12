import os
from pathlib import Path

from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession


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


def start_streaming(spark: SparkSession):
    stream_df = (
        spark.readStream.format("socket")
        .option("host", "127.0.0.1")
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
    query = (
        stream_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    ensure_windows_hadoop_setup()

    # checkpoint_dir = Path(__file__).resolve().parents[2] / "tmp" / "spark-checkpoints"
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SocketStreamConsumer")
        .config("spark.sql.shuffle.partitions", "1")
        # .config("spark.sql.streaming.checkpointLocation", str(checkpoint_dir.as_posix()))
        .getOrCreate()
    )

    start_streaming(spark)