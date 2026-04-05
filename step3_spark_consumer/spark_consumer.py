# -*- coding: utf-8 -*-
"""
spark_consumer.py  -  Step 3  (Windows cp1252 safe)
All Unicode box/emoji chars replaced with plain ASCII.
"""

import sys
import os
import logging
import traceback
from datetime import datetime

# Force UTF-8 output so Windows CMD never hits cp1252 encode errors
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

from spark_priority import process_batch
from mongo_writer   import MongoWriter

logging.basicConfig(
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("SparkConsumer")

# -- Config ---------------------------------------------------
KAFKA_BROKER     = "localhost:9092"
KAFKA_TOPIC      = "cctv-hand-events"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/cctv/raw_events"
TRIGGER_INTERVAL = "5 seconds"
CHECKPOINT_DIR   = "/tmp/spark_checkpoint/cctv"

# -- Event schema ---------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("camera_id",    IntegerType(), True),
    StructField("gesture",      StringType(),  True),
    StructField("danger_level", StringType(),  True),
    StructField("priority",     IntegerType(), True),
    StructField("timestamp",    StringType(),  True),
])


# -- Spark Session --------------------------------------------
def create_spark_session() -> SparkSession:
    # Tell Hadoop to use "root" instead of the Windows login name
    os.environ["HADOOP_USER_NAME"] = "root"

    spark = (SparkSession.builder
        .appName("CCTV-GestureAlertSystem")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.hadoop.job.ugi", "root")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready  (Spark %s)", spark.version)
    return spark


# -- Kafka reader ---------------------------------------------
def read_kafka_stream(spark: SparkSession):
    log.info("Connecting Kafka -> %s  topic: %s", KAFKA_BROKER, KAFKA_TOPIC)
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("kafka.metadata.max.age.ms", "30000")
            .option("kafka.request.timeout.ms", "60000")
            .option("kafka.session.timeout.ms", "30000")
            .load())


# -- Parse JSON -----------------------------------------------
def parse_events(raw_df):
    return (raw_df
            .select(
                F.col("timestamp").alias("kafka_received_at"),
                F.col("value").cast("string").alias("json_str"),
            )
            .withColumn("event", F.from_json("json_str", EVENT_SCHEMA))
            .select(
                "kafka_received_at",
                F.col("event.camera_id").alias("camera_id"),
                F.col("event.gesture").alias("gesture"),
                F.col("event.danger_level").alias("danger_level"),
                F.col("event.priority").alias("priority"),
                F.col("event.timestamp").alias("timestamp"),
            )
            .filter(F.col("camera_id").isNotNull()))


# -- HDFS writer ----------------------------------------------
def _write_to_hdfs(batch_df, batch_id: int):
    print(f"\n  [HDFS] Writing batch #{batch_id} -> {HDFS_OUTPUT_PATH}")
    try:
        df_out = batch_df.withColumn("date", F.to_date(F.current_timestamp()))
        (df_out.write
               .mode("append")
               .partitionBy("date")
               .json(HDFS_OUTPUT_PATH))
        print(f"  [HDFS] OK - Batch #{batch_id} written")
        print(f"  [HDFS] Path: {HDFS_OUTPUT_PATH}/date={datetime.now().date()}/")
    except Exception:
        print(f"  [HDFS] FAILED - Batch #{batch_id}:")
        traceback.print_exc()
        print("  [HDFS] Check: is Hadoop running?  docker-compose ps")


# -- MongoDB writer -------------------------------------------
def _write_to_mongo(all_events: list, alerts: list, batch_id: int):
    print(f"\n  [MongoDB] Writing batch #{batch_id} "
          f"(all={len(all_events)}, alerts={len(alerts)})")
    try:
        with MongoWriter() as writer:
            writer.write_all(all_events)
            writer.write_alerts(alerts)
            stats = writer.get_stats()
            print(f"  [MongoDB] OK - Write successful")
            print(f"  [MongoDB] all_events : {stats['all_events_count']} total docs")
            print(f"  [MongoDB] alerts     : {stats['alerts_count']} total docs")
    except Exception:
        print(f"  [MongoDB] FAILED:")
        traceback.print_exc()
        print("  [MongoDB] Check: is MongoDB running?  docker-compose ps")


# -- foreachBatch handler -------------------------------------
def handle_batch(batch_df, batch_id: int):
    count = batch_df.count()
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  SPARK BATCH #{batch_id}  |  {count} event(s) received")
    print(sep)

    if count == 0:
        print("  (empty batch - no gestures in last 5s)")
        return

    all_events, alerts = process_batch(batch_df, batch_id)
    _write_to_hdfs(batch_df, batch_id)
    _write_to_mongo(all_events, alerts, batch_id)


# -- Main -----------------------------------------------------
def main():
    sep = "=" * 65
    print(sep)
    print("  CCTV GESTURE ALERT SYSTEM  -  Step 3")
    print("  PySpark Structured Streaming Consumer")
    print(sep)
    print(f"  Kafka   : {KAFKA_BROKER}  ->  {KAFKA_TOPIC}")
    print(f"  HDFS    : {HDFS_OUTPUT_PATH}")
    print(f"  MongoDB : cctv_alerts  (alerts + all_events)")
    print(f"  Trigger : every {TRIGGER_INTERVAL}")
    print(f"  Ctrl+C  : stop\n")
    print(sep + "\n")

    spark        = create_spark_session()
    raw_stream   = read_kafka_stream(spark)
    event_stream = parse_events(raw_stream)

    query = (event_stream
             .writeStream
             .foreachBatch(handle_batch)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime=TRIGGER_INTERVAL)
             .start())

    log.info("Streaming started - waiting for Kafka events...")
    log.info("(Run Step 2 main.py in another terminal to send gestures)")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Spark] Stopping...")
        query.stop()
    finally:
        spark.stop()
        print("[Spark] Session closed.  Step 3 done.")


if __name__ == "__main__":
    main()
