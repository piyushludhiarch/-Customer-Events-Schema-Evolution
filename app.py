"""Read incoming event CSV files, classify them, and save clean results."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, size, split, trim, udf
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_time", "device")


def categorize_event(event_type):
    """Put each event type into a simple business category."""
    normalized = (event_type or "").strip().upper()
    if normalized in {"LOGIN", "LOGOUT", "SESSION_START", "SESSION_END"}:
        return "SESSION"
    if normalized in {"SEARCH", "VIEW", "CLICK"}:
        return "DISCOVERY"
    if normalized in {"PURCHASE", "PAYMENT", "REFUND"}:
        return "TRANSACTION"
    return "OTHER"


category_udf = udf(categorize_event, StringType())


def process_batch(batch_df: DataFrame, batch_id: int, output_dir: Path) -> None:
    """Clean one group of new files and save results for this batch."""
    # The two later columns are null for five-column files from the old release.
    valid_condition = col("_corrupt_record").isNull() | (
        (size(split(col("_corrupt_record"), ",", -1)) == len(REQUIRED_FIELDS))
        & col("browser").isNull()
        & col("app_version").isNull()
    )
    for field in REQUIRED_FIELDS:
        valid_condition = valid_condition & col(field).isNotNull()
        valid_condition = valid_condition & (trim(col(field).cast("string")) != "")

    events = batch_df.withColumn("event_category", category_udf(col("event_type")))
    valid = events.filter(valid_condition).persist()
    invalid = events.filter(~valid_condition).persist()

    valid_count = valid.count()
    invalid_count = invalid.count()
    statistics = valid.groupBy("event_category", "device").count()
    statistics = statistics.withColumn("batch_id", lit(batch_id))

    # One folder per batch prevents a restarted stream from duplicating old output.
    valid.write.mode("overwrite").parquet(str(output_dir / "events" / f"batch_id={batch_id}"))
    invalid.write.mode("overwrite").parquet(str(output_dir / "quarantine" / f"batch_id={batch_id}"))
    statistics.write.mode("overwrite").parquet(
        str(output_dir / "statistics" / f"batch_id={batch_id}")
    )

    print(
        "BATCH_PROCESSED "
        + json.dumps(
            {
                "batch_id": batch_id,
                "valid_records": valid_count,
                "quarantined_records": invalid_count,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    valid.unpersist()
    invalid.unpersist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream customer events from incoming CSV files.")
    parser.add_argument("--source", type=Path, required=True, help="Folder receiving CSV files")
    parser.add_argument("--output", type=Path, required=True, help="Folder for results")
    parser.add_argument("--checkpoint", type=Path, required=True, help="Folder for restart recovery")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all currently available files and exit (useful for staged demonstrations)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.source.mkdir(parents=True, exist_ok=True)

    # Spark expects HADOOP_HOME to be the parent of its bin folder on Windows.
    if os.name == "nt":
        hadoop_home = Path(os.environ.get("HADOOP_HOME", ""))
        if hadoop_home.name.lower() == "bin" and (hadoop_home / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = str(hadoop_home.parent)

    # Step 1: start Spark with the supplied schema and read new CSV files.
    spark = (
        SparkSession.builder.appName("CustomerEventsSchemaEvolution")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        events = (
            spark.readStream.schema(EVENT_SCHEMA)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .csv(str(args.source))
        )

        # Step 2: process each batch and remember progress in the checkpoint.
        def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
            process_batch(batch_df, batch_id, args.output)

        query = events.writeStream.foreachBatch(handle_batch)
        query = query.option("checkpointLocation", str(args.checkpoint))

        if args.once:
            query.trigger(availableNow=True).start().awaitTermination()
        else:
            query.trigger(processingTime="5 seconds").start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
