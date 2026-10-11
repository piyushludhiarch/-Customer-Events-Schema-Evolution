"""Read incoming event CSV files, classify them, and save clean results."""  # Briefly describes this program.

from __future__ import annotations  # Enables modern type-hint behavior.

import argparse  # Reads command-line options such as input and output paths.
import json  # Formats each batch result as a readable JSON message.
import os  # Checks the operating system and environment variables.
from pathlib import Path  # Handles file and folder paths safely.

from pyspark.sql import DataFrame, SparkSession  # Imports Spark tables and the Spark session.
from pyspark.sql.functions import col, lit, size, split, trim, udf  # Imports operations used to transform rows.
from pyspark.sql.types import (  # Imports Spark data types for the input schema.
    StringType,  # Defines text columns.
    StructField,  # Defines one named column in a schema.
    StructType,  # Groups the columns into a schema.
    TimestampType,  # Defines the event_time column as a timestamp.
)  # Ends the Spark type imports.


EVENT_SCHEMA = StructType(  # Declares one schema that supports both file versions.
    [  # Starts the list of input columns.
        StructField("event_id", StringType(), True),  # Stores the unique event ID.
        StructField("user_id", StringType(), True),  # Stores the user who made the event.
        StructField("event_type", StringType(), True),  # Stores the event name, such as LOGIN.
        StructField("event_time", TimestampType(), True),  # Stores when the event happened.
        StructField("device", StringType(), True),  # Stores the device used by the user.
        StructField("browser", StringType(), True),  # Stores the newer optional browser field.
        StructField("app_version", StringType(), True),  # Stores the newer optional app version.
        StructField("_corrupt_record", StringType(), True),  # Stores malformed CSV rows for review.
    ]  # Ends the list of input columns.
)  # Builds the complete input schema.

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_time", "device")  # Names the fields every good row must have.


def categorize_event(event_type):  # Creates the business category for one event type.
    """Put each event type into a simple business category."""  # Explains the function briefly.
    normalized = (event_type or "").strip().upper()  # Makes matching ignore missing values, spaces, and letter case.
    if normalized in {"LOGIN", "LOGOUT", "SESSION_START", "SESSION_END"}:  # Checks for session-related events.
        return "SESSION"  # Labels session activity.
    if normalized in {"SEARCH", "VIEW", "CLICK"}:  # Checks for finding or viewing content.
        return "DISCOVERY"  # Labels content-discovery activity.
    if normalized in {"PURCHASE", "PAYMENT", "REFUND"}:  # Checks for money-related events.
        return "TRANSACTION"  # Labels transaction activity.
    return "OTHER"  # Gives unfamiliar events a safe catch-all category.


category_udf = udf(categorize_event, StringType())  # Makes the Python category function usable in Spark.


def process_batch(batch_df: DataFrame, batch_id: int, output_dir: Path) -> None:  # Processes one group of new files.
    """Clean one group of new files and save results for this batch."""  # Describes the batch-processing function.
    # Accept a short five-column CSV row only when the two newer fields are absent.
    valid_condition = col("_corrupt_record").isNull() | (  # Accepts normal rows or checks legacy row width below.
        (size(split(col("_corrupt_record"), ",", -1)) == len(REQUIRED_FIELDS))  # Confirms an old row has five values.
        & col("browser").isNull()  # Confirms the old row has no browser value.
        & col("app_version").isNull()  # Confirms the old row has no app-version value.
    )  # Finishes the compatible-row condition.
    for field in REQUIRED_FIELDS:  # Checks each field required by the assignment.
        valid_condition = valid_condition & col(field).isNotNull()  # Rejects missing required values.
        valid_condition = valid_condition & (trim(col(field).cast("string")) != "")  # Rejects blank required values.

    events = batch_df.withColumn("event_category", category_udf(col("event_type")))  # Adds a category to every input row.
    valid = events.filter(valid_condition).persist()  # Keeps and temporarily caches good rows.
    invalid = events.filter(~valid_condition).persist()  # Keeps and temporarily caches bad rows for quarantine.

    valid_count = valid.count()  # Counts rows accepted for downstream use.
    invalid_count = invalid.count()  # Counts rows sent to quarantine.
    statistics = valid.groupBy("event_category", "device").count()  # Counts valid events by category and device.
    statistics = statistics.withColumn("batch_id", lit(batch_id))  # Adds the batch number to the statistics.

    # A separate folder per batch makes retries replace output instead of duplicating it.
    valid.write.mode("overwrite").parquet(str(output_dir / "events" / f"batch_id={batch_id}"))  # Saves clean events.
    invalid.write.mode("overwrite").parquet(str(output_dir / "quarantine" / f"batch_id={batch_id}"))  # Saves rejected events.
    statistics.write.mode("overwrite").parquet(  # Starts saving the category/device counts.
        str(output_dir / "statistics" / f"batch_id={batch_id}")  # Chooses this batch's statistics folder.
    )  # Finishes writing statistics.

    print(  # Prints a simple progress message for the demonstration.
        "BATCH_PROCESSED "  # Labels the line so it is easy to find in logs.
        + json.dumps(  # Converts the batch details into JSON text.
            {  # Starts the details to include in the message.
                "batch_id": batch_id,  # Shows which Spark batch was processed.
                "valid_records": valid_count,  # Shows how many good rows were saved.
                "quarantined_records": invalid_count,  # Shows how many bad rows were separated.
            },  # Ends the batch details.
            sort_keys=True,  # Prints JSON keys in a consistent order.
        ),  # Finishes creating the JSON text.
        flush=True,  # Prints immediately so the live demo shows progress.
    )  # Finishes printing the progress message.
    valid.unpersist()  # Frees Spark's temporary cache for good rows.
    invalid.unpersist()  # Frees Spark's temporary cache for quarantined rows.


def parse_args() -> argparse.Namespace:  # Reads and returns command-line settings.
    parser = argparse.ArgumentParser(description="Stream customer events from incoming CSV files.")  # Creates the command-line parser.
    parser.add_argument("--source", type=Path, required=True, help="Folder receiving CSV files")  # Requires the incoming-files folder.
    parser.add_argument("--output", type=Path, required=True, help="Folder for results")  # Requires the output folder.
    parser.add_argument("--checkpoint", type=Path, required=True, help="Folder for restart recovery")  # Requires the saved progress folder.
    parser.add_argument(  # Starts the optional one-time processing setting.
        "--once",  # Adds a flag for processing current files and then stopping.
        action="store_true",  # Makes the flag true when it appears on the command line.
        help="Process all currently available files and exit (useful for staged demonstrations)",  # Explains when to use the flag.
    )  # Finishes adding the optional setting.
    return parser.parse_args()  # Returns the settings entered by the user.


def main() -> None:  # Starts the application.
    args = parse_args()  # Reads the input, output, checkpoint, and run-mode settings.
    args.source.mkdir(parents=True, exist_ok=True)  # Creates the input folder if it does not exist.

    # Spark expects HADOOP_HOME to be the parent folder of bin on Windows.
    if os.name == "nt":  # Runs this fix only on Windows.
        hadoop_home = Path(os.environ.get("HADOOP_HOME", ""))  # Reads the optional Hadoop folder setting.
        if hadoop_home.name.lower() == "bin" and (hadoop_home / "winutils.exe").is_file():  # Detects a bin-folder path with winutils.
            os.environ["HADOOP_HOME"] = str(hadoop_home.parent)  # Sets Hadoop home to the parent directory Spark expects.

    # Step 1: start Spark with the supplied schema and watch the input folder.
    spark = (  # Starts building the Spark session.
        SparkSession.builder.appName("CustomerEventsSchemaEvolution")  # Gives this Spark application a clear name.
        .config("spark.sql.session.timeZone", "UTC")  # Makes timestamp handling consistent.
        .getOrCreate()  # Reuses an existing Spark session or creates one.
    )  # Finishes creating the Spark session.
    spark.sparkContext.setLogLevel("WARN")  # Hides extra Spark messages while keeping warnings and errors.
    try:  # Ensures Spark is stopped even if stream processing fails.
        events = (  # Starts configuring the streaming CSV reader.
            spark.readStream.schema(EVENT_SCHEMA)  # Watches for new CSV data using the declared schema.
            .option("header", "true")  # Treats the first row in each CSV as column names.
            .option("mode", "PERMISSIVE")  # Keeps malformed rows available for validation.
            .option("columnNameOfCorruptRecord", "_corrupt_record")  # Names the column for malformed input.
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")  # Defines the required event-time text format.
            .csv(str(args.source))  # Reads CSV files from the chosen input folder.
        )  # Finishes configuring the streaming reader.

        # Step 2: process each new batch and record progress in the checkpoint.
        def handle_batch(batch_df: DataFrame, batch_id: int) -> None:  # Defines what Spark does for each new batch.
            process_batch(batch_df, batch_id, args.output)  # Cleans the batch and saves its output.

        query = events.writeStream.foreachBatch(handle_batch)  # Connects each input batch to the processing function.
        query = query.option("checkpointLocation", str(args.checkpoint))  # Saves progress so a restart can recover.

        if args.once:  # Checks whether the user requested the staged demonstration mode.
            query.trigger(availableNow=True).start().awaitTermination()  # Processes available files once and exits.
        else:  # Uses continuous streaming when --once was not provided.
            query.trigger(processingTime="5 seconds").start().awaitTermination()  # Checks for new files every five seconds.
    finally:  # Runs whether processing succeeds or raises an error.
        spark.stop()  # Shuts down Spark cleanly.


if __name__ == "__main__":  # Checks that this file was run directly.
    main()  # Starts the application.
