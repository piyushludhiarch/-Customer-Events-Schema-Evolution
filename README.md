# Customer Events Schema Evolution

## 1. Project overview

This project demonstrates a PySpark Structured Streaming pipeline for customer
event CSV files. The input format changes after a software release: earlier
files have five columns, while later files also include `browser` and
`app_version`.

The application reads newly arriving CSV files, validates their records,
categorizes valid events, and writes three kinds of Parquet output:

- Valid events, including their event category.
- Invalid or malformed records, kept separately for review.
- Counts grouped by event category and device.

Spark checkpointing records which input files have been processed. The demo
stops and restarts the application with the same checkpoint to show recovery.

## 2. Folder guide

```text
project/
|-- .git/                         Git repository metadata
|-- .venv/                        Local Python environment and installed packages
|-- input/
|   |-- day_01/events.csv         Original five-column event data
|   |-- day_02/events.csv         Event data with browser and app_version
|   |-- day_03/events.csv         More evolved-schema events, including bad rows
|   `-- day_04_incompatible/
|       `-- events.csv            Incompatible event_time example
|-- scripts/
|   `-- run_demo.py               Runs the staged end-to-end demonstration
|-- submission/
|   |-- evidence/
|   |   `-- demo_log.txt          Verified run and output checks
|   `-- output/
|       |-- events/               Checked-in valid-event Parquet batches
|       |-- quarantine/           Checked-in rejected-record Parquet batches
|       `-- statistics/           Checked-in category/device count batches
|-- tests/
|   `-- test_event_categories.py  Tests the event-category function
|-- .gitignore                    Excludes local/generated files from Git
|-- README.md                     This project and usage guide
|-- app.py                        Streaming application and transformations
`-- requirements.txt              Python package dependencies
```

### Folders

- **`.git/`** — Git's internal repository metadata. It is managed by Git and
  should not be edited or deleted manually.
- **`.venv/`** — the local virtual environment. It contains the Python
  interpreter and installed packages for this project, including PySpark. It
  is machine-specific, ignored by Git, and can be recreated from
  `requirements.txt`.
- **`input/`** — the sample CSV files used by the demo. Each day is in its own
  subfolder to keep the original sample files organized. `run_demo.py` stages
  each day's CSV in the persistent demo workspace as it arrives.
- **`input/day_01/`** — legacy data with the original five columns.
- **`input/day_02/`** — data after the schema change, with `browser` and
  `app_version`.
- **`input/day_03/`** — evolved-schema data that includes valid events and
  records with missing or invalid values.
- **`input/day_04_incompatible/`** — a separate incompatible-type example.
  Its `event_time` is numeric instead of the timestamp format expected by the
  application.
- **`scripts/`** — helper scripts for running the staged demonstration.
- **`tests/`** — automated tests for event categorization.
- **`submission/evidence/`** — a saved text log from a recorded run. It
  summarizes the verified batches, checkpoint recovery, output schema, event
  counts, and quarantine reasons.
- **`submission/output/events/`** — checked-in Parquet data for valid events,
  partitioned into `batch_id=...` folders.
- **`submission/output/quarantine/`** — checked-in Parquet data for rejected
  records, also partitioned by batch.
- **`submission/output/statistics/`** — checked-in Parquet counts grouped by
  category and device, partitioned by batch.

When you run the demo, it reuses the fixed `demo_workspace/` folder in the
project root. It creates that folder only if it does not already exist. It
contains:

- `incoming/` — copies of sample files as they arrive during the demo.
- `output/events/` — clean, categorized events from the current run.
- `output/quarantine/` — rows that failed validation.
- `output/statistics/` — counts for each category/device combination.
- `checkpoint/` — Spark's progress information, needed to resume that run.
- `evidence/demo_log.txt` — terminal output captured for that run.

The checkpoint, outputs, and log remain in this folder between runs. The demo
reuses stable sample filenames and skips a sample already staged, so rerunning
it does not duplicate the same records. Do not confuse this generated folder
with `submission/`, which contains the checked-in example output.

## 3. File guide

- **`app.py`** — the main application. It defines the input schema and
  required fields, contains the event-category UDF, reads streaming CSV files,
  validates each micro-batch, and writes event, quarantine, and statistics
  Parquet files. It also configures checkpointing and supports both continuous
  processing and one-time processing with `--once`.
- **`scripts/run_demo.py`** — the demonstration driver. It reuses
  `demo_workspace/`, copies the four sample CSVs into the watched folder in
  stages only if they have not already been staged, starts the app for each
  stage, reuses the same checkpoint, and saves/prints a log. It accepts
  `--java-home`.
- **`requirements.txt`** — pins the Python dependency to `pyspark==4.2.0`.
- **`tests/test_event_categories.py`** — uses Python's built-in `unittest`
  framework to test event-to-category mappings, case/space handling, and
  missing or unknown event types.
- **`input/day_01/events.csv`** — sample rows with
  `event_id,user_id,event_type,event_time,device`.
- **`input/day_02/events.csv`** — sample rows with the original columns plus
  `browser,app_version`.
- **`input/day_03/events.csv`** — evolved-schema examples with valid events, an
  invalid timestamp, and a missing event ID.
- **`input/day_04_incompatible/events.csv`** — a row whose `event_time` is an
  integer epoch-like value rather than the configured timestamp text format.
- **`submission/evidence/demo_log.txt`** — a human-readable record of a prior
  demo, with batch IDs and valid/quarantined record counts. It is evidence of
  that recorded run, not a live log.
- **`submission/output/events/batch_id=.../part-*.snappy.parquet`** — the
  columnar data files for valid events. They include the source fields and
  `event_category`.
- **`submission/output/quarantine/batch_id=.../part-*.snappy.parquet`** — the
  columnar data files for rejected records, retaining available input fields
  and a `quarantine_reason` value for inspection.
- **`submission/output/statistics/batch_id=.../part-*.snappy.parquet`** — the
  columnar data files with `event_category`, `device`, `count`, and `batch_id`.
- **`.gitignore`** — excludes Python caches and generated run folders from Git
  while allowing the `submission/output/` files to be tracked.
- **`.venv/` files** — local interpreter, installed PySpark and its
  dependencies. These are created by Python/pip and are not committed as
  project source.
- **`.git/` files** — repository internals managed by Git; they are not
  application input or output.

Parquet output uses Spark-generated part filenames. Their exact names can
change between runs; the `batch_id` folders and the columns/records inside
them are what identify the batch and its data.

## 4. How the full project works

### Input and schema evolution

The assignment describes an incoming location called `/event_source/`. In this
project, the sample inputs are stored under `input/`. The application accepts
the watched input directory as a command-line argument, so another directory
can be supplied when running it directly.

Day 01 has five fields:

```text
event_id,user_id,event_type,event_time,device
```

Day 02 and later use the extended schema:

```text
event_id,user_id,event_type,event_time,device,browser,app_version
```

`app.py` declares one schema containing both versions' columns. Spark reads
older rows with `browser` and `app_version` as null, and reads the added fields
from newer files. The five-column Day 01 header can cause Spark's
`CSVHeaderChecker` to report a header-width warning against the seven-column
schema. The saved output has been checked: the original fields remain aligned,
the two later fields are null for Day 01, and Day 02's browser/version values
are preserved.

### Validation, categorization, and outputs

For each batch, the application:

1. Uses the declared CSV schema and configured timestamp format to parse rows.
2. Checks that required fields (`event_id`, `user_id`, `event_type`,
   `event_time`, and `device`) are present and non-blank.
3. Applies the `categorize_event` Python UDF to assign a business category:
   `SESSION`, `DISCOVERY`, `TRANSACTION`, or `OTHER`.
4. Separates valid records from invalid or malformed records.
5. Writes valid events, quarantined records, and counts grouped by
   `event_category` and `device` to batch-specific Parquet folders.
6. Prints a `BATCH_PROCESSED` summary with the batch ID and record counts.

Quarantined rows also include a `quarantine_reason` column. It identifies
unparseable event timestamps, other malformed CSV rows, and missing or blank
required fields. In the Day 03 sample, `E010` is quarantined because its
`event_time` is `not-a-timestamp`; the following row is quarantined because
its `event_id` is blank.

### Incremental processing and recovery

Spark's file-streaming source discovers files in the watched directory. The
checkpoint records progress so previously processed files are not needlessly
reprocessed. `run_demo.py` demonstrates this by processing Day 01, then Day 02,
stopping the application, and restarting with the same checkpoint when Day 03
arrives. It then supplies Day 04 to show an incompatible `event_time` value
being quarantined.

The demo uses `availableNow` mode (`--once`) for each stage: each app process
handles files currently available and exits. The normal app mode, without
`--once`, checks for new files every five seconds and continues running until
stopped.

## 5. Requirements and setup

The project needs:

- Python 3.12 (the configured project environment uses Python 3.12).
- PySpark 4.2.0, installed from `requirements.txt`.
- A Java installation compatible with the installed Spark version.

In PowerShell, open the project folder. Create `.venv` only if it does not
already exist:

```powershell
cd "C:\Users\PIYUSH\Downloads\new project"
py -3.12 -m venv .venv
```

Install or refresh the Python dependency:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Set `JAVA_HOME` to the JDK folder on your computer. This example is the Java
25 installation used for the recent local demo; change it if your JDK is
installed elsewhere:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-25.0.4'
$env:SPARK_LOCAL_IP = '127.0.0.1'
```

## 6. Run the staged demonstration

From the project root in PowerShell:

```powershell
.\.venv\Scripts\python.exe scripts\run_demo.py --java-home $env:JAVA_HOME
```

The script prints each stage's Spark output in the terminal. All runs reuse
`demo_workspace/` and its output, log, and checkpoint, so it does not create a
new workspace folder each time.

The app's `--source`, `--output`, and `--checkpoint` paths remain separate
because Spark needs to know where input is watched, where results are written,
and where processing progress is saved.

## 7. Run the application directly

The application requires separate source, output, and checkpoint locations.
For example, create an incoming folder and copy CSV files into it, then start
continuous processing:

```powershell
New-Item -ItemType Directory -Force -Path ".\event_source" | Out-Null
.\.venv\Scripts\python.exe .\app.py `
  --source ".\event_source" `
  --output ".\output" `
  --checkpoint ".\checkpoint"
```

Add new CSV files to `event_source` while the application is running. It scans
for newly discovered files and processes them in micro-batches. Stop it with
`Ctrl+C`. Keep the same checkpoint directory when restarting the same stream;
do not reuse a checkpoint for a different source or an unrelated run.

To process only currently available files and exit, add `--once`:

```powershell
.\.venv\Scripts\python.exe .\app.py `
  --source ".\event_source" `
  --output ".\output" `
  --checkpoint ".\checkpoint" `
  --once
```

## 8. Run the tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## 9. Expected demo evidence

A successful staged run prints four `BATCH_PROCESSED` messages with increasing
batch IDs. The example data has:

| Batch | Valid records | Quarantined records | Demonstration |
|---:|---:|---:|---|
| 0 | 3 | 0 | Day 01, original schema |
| 1 | 3 | 0 | Day 02, extended schema |
| 2 | 3 | 2 | Day 03, restart/recovery and invalid records |
| 3 | 0 | 1 | Day 04, incompatible `event_time` |

The event Parquet output contains the category field. The statistics output
contains counts grouped by category and device. The quarantine output retains
rejected records for review. Batch counts and the demo log are useful run
evidence; inspect the Parquet data itself when validating fields or exact
statistics.
