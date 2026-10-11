# Customer Events Streaming Project

## What this project does

Think of the application as a small conveyor belt:

1. A CSV file arrives in the input folder.
2. Spark reads each new file once.
3. The program adds an event category and checks that required values exist.
4. Good rows go to the events output; bad rows go to quarantine.
5. Spark saves counts by event category and device.
6. A checkpoint remembers which files Spark already processed.

Older files do not have `browser` or `app_version`; their values are kept as
empty (null). Newer files keep those values. The example includes one bad
timestamp and one missing event ID so the quarantine behavior is visible.

## Run the demonstration

You need Python, PySpark 4.2.0, and Java 17 or later. In PowerShell, from this
project folder:

```powershell
py -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:\path\to\your\jdk-17'
py scripts\run_demo.py
```

Replace the Java path with the location of your Java 17 installation. The demo
automatically runs these steps:

1. Process Day 01.
2. Add and process Day 02.
3. Stop Spark, add Day 03, and restart using the same checkpoint.
4. Add a deliberately bad timestamp and show that it is quarantined.

The output prints a `BATCH_PROCESSED` line for each step. Increasing batch IDs
show that Spark resumed from its checkpoint instead of starting over.

## Where things are

- `app.py` — the streaming program.
- `input/day_01` to `input/day_03` — the assignment's sample event files.
- `input/day_04_incompatible` — the example with an invalid timestamp.
- `scripts/run_demo.py` — runs the staged file arrivals and restart for you.
- `tests/` — small tests for event categories.
- `demo_workspace_*/output/events` — clean, categorized events.
- `demo_workspace_*/output/quarantine` — missing or invalid records.
- `demo_workspace_*/output/statistics` — counts by category and device.
- `demo_workspace_*/checkpoint` — Spark's saved progress; keep it when restarting.
- `demo_workspace_*/evidence/demo_log.txt` — example run evidence.
- `submission/output/` and `submission/evidence/` — checked-in output and evidence
  from a verified run. The runtime checkpoint remains local because Spark
  checkpoints contain machine-specific input paths; the evidence log records
  the same-checkpoint restart and incrementing batch IDs.

## Short explanation for your mentor

> “I used Spark Structured Streaming to watch a folder for new event CSVs. I
> defined one schema that includes both the original and new fields, so old
> records still work and the added fields are empty for them. A small UDF
> classifies events as SESSION, DISCOVERY, or TRANSACTION. The program saves
> valid events, quarantines incomplete or malformed rows, and writes counts by
> category and device. The checkpoint lets Spark continue with new files after
> a restart.”

## Run the tests

```powershell
py -m unittest discover -s tests -v
```
