"""Run a staged arrival, stop, and checkpoint-recovery demonstration."""  # States what the helper script demonstrates.

from __future__ import annotations  # Enables modern type-hint behavior.

import argparse                      # Reads optional settings supplied by the user.
import filecmp                        # Detects whether a sample file was already staged.
import os                             # Reads environment settings and detects Windows.
import shutil                     # Copies each sample CSV into the streaming input folder.
import subprocess                 # Starts the Spark application for each demonstration stage.
import sys                        # Finds the current Python and returns error codes.
from datetime import datetime      # Gives each staged CSV a unique name.
from pathlib import Path             # Handles file and folder paths safely.

import pyspark                      # Locates the Spark distribution for Windows launchers.

PROJECT_ROOT = Path(__file__).resolve().parents[1]           # Finds the main project folder from this script's location.
SAMPLE_DIR = PROJECT_ROOT / "input"                               # Points to the folder containing the example CSV files.
DEMO_DIR = PROJECT_ROOT / "demo_workspace"                  # Reuses one workspace instead of making a folder per run.


def run_stage(stage: str, source: Path, output: Path, checkpoint: Path, log_path: Path) -> None:       # Runs one stream stage.
    log_path.parent.mkdir(parents=True, exist_ok=True)                                  # Ensures the evidence folder exists before writing logs.
    command = [                      # Starts the command used to launch the streaming program.
        sys.executable,                    # Uses the same Python interpreter as this demo script.
        str(PROJECT_ROOT / "app.py"),            # Runs the project's Spark application.
        "--source",                             # Tells the application its next argument is the input folder.
        str(source),                          # Supplies the folder where sample files arrive.
        "--output",                            # Tells the application its next argument is the output folder.
        str(output),                          #  Supplies the folder where results should be written.
        "--checkpoint",                          # Tells the application its next argument is the checkpoint folder.
        str(checkpoint),                          # Reuses this folder so Spark can recover its progress.
        "--once",                                # Stops after processing all files currently available.
    ]                                           # Finishes building the application command.
    print(f"\n=== {stage} ===", flush=True)                    # Announces which part of the demo is running.
    environment = os.environ.copy()                                # Copies settings so the child Spark process inherits them.
    environment["PATH"] = str(Path(sys.executable).resolve().parent) + os.pathsep + environment.get("PATH", "")
    environment["PYSPARK_PYTHON"] = "python"  # Resolves the selected interpreter through PATH without spaces.
    environment["PYSPARK_DRIVER_PYTHON"] = "python"
    environment["SPARK_HOME"] = str(Path(pyspark.__file__).resolve().parent)  # Avoids Windows path lookup with Python paths containing spaces.
    java_home = environment.get("JAVA_HOME")                       # Reads the Java installation path if it was configured.
    if java_home:                                                 # Adds Java to PATH only when JAVA_HOME has a value.
        environment["PATH"] = str(Path(java_home) / "bin") + os.pathsep + environment.get("PATH", "")    # Helps Spark find Java.

    result = subprocess.run(                         # Starts Spark and waits for this stage to finish.
        command,                                  # Runs the command assembled above.
        cwd=PROJECT_ROOT,                            # Runs from the project folder so relative paths work.
        env=environment,                             # Passes the Python and Java settings into the Spark process.
        text=True,                                     # Captures the program output as readable text.
        encoding="utf-8",                              # Reads output using UTF-8 encoding.
        errors="replace",                             # Replaces any characters that cannot be decoded.
        stdout=subprocess.PIPE,                       # Captures normal output for the demo log.
        stderr=subprocess.STDOUT,                      # Combines error messages with normal output.
        check=False,                              # Lets this script print output before handling a failure.
    )                                                     # Finishes and returns the completed process result.
    with log_path.open("a", encoding="utf-8") as log:                 # Opens the evidence log to add this stage's output.
        log.write(f"\n=== {stage} ===\n{result.stdout}")                  # Saves the stage name and Spark output for evidence.
    print(result.stdout, end="")                                    # Shows Spark output in the terminal.
    if result.returncode != 0:                                       # Checks whether Spark reported a failure.
        raise RuntimeError(f"{stage} failed with exit code {result.returncode}; see {log_path}")      # Reports failure and log location.


def copy_arrival(day: str, source: Path) -> None:                         # Copies a sample only when it is not already staged.
    sample = SAMPLE_DIR / day / "events.csv"                              # Locates the sample file for this stage.
    if any(filecmp.cmp(sample, existing, shallow=False) for existing in source.glob("*.csv")):
        print(f"Already staged: {day}.csv", flush=True)
        return
    target = source / f"{day}.csv"                                        # Uses a stable filename across demo runs.
    if target.exists():
        target = source / f"{datetime.now():%Y%m%d_%H%M%S_%f}_{day}.csv"  # Keeps changed versions as new stream inputs.
    shutil.copy2(sample, target)                                           # Copies the chosen day's sample file.
    print(f"Arrived: {target.name}", flush=True)                              # Confirms the new file arrived.


def parse_args() -> argparse.Namespace:                            # Reads settings for the demonstration.
    parser = argparse.ArgumentParser(description=__doc__)               # Creates a parser using this script's description.
    parser.add_argument(  # Adds a setting for the Java installation.
        "--java-home",  # Names the optional Java-folder setting.
        type=Path,  # Converts the supplied text into a path.
        default=os.environ.get("JAVA_HOME"),  # Uses JAVA_HOME when the user did not supply this option.
        help="Java 17+ home directory (defaults to JAVA_HOME)",  # Explains the supported Java setting.
    )  # Finishes adding the Java-folder setting.
    return parser.parse_args()  # Returns the settings entered by the user.


def main() -> None:  # Runs all the demonstration stages in order.
    args = parse_args()  # Reads the output-folder and Java settings.
    if args.java_home:  # Checks whether a Java installation was selected.
        os.environ["JAVA_HOME"] = str(args.java_home.resolve())  # Makes the selected Java path visible to Spark.
    if not os.environ.get("JAVA_HOME"):
        raise RuntimeError("JAVA_HOME is not set. Install a JDK 17+ and rerun the demo.")
    source = DEMO_DIR / "incoming"  # Reuses the folder that Spark watches for CSV files.
    output = DEMO_DIR / "output"  # Reuses the folder where Spark writes results.
    checkpoint = DEMO_DIR / "checkpoint"  # Reuses Spark's progress between demo runs.
    evidence = DEMO_DIR / "evidence"  # Reuses the folder for the demo log.
    source.mkdir(parents=True, exist_ok=True)  # Creates the input folder and any missing parent folders.
    evidence.mkdir(parents=True, exist_ok=True)  # Creates the evidence folder and any missing parent folders.
    log_path = evidence / "demo_log.txt"  # Chooses the file that records each Spark run.
    print(f"Demo workspace: {DEMO_DIR}", flush=True)  # Shows the persistent output and checkpoint location.
    copy_arrival("day_01", source)  # Adds the original five-column input file.
    run_stage("Start application; process Day 01", source, output, checkpoint, log_path)  # Processes the first day's file.

    copy_arrival("day_02", source)  # Adds a file containing the two new columns.
    run_stage("Day 02 arrives; process incrementally", source, output, checkpoint, log_path)  # Processes only newly arrived input.

    print("\nApplication is stopped. Day 03 arrives while it is offline.", flush=True)  # Shows the planned restart scenario.
    copy_arrival("day_03", source)  # Adds Day 03 while the previous Spark run is stopped.
    run_stage("Restart with the same checkpoint; recover and process Day 03", source, output, checkpoint, log_path)  # Demonstrates recovery.

    copy_arrival("day_04_incompatible", source)  # Adds the sample containing an invalid timestamp.
    run_stage("Incompatible event_time type; quarantine malformed record", source, output, checkpoint, log_path)  # Demonstrates bad-row handling.

    print(f"\nEvidence log: {log_path}")  # Shows where to find the captured batch messages.
    print(f"Parquet output: {output}")  # Shows where to find events, quarantine, and statistics.
    print(f"Checkpoint: {checkpoint}")  # Shows where Spark saved its restart progress.
    if os.name == "nt":  # Checks whether the demo is running on Windows.
        print("The application stopped cleanly after each available-now run.")  # Explains that each demo stage has ended.


if __name__ == "__main__":  # Checks that this file was launched directly.
    main()  # Starts the complete demonstration.
