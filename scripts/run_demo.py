"""Run a staged arrival, stop, and checkpoint-recovery demonstration."""  # States what the helper script demonstrates.

from __future__ import annotations  # Enables modern type-hint behavior.

import argparse  # Reads optional settings supplied by the user.
import os  # Reads environment settings and detects Windows.
import shutil  # Copies each sample CSV into the streaming input folder.
import subprocess  # Starts the Spark application for each demonstration stage.
import sys  # Finds the current Python and returns error codes.
from datetime import datetime  # Creates a unique default demo folder name.
from pathlib import Path  # Handles file and folder paths safely.

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # Finds the main project folder from this script's location.
SAMPLE_DIR = PROJECT_ROOT / "input"  # Points to the folder containing the example CSV files.


def run_stage(stage: str, source: Path, output: Path, checkpoint: Path, log_path: Path) -> None:  # Runs one stream stage.
    command = [  # Starts the command used to launch the streaming program.
        sys.executable,  # Uses the same Python interpreter as this demo script.
        str(PROJECT_ROOT / "app.py"),  # Runs the project's Spark application.
        "--source",  # Tells the application its next argument is the input folder.
        str(source),  # Supplies the folder where sample files arrive.
        "--output",  # Tells the application its next argument is the output folder.
        str(output),  # Supplies the folder where results should be written.
        "--checkpoint",  # Tells the application its next argument is the checkpoint folder.
        str(checkpoint),  # Reuses this folder so Spark can recover its progress.
        "--once",  # Stops after processing all files currently available.
    ]  # Finishes building the application command.
    print(f"\n=== {stage} ===", flush=True)  # Announces which part of the demo is running.
    environment = os.environ.copy()  # Copies settings so the child Spark process inherits them.
    environment["PYSPARK_PYTHON"] = sys.executable  # Tells Spark which Python to use for worker processes.
    environment["PYSPARK_DRIVER_PYTHON"] = sys.executable  # Tells PySpark which Python to use for the driver.
    java_home = environment.get("JAVA_HOME")  # Reads the Java installation path if it was configured.
    if java_home:  # Adds Java to PATH only when JAVA_HOME has a value.
        environment["PATH"] = str(Path(java_home) / "bin") + os.pathsep + environment.get("PATH", "")  # Helps Spark find Java.

    result = subprocess.run(  # Starts Spark and waits for this stage to finish.
        command,  # Runs the command assembled above.
        cwd=PROJECT_ROOT,  # Runs from the project folder so relative paths work.
        env=environment,  # Passes the Python and Java settings into the Spark process.
        text=True,  # Captures the program output as readable text.
        encoding="utf-8",  # Reads output using UTF-8 encoding.
        errors="replace",  # Replaces any characters that cannot be decoded.
        stdout=subprocess.PIPE,  # Captures normal output for the demo log.
        stderr=subprocess.STDOUT,  # Combines error messages with normal output.
        check=False,  # Lets this script print output before handling a failure.
    )  # Finishes and returns the completed process result.
    with log_path.open("a", encoding="utf-8") as log:  # Opens the evidence log to add this stage's output.
        log.write(f"\n=== {stage} ===\n{result.stdout}")  # Saves the stage name and Spark output for evidence.
    print(result.stdout, end="")  # Shows Spark output in the terminal.
    if result.returncode != 0:  # Checks whether Spark reported a failure.
        raise RuntimeError(f"{stage} failed with exit code {result.returncode}; see {log_path}")  # Reports failure and log location.


def copy_arrival(day: str, source: Path) -> None:  # Copies one sample day into the watched folder.
    target = source / f"{day}.csv"  # Builds a unique CSV name in the incoming folder.
    shutil.copy2(SAMPLE_DIR / day / "events.csv", target)  # Copies the chosen day's sample file.
    print(f"Arrived: {target.name}", flush=True)  # Confirms the new file arrived.


def parse_args() -> argparse.Namespace:  # Reads settings for the demonstration.
    parser = argparse.ArgumentParser(description=__doc__)  # Creates a parser using this script's description.
    parser.add_argument(  # Adds a setting for where demo data and results are saved.
        "--work-dir",  # Names the optional work-folder setting.
        type=Path,  # Converts the supplied text into a path.
        default=PROJECT_ROOT / f"demo_workspace_{datetime.now():%Y%m%d_%H%M%S}",  # Uses a new timestamped folder by default.
        help="Fresh directory for staged input, output, checkpoint, and evidence",  # Explains this setting.
    )  # Finishes adding the work-folder setting.
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
    work_dir = args.work_dir.resolve()  # Converts the work folder to an absolute path.
    if work_dir.exists() and any(work_dir.iterdir()):  # Avoids overwriting files in a non-empty folder.
        raise FileExistsError(f"Work directory is not empty: {work_dir}")  # Tells the user to choose a fresh folder.

    source = work_dir / "incoming"  # Creates the folder that Spark watches for CSV files.
    output = work_dir / "output"  # Creates the folder where Spark writes results.
    checkpoint = work_dir / "checkpoint"  # Creates the folder where Spark remembers processed files.
    evidence = work_dir / "evidence"  # Creates the folder for the demo log.
    source.mkdir(parents=True, exist_ok=True)  # Creates the input folder and any missing parent folders.
    evidence.mkdir(parents=True, exist_ok=True)  # Creates the evidence folder and any missing parent folders.
    log_path = evidence / "demo_log.txt"  # Chooses the file that records each Spark run.

    print(f"Demo workspace: {work_dir}", flush=True)  # Shows where output and evidence will be saved.
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
