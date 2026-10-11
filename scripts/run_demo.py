"""Run a staged arrival, stop, and checkpoint-recovery demonstration."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = PROJECT_ROOT / "input"


def run_stage(stage: str, source: Path, output: Path, checkpoint: Path, log_path: Path) -> None:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "app.py"),
        "--source",
        str(source),
        "--output",
        str(output),
        "--checkpoint",
        str(checkpoint),
        "--once",
    ]
    print(f"\n=== {stage} ===", flush=True)
    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["PYSPARK_DRIVER_PYTHON"] = sys.executable
    java_home = environment.get("JAVA_HOME")
    if java_home:
        environment["PATH"] = str(Path(java_home) / "bin") + os.pathsep + environment.get("PATH", "")

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\n=== {stage} ===\n{result.stdout}")
    print(result.stdout, end="")
    if result.returncode != 0:
        raise RuntimeError(f"{stage} failed with exit code {result.returncode}; see {log_path}")


def copy_arrival(day: str, source: Path) -> None:
    target = source / f"{day}.csv"
    shutil.copy2(SAMPLE_DIR / day / "events.csv", target)
    print(f"Arrived: {target.name}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=PROJECT_ROOT / f"demo_workspace_{datetime.now():%Y%m%d_%H%M%S}",
        help="Fresh directory for staged input, output, checkpoint, and evidence",
    )
    parser.add_argument(
        "--java-home",
        type=Path,
        default=os.environ.get("JAVA_HOME"),
        help="Java 17+ home directory (defaults to JAVA_HOME)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.java_home:
        os.environ["JAVA_HOME"] = str(args.java_home.resolve())
    work_dir = args.work_dir.resolve()
    if work_dir.exists() and any(work_dir.iterdir()):
        raise FileExistsError(f"Work directory is not empty: {work_dir}")

    source = work_dir / "incoming"
    output = work_dir / "output"
    checkpoint = work_dir / "checkpoint"
    evidence = work_dir / "evidence"
    source.mkdir(parents=True, exist_ok=True)
    evidence.mkdir(parents=True, exist_ok=True)
    log_path = evidence / "demo_log.txt"

    print(f"Demo workspace: {work_dir}", flush=True)
    copy_arrival("day_01", source)
    run_stage("Start application; process Day 01", source, output, checkpoint, log_path)

    copy_arrival("day_02", source)
    run_stage("Day 02 arrives; process incrementally", source, output, checkpoint, log_path)

    print("\nApplication is stopped. Day 03 arrives while it is offline.", flush=True)
    copy_arrival("day_03", source)
    run_stage("Restart with the same checkpoint; recover and process Day 03", source, output, checkpoint, log_path)

    copy_arrival("day_04_incompatible", source)
    run_stage("Incompatible event_time type; quarantine malformed record", source, output, checkpoint, log_path)

    print(f"\nEvidence log: {log_path}")
    print(f"Parquet output: {output}")
    print(f"Checkpoint: {checkpoint}")
    if os.name == "nt":
        print("The application stopped cleanly after each available-now run.")


if __name__ == "__main__":
    main()
