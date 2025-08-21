#!/usr/bin/env python3
"""
Online smoke test against GDC for TCGA-LUSC.

This script performs:
1) A project summary check via tt.ListDatasets() to assert canonical counts
   (504 cases and 32,329 files for TCGA-LUSC).
2) A RAW run via tt.Download(..., filetypes=[".svs"], raw=True) and asserts
   the Diagnostic Slide subset matches expected counts (478 cases, 512 .svs files).

Usage:
    python online_test.py --out ./_lusc_raw_check

Requires internet access. For controlled-access files, set $GDC_TOKEN,
but this test only touches open metadata and .svs indices.
"""

import argparse
import sys
from pathlib import Path
import pandas as pd

import tcga_tools as tt


# --- Expected canonical metrics for TCGA-LUSC (from GDC portal) ---
PROJECT_ID = "TCGA-LUSC"
EXPECTED_PROJECT_CASES = 504
EXPECTED_PROJECT_FILES = 32329
EXPECTED_SVS_CASES = 478          # Experimental Strategy: Diagnostic Slide → Cases
EXPECTED_SVS_FILES = 512          # Experimental Strategy: Diagnostic Slide → Files


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"[OK] {msg}")


def check_project_counts() -> None:
    df = tt.ListDatasets(program="TCGA")
    row = df.loc[df["project_id"] == PROJECT_ID]
    if row.empty:
        fail(f"Project {PROJECT_ID} not found in ListDatasets() result.")

    cases = int(row["summary.case_count"].iloc[0])
    files = int(row["summary.file_count"].iloc[0])

    if cases != EXPECTED_PROJECT_CASES:
        fail(f"{PROJECT_ID}: expected {EXPECTED_PROJECT_CASES} cases, got {cases}")
    ok(f"{PROJECT_ID} cases == {EXPECTED_PROJECT_CASES}")

    if files != EXPECTED_PROJECT_FILES:
        fail(f"{PROJECT_ID}: expected {EXPECTED_PROJECT_FILES} files, got {files}")
    ok(f"{PROJECT_ID} files == {EXPECTED_PROJECT_FILES}")


def check_raw_svs(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = tt.Download(
        dataset_name=PROJECT_ID,
        filetypes=[".svs"],          # Diagnostic Slide
        annotations=["all"],
        output_dir=str(out_dir),
        raw=True,                    # metadata-only, do not download files
        statistics=True,             # also compute stats.json
        visualizations=False,
    )

    files_csv = Path(artifacts["files_csv"])
    if not files_csv.exists():
        fail("files_metadata.csv not created by raw run.")

    df = pd.read_csv(files_csv)
    n_files = len(df)
    n_cases = df["cases.case_id"].nunique() if "cases.case_id" in df.columns else 0

    if n_files != EXPECTED_SVS_FILES:
        fail(f".svs files: expected {EXPECTED_SVS_FILES}, got {n_files}")
    ok(f".svs files == {EXPECTED_SVS_FILES}")

    if n_cases != EXPECTED_SVS_CASES:
        fail(f".svs cases: expected {EXPECTED_SVS_CASES}, got {n_cases}")
    ok(f".svs cases == {EXPECTED_SVS_CASES}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="./_lusc_raw_check", help="Output directory for raw run artifacts")
    args = p.parse_args()
    out_dir = Path(args.out)

    print("== Checking project summary counts ==")
    check_project_counts()

    print("\n== Running RAW .svs check ==")
    check_raw_svs(out_dir)

    print("\nAll checks passed ✅")


if __name__ == "__main__":
    main()
