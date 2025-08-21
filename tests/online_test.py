# tests/online_test.py
"""
Compares online raw retrieval of TCGA-LUSC with fixed dataset statistics.
"""
import os
from pathlib import Path

import pandas as pd
import pytest

import tcga_tools as tt

pytestmark = pytest.mark.online  # run with: pytest -m online -v

# --- Canonical metrics for TCGA-LUSC (from GDC portal snapshot) ---
PROJECT_ID = "TCGA-LUSC"
EXPECTED_PROJECT_CASES = 504
EXPECTED_PROJECT_FILES = 32329
EXPECTED_SVS_CASES = 478          # Diagnostic Slide → Cases
EXPECTED_SVS_FILES = 512          # Diagnostic Slide → Files


def _skip_if_offline():
    """Best-effort ping to GDC via a tiny call; skip if offline."""
    try:
        df = tt.ListDatasets(program="TCGA")
        assert isinstance(df, pd.DataFrame)
    except Exception as e:
        pytest.skip(f"GDC not reachable / offline: {e}")


def test_project_counts_lusc():
    _skip_if_offline()

    df = tt.ListDatasets(program="TCGA")
    row = df.loc[df["project_id"] == PROJECT_ID]
    assert not row.empty, f"Project {PROJECT_ID} not found in ListDatasets()"
    print("Successfully verified project presence in ListDatasets().")

    cases = int(row["summary.case_count"].iloc[0])
    files = int(row["summary.file_count"].iloc[0])

    assert cases == EXPECTED_PROJECT_CASES, f"Expected {EXPECTED_PROJECT_CASES} cases, got {cases}"
    assert files == EXPECTED_PROJECT_FILES, f"Expected {EXPECTED_PROJECT_FILES} files, got {files}"
    print("Successfully verified project case and file counts.")


def test_lusc_svs_counts_and_annotations(tmp_path: Path):
    _skip_if_offline()

    out_dir = tmp_path / "lusc_raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Raw run: discover metadata + annotations only; do not download data files
    artifacts = tt.Download(
        dataset_name=PROJECT_ID,
        datatype=["WSI"],
        annotations=["all"],
        output_dir=str(out_dir),
        raw=True,
        statistics=True,
        visualizations=False,
    )

    # ---- Verify files_metadata for LUSC .svs ----
    files_csv = Path(artifacts["files_csv"])
    assert files_csv.exists(), "files_metadata.csv not created by raw run."
    print("Successfully verified files_metadata.csv creation.")
    files_df = pd.read_csv(files_csv)
    print(files_df.head())

    # Keep only TCGA-LUSC rows (defensive in case multi-project sneaks in)
    if "cases.project.project_id" in files_df.columns:
        files_df = files_df[files_df["cases.project.project_id"] == PROJECT_ID]

    # Sanity: patient column exists for easy grouping
    assert "patient" in files_df.columns, "'patient' column missing in files_metadata.csv"
    print("Successfully verified 'patient' column presence.")

    n_files = len(files_df)
    n_cases = files_df["patient"].nunique()
    print("Found {} files of {} cases".format(n_files, n_cases))

    #Check for duplicates
    assert n_files == files_df["id"].nunique(), "Duplicate file IDs found"
    print("Successfully verified file IDs.")

    assert n_files == EXPECTED_SVS_FILES, f"Expected {EXPECTED_SVS_FILES} .svs files, got {n_files}"
    print("Successfully verified .svs file count.")
    assert n_cases == EXPECTED_SVS_CASES, f"Expected {EXPECTED_SVS_CASES} .svs cases, got {n_cases}"
    print("Successfully verified .svs case count.")
    
    # ---- Verify grouping output exists and has expected columns ----
    groups_csv = artifacts.get("groups_csv")
    assert groups_csv is not None and Path(groups_csv).exists(), "groups.csv missing"
    print("Successfully verified groups.csv creation.")
    groups_df = pd.read_csv(groups_csv)
    for col in ["case_id", "submitter_id", "project_id", "group"]:
        assert col in groups_df.columns, f"groups.csv missing column '{col}'"
    print("Successfully verified expected columns in groups.csv.")
    print(groups_df.head())

    # ---- Verify annotations exist and are non-empty (best-effort) ----
    clinical_csv = artifacts.get("clinical_csv")
    diagnosis_csv = artifacts.get("diagnosis_csv")
    molecular_csv = artifacts.get("molecular_csv")
    report_csv = artifacts.get("report_csv")

    assert clinical_csv is not None and Path(clinical_csv).exists(), "clinical.csv missing"
    print("Successfully verified clinical.csv creation.")
    assert diagnosis_csv is not None and Path(diagnosis_csv).exists(), "diagnosis.csv missing"
    print("Successfully verified diagnosis.csv creation.")

    # Clinical + Diagnosis should have at least one row for LUSC
    clinical_df = pd.read_csv(clinical_csv)
    diagnosis_df = pd.read_csv(diagnosis_csv)
    assert len(clinical_df) > 0, "clinical.csv is empty"
    print("Successfully verified clinical.csv is non-empty.")
    assert len(diagnosis_df) > 0, "diagnosis.csv is empty"
    print("Successfully verified diagnosis.csv is non-empty.")

    # Molecular and report indexes may vary by project; check presence if created
    if molecular_csv is not None and Path(molecular_csv).exists():
        mol_df = pd.read_csv(molecular_csv)
        print(mol_df)
        print(mol_df.columns)
        assert "cases.case_id" in mol_df.columns or "cases.submitter_id" in mol_df.columns, \
            "molecular_index.csv missing case linkage columns"
        print("Successfully verified molecular_index.csv case linkage columns.")
    if report_csv is not None and Path(report_csv).exists():
        rep_df = pd.read_csv(report_csv)
        assert "cases.case_id" in rep_df.columns or "cases.submitter_id" in rep_df.columns, \
            "reports_index.csv missing case linkage columns"
        print("Successfully verified reports_index.csv case linkage columns.")

    # ---- Stats file should exist when statistics=True ----
    stats_json = artifacts.get("stats_json")
    assert stats_json is not None and Path(stats_json).exists(), "stats.json missing"
    print("Successfully verified stats.json creation.")