from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def _locate_dataset_dir(output_dir: Path) -> Path:
    if (output_dir / "files_metadata.csv").exists():
        return output_dir
    candidates = sorted(
        path
        for path in output_dir.iterdir()
        if path.is_dir() and (path / "files_metadata.csv").exists()
    )
    if len(candidates) == 1:
        return candidates[0]
    raise FileNotFoundError(
        f"Could not find a unique dataset directory with files_metadata.csv under {output_dir}"
    )


def validate_output_dir(
    output_dir: str | Path,
    *,
    expected_dataset: str | None = None,
    required_artifacts: Iterable[str] | None = None,
    allow_empty_files: bool = False,
) -> dict[str, Any]:
    """Validate a previously generated TCGA-Tools output directory.

    Parameters
    ----------
    output_dir:
        Root output directory or a specific per-dataset subdirectory.
    expected_dataset:
        Optional TCGA project identifier expected in the metadata columns.
    required_artifacts:
        Optional artifact filenames expected alongside ``files_metadata.csv``.
    allow_empty_files:
        If ``False``, the validator requires at least one file row.

    Returns
    -------
    dict[str, Any]
        JSON-serializable validation summary containing row counts and artifact paths.
    """
    root = Path(output_dir)
    assert root.exists(), f"Output directory does not exist: {root}"

    dataset_dir = _locate_dataset_dir(root)
    files_csv = dataset_dir / "files_metadata.csv"
    files_df = pd.read_csv(files_csv)
    run_log_path = root / "run_log.json"
    run_log = None
    if run_log_path.exists():
        run_log = json.loads(run_log_path.read_text())
    source = (run_log or {}).get("source", "gdc")
    if {"repo_path", "local_path"}.issubset(files_df.columns):
        source = "patho-bench"

    required_columns = (
        {"patient"} if source != "patho-bench" else {"repo_path", "local_path"}
    )
    missing_columns = sorted(required_columns.difference(files_df.columns))
    assert not missing_columns, (
        f"{files_csv} is missing required columns: {missing_columns}"
    )
    if not allow_empty_files:
        assert not files_df.empty, f"{files_csv} is empty"

    if (
        expected_dataset
        and "cases.project.project_id" in files_df.columns
        and not files_df.empty
    ):
        observed = set(
            files_df["cases.project.project_id"].dropna().astype(str).unique()
        )
        assert observed == {expected_dataset}, (
            f"Expected dataset {expected_dataset}, observed {sorted(observed)} in {files_csv}"
        )

    artifact_names = [
        "groups.csv",
        "clinical.csv",
        "diagnosis.csv",
        "molecular_index.csv",
        "patient_studies.csv",
        "raw_data_manifest.json",
        "reports_index.csv",
    ]
    if required_artifacts:
        artifact_names.extend(required_artifacts)

    artifacts = {}
    for name in sorted(set(artifact_names)):
        path = dataset_dir / name
        artifacts[name] = str(path) if path.exists() else None

    return {
        "output_dir": str(root),
        "dataset_dir": str(dataset_dir),
        "files_csv": str(files_csv),
        "file_rows": int(len(files_df)),
        "patient_count": int(files_df["patient"].dropna().nunique())
        if "patient" in files_df.columns
        else None,
        "columns": list(files_df.columns),
        "artifacts": artifacts,
        "run_log_present": run_log is not None,
        "run_log": run_log,
    }
