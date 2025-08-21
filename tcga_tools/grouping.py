from __future__ import annotations
import pandas as pd
from .config import SAMPLE_TYPE_TO_GROUP


def build_patient_groups(files_df: pd.DataFrame) -> pd.DataFrame:
    
    print(files_df)
    """Return a groups table (one row per case) with basic sample-type grouping.

    Columns: case_id, submitter_id, project_id, has_tumor, has_normal, group
    """
    # Extract case- and sample-level columns if present
    cols = {
        "case_id": "cases.case_id",
        "submitter_id": "cases.submitter_id",
        "project_id": "cases.project.project_id",
        "sample_type": "cases.samples.sample_type",
    }
    sub = files_df[[v for v in cols.values() if v in files_df.columns]].copy()
    if not sub.shape[1]:
        return pd.DataFrame()
    sub = sub.rename(columns={v: k for k, v in cols.items() if v in files_df.columns})
    # There may be multiple rows per case; aggregate
    agg = (
        sub.assign(
            is_tumor=sub["sample_type"].map(lambda s: s in {"Primary Tumor", "Metastatic", "Recurrent Tumor"}),
            is_normal=sub["sample_type"].map(lambda s: s in {"Solid Tissue Normal", "Blood Derived Normal"}),
        )
        .groupby(["case_id", "submitter_id", "project_id"], dropna=False)
        .agg(has_tumor=("is_tumor", "max"), has_normal=("is_normal", "max"))
        .reset_index()
    )
    def label(row):
        if row.has_tumor and row.has_normal:
            return "paired"
        if row.has_tumor:
            return "tumor_only"
        if row.has_normal:
            return "normal_only"
        return "other"
    agg["group"] = agg.apply(label, axis=1)
    return agg
