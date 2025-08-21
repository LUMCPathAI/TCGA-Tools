from __future__ import annotations
from typing import Iterable

import pandas as pd

from .api import GDCClient
from .filters import Filters as F
from .config import DEFAULT_CASE_FIELDS, CLINICAL_FIELDS, DIAGNOSIS_FIELDS, MOLECULAR_CATEGORIES, REPORT_DATA_TYPES
from .utils import flatten_hits


def build_clinical_csv(client: GDCClient, case_ids: Iterable[str]) -> pd.DataFrame:
    """Clinical/survival/outcome/treatment signals from the *cases* endpoint.

    Best-effort: requests a broad union of fields; if the API rejects some, the
    client retries without explicit fields and we still flatten whatever we get.
    """
    case_filters = F.IN("cases.case_id", list(case_ids))
    fields = list(dict.fromkeys([*DEFAULT_CASE_FIELDS, *CLINICAL_FIELDS]))
    hits = client.cases_query(case_filters, fields)
    return flatten_hits(hits)


def build_diagnosis_csv(client: GDCClient, case_ids: Iterable[str]) -> pd.DataFrame:
    """Diagnosis/subtyping details from *cases* endpoint (diagnoses.*)."""
    case_filters = F.IN("cases.case_id", list(case_ids))
    fields = list(dict.fromkeys([*DEFAULT_CASE_FIELDS, *DIAGNOSIS_FIELDS]))
    hits = client.cases_query(case_filters, fields)
    return flatten_hits(hits)


def build_molecular_index(client: GDCClient, project_id: str, case_ids: Iterable[str]) -> pd.DataFrame:
    """Index of available molecular files (DNA/RNA/CNV/Methylation) from *files* endpoint."""
    filters = F.AND(
        F.EQ("cases.project.project_id", project_id),
        F.IN("data_category", MOLECULAR_CATEGORIES),
        F.IN("cases.case_id", list(case_ids)),
    )
    fields = [
        "id",
        "file_name",
        "data_category",
        "data_type",
        "data_format",
        "experimental_strategy",
        "cases.case_id",
        "cases.submitter_id",
        "cases.samples.sample_type",
        "cases.samples.sample_id",
    ]
    hits = client.paged_query("files", filters, fields)
    return flatten_hits(hits)


def build_reports_index(client: GDCClient, project_id: str, case_ids: Iterable[str]) -> pd.DataFrame:
    """Index of free-text reports for each case (clinical/pathology PDFs/XML)."""
    filters = F.AND(
        F.EQ("cases.project.project_id", project_id),
        F.IN("data_category", ["Clinical"]),
        F.IN("data_type", REPORT_DATA_TYPES),
        F.IN("cases.case_id", list(case_ids)),
    )
    fields = [
        "id",
        "file_name",
        "data_category",
        "data_type",
        "data_format",
        "cases.case_id",
        "cases.submitter_id",
    ]
    hits = client.paged_query("files", filters, fields)
    return flatten_hits(hits)