# tcga_tools/datasets.py
from __future__ import annotations
from typing import Iterable, Optional

import pandas as pd

from .api import GDCClient
from .filters import Filters as F
from .patho_bench import list_patho_bench_datasets, list_patho_bench_tasks
from .tcia import list_tcia_data_types, list_tcia_datasets
from .utils import flatten_hits


def list_datasets(
    program: str = "TCGA",
    fields: Optional[Iterable[str]] = None,
    as_dataframe: bool = True,
    *,
    source: str = "gdc",
    include_data_types: bool = False,
):
    """
    List available datasets from GDC, TCIA, or Patho-Bench.

    Returns
    -------
    pd.DataFrame | list[dict]
        For ``source="gdc"``, returns a DataFrame with metadata columns:
        ['project_id', 'name', 'disease_type', 'primary_site',
         'summary.case_count', 'summary.file_count'].
        For ``source="tcia"``, returns TCIA collection names and, optionally,
        per-collection modalities under ``data_types``.
        For ``source="patho-bench"``, returns benchmark datasets and, optionally,
        per-dataset task names under ``tasks``.
    """
    normalized_source = source.lower()
    if normalized_source == "tcia":
        return list_tcia_datasets(
            include_data_types=include_data_types,
            as_dataframe=as_dataframe,
        )
    if normalized_source == "patho-bench":
        return list_patho_bench_datasets(
            include_data_types=include_data_types,
            as_dataframe=as_dataframe,
        )
    if normalized_source != "gdc":
        raise ValueError(f"Unsupported source: {source}")

    client = GDCClient()
    filters = F.EQ("program.name", program)
    default_fields = [
        "project_id",
        "name",
        "disease_type",
        "primary_site",
        "summary.case_count",
        "summary.file_count",
    ]
    fld = list(fields) if fields else default_fields
    hits = client.paged_query("projects", filters, fld)
    df = flatten_hits(hits)
    return df if as_dataframe else df.to_dict(orient="records")


def list_data_types(
    dataset_name: str | None = None,
    *,
    source: str = "gdc",
    as_dataframe: bool = True,
):
    """List supported data-type values for a dataset source.

    For ``source="tcia"``, this lists TCIA modalities globally or for one
    collection. For ``source="patho-bench"``, this lists benchmark task names.
    For ``source="gdc"``, a dataset name is required and the function returns
    distinct file data_type/data_format combinations.
    """
    normalized_source = source.lower()
    if normalized_source == "tcia":
        return list_tcia_data_types(
            dataset_name=dataset_name,
            as_dataframe=as_dataframe,
        )
    if normalized_source == "patho-bench":
        return list_patho_bench_tasks(
            dataset_name=dataset_name,
            as_dataframe=as_dataframe,
        )
    if normalized_source != "gdc":
        raise ValueError(f"Unsupported source: {source}")
    if not dataset_name:
        raise ValueError("dataset_name is required for GDC data-type listing")

    client = GDCClient()
    filters = F.EQ("cases.project.project_id", dataset_name)
    hits = client.paged_query(
        "files",
        filters,
        ["data_type", "data_format", "experimental_strategy"],
    )
    df = flatten_hits(hits)
    if df.empty:
        out = pd.DataFrame(
            columns=[
                "dataset_name",
                "data_type",
                "data_format",
                "experimental_strategy",
                "source",
            ]
        )
    else:
        out = (
            df[["data_type", "data_format", "experimental_strategy"]]
            .drop_duplicates()
            .sort_values(["data_type", "data_format"], na_position="last")
            .assign(dataset_name=dataset_name, source="gdc")
        )
        out = out[
            [
                "dataset_name",
                "data_type",
                "data_format",
                "experimental_strategy",
                "source",
            ]
        ]
    return out if as_dataframe else out.to_dict(orient="records")
