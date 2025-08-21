# tcga_tools/datasets.py
from __future__ import annotations
from typing import Iterable, Optional

import pandas as pd

from .api import GDCClient
from .filters import Filters as F
from .utils import flatten_hits


def list_datasets(program: str = "TCGA",
                  fields: Optional[Iterable[str]] = None,
                  as_dataframe: bool = True):
    """
    List available projects (datasets) in the GDC for a program (default: TCGA).

    Returns
    -------
    pd.DataFrame | list[dict]
        By default a DataFrame with metadata columns:
        ['project_id', 'name', 'disease_type', 'primary_site',
         'summary.case_count', 'summary.file_count'].
        If as_dataframe=False, returns a list of dicts instead.
    """
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
