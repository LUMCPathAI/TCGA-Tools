"""TCGA-Tools — light-weight Python client for NCI GDC/TCGA.

Usage
-----
>>> import tcga_tools as tt
>>> tt.Download(dataset_name="TCGA-LUSC", filetypes=[".svs"],
...             annotations=["clinical", "molecular", "report"],
...             output_dir="./TCGA-LUSC")

This package wraps the public GDC REST API and produces:
- Downloaded data files matching a project + file type
- A files_metadata.csv describing every downloaded file
- Optional annotations CSVs (clinical/molecular/report)
- A groups.csv that groups patients/cases and samples

Environment
-----------
- Optional auth token for controlled-access files via env var ``GDC_TOKEN``.
- Dependencies: requests, pandas, tqdm.
"""

from .datasets import (
    list_data_types,
    list_datasets,
    list_datasets as ListDatasets,
)
from .downloader import download, download as Download
from .validation import validate_output_dir

__all__ = [
    "Download",
    "ListDatasets",
    "download",
    "list_data_types",
    "list_datasets",
    "validate_output_dir",
]
