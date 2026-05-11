"""TCGA-Tools — light-weight Python client for NCI GDC/TCGA.

Usage
-----
>>> import tcga_tools as tt
>>> tt.Download(dataset_name="TCGA-LUSC", filetypes=[".svs"],
...             annotations=["clinical", "molecular", "report"],
...             output_dir="./TCGA-LUSC")

New architecture
----------------
>>> from tcga_tools.pathology import PathologyDataPortal
>>> portal = PathologyDataPortal()
>>> sop_list = portal.list_tcia_sop_instance_uids(
...     tt.TciaSeriesQuery(series_instance_uid="uid.series.instance")
... )
>>> portal.download_tcia_series(series_instance_uid="uid.series.instance", output_dir="./TCIA")

This package wraps the public GDC REST API and TCIA endpoints to produce:
- Downloaded data files matching a project + file type
- A files_metadata.csv describing every downloaded file
- Optional annotations CSVs (clinical/molecular/report)
- A groups.csv that groups patients/cases and samples
"""

from .datasets import (
    list_data_types,
    list_datasets,
    list_datasets as ListDatasets,
)
from .downloader import download, download as Download
from .validation import validate_output_dir
from .pathology import PathologyDataPortal
from .services.tcia_pathology import TciaSeriesQuery

__all__ = [
    "Download",
    "ListDatasets",
    "download",
    "list_data_types",
    "list_datasets",
    "validate_output_dir",
    "PathologyDataPortal",
    "TciaSeriesQuery",
]
