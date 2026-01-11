"""TCGA-Tools — modular client for TCGA/GDC and TCIA pathology data.

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

from .downloader import download as Download
from .datasets import list_datasets as ListDatasets
from .pathology import PathologyDataPortal
from .services.tcia_pathology import TciaSeriesQuery

__all__ = [
    "Download",
    "ListDatasets",
    "PathologyDataPortal",
    "TciaSeriesQuery",
]
