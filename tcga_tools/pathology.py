"""High-level facade for TCGA and TCIA pathology workflows."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd

from .adapters.gdc_api_wrapper import GdcApiWrapperDownloader
from .adapters.gdc_metadata import GdcRestMetadataClient
from .api import GDCClient
from .config import DEFAULT_FILE_FIELDS
from .adapters.tcia_api_wrapper import TciaApiWrapperClient
from .services.tcia_pathology import TciaPathologyService, TciaSeriesQuery
from .services.tcga_pathology import TcgaPathologyService
from .utils import read_env_token


@dataclass
class PathologyDataPortal:
    """Entry point for TCGA + TCIA pathology data workflows."""

    def __post_init__(self) -> None:
        token = read_env_token()
        gdc_client = GDCClient(token=token)
        metadata_adapter = GdcRestMetadataClient(client=gdc_client)
        download_adapter = GdcApiWrapperDownloader()
        self.tcga = TcgaPathologyService(metadata_client=metadata_adapter, downloader=download_adapter)
        self.tcia = TciaPathologyService(client=TciaApiWrapperClient())

    def list_tcga_files(
        self,
        *,
        project_id: str,
        filetypes: Iterable[str],
        fields: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """List TCGA file metadata for a pathology project."""
        return self.tcga.list_files(project_id=project_id, filetypes=filetypes, fields=fields or DEFAULT_FILE_FIELDS)

    def download_tcga_project(
        self,
        *,
        project_id: str,
        filetypes: Iterable[str],
        output_dir: str,
        fields: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """Download all matching TCGA files and return the metadata DataFrame."""
        df = self.list_tcga_files(project_id=project_id, filetypes=filetypes, fields=fields)
        if "id" in df.columns and not df.empty:
            self.tcga.download_files(uuids=df["id"].tolist(), output_dir=output_dir)
        return df

    def list_tcia_sop_instance_uids(self, query: TciaSeriesQuery) -> dict | list | str | None:
        """Return SOPInstanceUIDs for a TCIA series (JSON by default)."""
        return self.tcia.get_sop_instance_uids(query).payload

    def download_tcia_single_image(
        self,
        *,
        series_instance_uid: str,
        sop_instance_uid: str,
        output_dir: str,
        name: Optional[str] = None,
    ) -> str:
        """Download a single TCIA DICOM instance and return the file path."""
        return self.tcia.download_single_image(
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            output_dir=output_dir,
            name=name,
        ).path

    def download_tcia_series(
        self,
        *,
        series_instance_uid: str,
        output_dir: str,
        name: Optional[str] = None,
    ) -> str:
        """Download a TCIA series as a zip file and return the file path."""
        return self.tcia.download_series_images(
            series_instance_uid=series_instance_uid,
            output_dir=output_dir,
            name=name,
        ).path
