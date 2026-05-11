"""Use-cases for TCIA pathology downloads and metadata."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..ports import DownloadResult, SopInstanceResult, TciaDataPort


@dataclass
class TciaSeriesQuery:
    """Query parameters for TCIA SeriesInstanceUID lookups."""

    series_instance_uid: str
    format_: str = "JSON"
    path: Optional[str] = None
    name: Optional[str] = None


@dataclass
class TciaPathologyService:
    """Service that orchestrates TCIA series/instance interactions."""

    client: TciaDataPort

    def get_sop_instance_uids(self, query: TciaSeriesQuery) -> SopInstanceResult:
        """Retrieve SOPInstanceUIDs for a given SeriesInstanceUID."""
        return self.client.sop_instance_uids(
            series_instance_uid=query.series_instance_uid,
            format_=query.format_,
            path=query.path,
            name=query.name,
        )

    def download_single_image(
        self,
        *,
        series_instance_uid: str,
        sop_instance_uid: str,
        output_dir: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        """Download a single DICOM image from TCIA."""
        return self.client.download_single_image(
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            path=output_dir,
            name=name,
        )

    def download_series_images(
        self,
        *,
        series_instance_uid: str,
        output_dir: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        """Download a series as a zip file from TCIA."""
        return self.client.download_series_images(
            series_instance_uid=series_instance_uid,
            path=output_dir,
            name=name,
        )
