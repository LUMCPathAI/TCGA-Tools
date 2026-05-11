"""Ports (interfaces) for TCGA-Tools clean architecture.

These abstract interfaces define dependency directionality between
use-cases (services) and infrastructure adapters (GDC/TCIA clients).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence


@dataclass(frozen=True)
class DownloadResult:
    """Represents a completed download on disk."""

    path: str
    filename: str


class FileDownloadPort(ABC):
    """Abstract file download interface."""

    @abstractmethod
    def download_single(self, uuid: str, *, path: str, name: Optional[str] = None) -> DownloadResult:
        """Download a single file by UUID to ``path`` with optional ``name``."""

    @abstractmethod
    def download_multiple(self, uuids: Sequence[str], *, path: str) -> DownloadResult:
        """Download multiple files by UUID (POST) into ``path``."""


class GdcMetadataPort(ABC):
    """Abstract access to GDC metadata endpoints."""

    @abstractmethod
    def paged_query(self, endpoint: str, filters: dict, fields: Optional[Iterable[str]]) -> list[dict]:
        """Run a paginated query and return all hits."""

    @abstractmethod
    def download_manifest(self, filters: dict, *, manifest_path: str) -> str:
        """Download a GDC manifest TSV for the given filters."""


@dataclass(frozen=True)
class SopInstanceResult:
    """Represents a TCIA SOP Instance UID lookup result."""

    series_instance_uid: str
    format: str
    payload: dict | list | str | None
    filename: Optional[str] = None


class TciaDataPort(ABC):
    """Abstract access to TCIA series and instance downloads."""

    @abstractmethod
    def sop_instance_uids(
        self,
        series_instance_uid: str,
        *,
        format_: str = "JSON",
        path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> SopInstanceResult:
        """Get SOPInstanceUIDs for a series."""

    @abstractmethod
    def download_single_image(
        self,
        series_instance_uid: str,
        sop_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        """Download a single DICOM image."""

    @abstractmethod
    def download_series_images(
        self,
        series_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        """Download a series as a zip file."""
