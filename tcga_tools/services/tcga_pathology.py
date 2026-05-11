"""Use-cases for TCGA pathology downloads and metadata."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from ..config import DEFAULT_FILE_FIELDS
from ..filters import Filters as F
from ..ports import FileDownloadPort, GdcMetadataPort
from ..utils import flatten_hits


@dataclass
class TcgaPathologyService:
    """Service that orchestrates TCGA metadata queries and downloads."""

    metadata_client: GdcMetadataPort
    downloader: FileDownloadPort

    def _build_file_filters(self, project_id: str, filetypes: Iterable[str]) -> dict:
        clauses = []
        for ext in filetypes:
            ext = ext.lower()
            clauses.append(F.IN("file_name", [f"*{ext}"]))
        filetype_filter = clauses[0] if len(clauses) == 1 else F.OR(*clauses)
        return F.AND(
            F.EQ("cases.project.project_id", project_id),
            filetype_filter,
        )

    def list_files(
        self,
        *,
        project_id: str,
        filetypes: Iterable[str],
        fields: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """Return TCGA file metadata for the given project and filetypes."""
        filters = self._build_file_filters(project_id, filetypes)
        hits = self.metadata_client.paged_query("files", filters, fields or DEFAULT_FILE_FIELDS)
        return flatten_hits(hits)

    def download_files(
        self,
        *,
        uuids: Iterable[str],
        output_dir: str,
    ) -> list[Path]:
        """Download a set of files (POST) using the GDC wrapper."""
        result = self.downloader.download_multiple(list(uuids), path=output_dir)
        return [Path(result.path)]

    def download_file(self, *, uuid: str, output_dir: str, name: Optional[str] = None) -> Path:
        """Download a single file (GET) using the GDC wrapper."""
        result = self.downloader.download_single(uuid, path=output_dir, name=name)
        return Path(result.path)
