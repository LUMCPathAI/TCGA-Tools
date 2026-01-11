"""GDC API adapter built on top of gdc-api-wrapper."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence
import importlib.util
import logging

from ..ports import DownloadResult, FileDownloadPort

log = logging.getLogger("tcga_tools")


@dataclass(frozen=True)
class GdcApiWrapperDownloader(FileDownloadPort):
    """Download adapter leveraging gdc-api-wrapper's Data class."""

    def _ensure_dependency(self) -> None:
        if importlib.util.find_spec("gdcapiwrapper") is None:
            raise ModuleNotFoundError(
                "gdc-api-wrapper is required. Install with: pip install gdc-api-wrapper"
            )

    def download_single(self, uuid: str, *, path: str, name: Optional[str] = None) -> DownloadResult:
        self._ensure_dependency()
        from gdcapiwrapper.tcga import Data  # type: ignore

        out_dir = Path(path)
        out_dir.mkdir(parents=True, exist_ok=True)
        response, filename = Data.download(uuid=uuid, path=str(out_dir), name=name)
        if response is None:
            log.warning("GDC wrapper returned empty response for %s", uuid)
        resolved = out_dir / (filename or name or uuid)
        return DownloadResult(path=str(resolved), filename=resolved.name)

    def download_multiple(self, uuids: Sequence[str], *, path: str) -> DownloadResult:
        self._ensure_dependency()
        from gdcapiwrapper.tcga import Data  # type: ignore

        out_dir = Path(path)
        out_dir.mkdir(parents=True, exist_ok=True)
        response, filename = Data.download_multiple(uuid_list=list(uuids), path=str(out_dir))
        if response is None:
            log.warning("GDC wrapper returned empty response for multi-download")
        resolved = out_dir / (filename or "gdc_download.tar.gz")
        return DownloadResult(path=str(resolved), filename=resolved.name)
