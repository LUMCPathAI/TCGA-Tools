"""TCIA API adapter built on top of gdc-api-wrapper."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import importlib.util
import logging

from ..ports import DownloadResult, SopInstanceResult, TciaDataPort

log = logging.getLogger("tcga_tools")


@dataclass(frozen=True)
class TciaApiWrapperClient(TciaDataPort):
    """Adapter for TCIA endpoints provided by gdc-api-wrapper."""

    def _ensure_dependency(self) -> None:
        if importlib.util.find_spec("gdcapiwrapper") is None:
            raise ModuleNotFoundError(
                "gdc-api-wrapper is required. Install with: pip install gdc-api-wrapper"
            )

    def sop_instance_uids(
        self,
        series_instance_uid: str,
        *,
        format_: str = "JSON",
        path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> SopInstanceResult:
        self._ensure_dependency()
        from gdcapiwrapper.tcia import Data  # type: ignore

        response, payload = Data.sop_instance_uids(
            series_instance_uid=series_instance_uid,
            format_=format_,
            path=path,
            name=name,
        )
        if response is None:
            log.warning("TCIA wrapper returned empty response for %s", series_instance_uid)
        return SopInstanceResult(
            series_instance_uid=series_instance_uid,
            format=format_,
            payload=payload,
            filename=payload if isinstance(payload, str) else None,
        )

    def download_single_image(
        self,
        series_instance_uid: str,
        sop_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        self._ensure_dependency()
        from gdcapiwrapper.tcia import Data  # type: ignore

        out_dir = Path(path)
        out_dir.mkdir(parents=True, exist_ok=True)
        response, filename = Data.download_single_image(
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            path=str(out_dir),
            name=name,
        )
        if response is None:
            log.warning("TCIA wrapper returned empty response for %s", sop_instance_uid)
        resolved = out_dir / (filename or name or sop_instance_uid)
        return DownloadResult(path=str(resolved), filename=resolved.name)

    def download_series_images(
        self,
        series_instance_uid: str,
        *,
        path: str,
        name: Optional[str] = None,
    ) -> DownloadResult:
        self._ensure_dependency()
        from gdcapiwrapper.tcia import Data  # type: ignore

        out_dir = Path(path)
        out_dir.mkdir(parents=True, exist_ok=True)
        response, filename = Data.download_series_instance_images(
            series_instance_uid=series_instance_uid,
            path=str(out_dir),
            name=name,
        )
        if response is None:
            log.warning("TCIA wrapper returned empty response for %s", series_instance_uid)
        resolved = out_dir / (filename or name or f"{series_instance_uid}.zip")
        return DownloadResult(path=str(resolved), filename=resolved.name)
