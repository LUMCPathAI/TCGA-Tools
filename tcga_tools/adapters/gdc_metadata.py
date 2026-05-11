"""GDC metadata adapter using the native GDC REST client."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from ..api import GDCClient
from ..ports import GdcMetadataPort


@dataclass
class GdcRestMetadataClient(GdcMetadataPort):
    """Adapter wrapping the existing GDCClient for metadata queries."""

    client: GDCClient

    def paged_query(self, endpoint: str, filters: dict, fields: Optional[Iterable[str]]) -> list[dict]:
        return self.client.paged_query(endpoint, filters, fields)

    def download_manifest(self, filters: dict, *, manifest_path: str) -> str:
        return self.client.download_manifest_for_query(filters, manifest_path=manifest_path)
