from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, List, Optional
import json
import requests

from .config import GDC_BASE_URL

log = logging.getLogger("tcga_tools")

class GDCClient:
    """Thin wrapper over the GDC REST API.

    Supports search-and-retrieval (projects, cases, files, annotations)
    with pagination, and data/manifest downloads.
    """

    def __init__(self, base_url: str | None = None, token: Optional[str] = None, timeout: int = 60):
        self.base_url = base_url or GDC_BASE_URL
        self.session = requests.Session()
        self.timeout = timeout
        self.last_queries: list[dict[str, Any]] = []
        if token:
            self.session.headers.update({"X-Auth-Token": token})

    # ------------------------- Core HTTP helpers -------------------------
    def _get_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post_json(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        r = self.session.post(url, json=payload, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # ------------------------- Search & retrieval ------------------------
    def paged_query(self, endpoint: str, filters: Dict[str, Any], fields: Optional[Iterable[str]], size: int = 5000) -> List[Dict[str, Any]]:
        """Return *all* hits for a filters+fields query, honoring pagination.
        If the API rejects the field list (HTTP 400), retry without explicit fields.
        """
        from_ = 0
        all_hits: List[Dict[str, Any]] = []
        fields_str = ",".join(fields) if fields else None
        while True:
            payload = {
                "filters": filters,
                "format": "JSON",
                "size": size,
                "from": from_,
            }
            if fields_str:
                payload["fields"] = fields_str
            try:
                data = self._post_json(endpoint, payload)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 400 and fields_str is not None:
                    log.warning("%s rejected requested fields; retrying without 'fields'", endpoint)
                    fields_str = None
                    continue
                raise
            hits = data.get("data", {}).get("hits", [])
            all_hits.extend(hits)
            pg = data.get("data", {}).get("pagination", {})
            total = int(pg.get("total", 0))
            from_ = from_ + size
            if len(all_hits) >= total or not hits:
                break
            
            
        self.last_queries.append({
            "endpoint": endpoint,
            "filters": filters,
            "requested_fields": list(fields) if fields else None,
            "returned_count": len(all_hits),
        })
        
        return all_hits

    def cases_query(self, filters: Dict[str, Any], fields: Optional[Iterable[str]], size: int = 5000) -> List[Dict[str, Any]]:
        return self.paged_query("cases", filters, fields, size=size)

    # -------------------------- Downloads --------------------------------
    def download_single(self, uuid: str, *, target_path: str, related_files: bool = False) -> str:
        """Stream-download a single file by UUID using the /data endpoint.

        If ``related_files=True``, include indices when available (e.g., .bai for BAM).
        """
        url = f"{self.base_url}/data/{uuid}"
        params = {"related_files": "true"} if related_files else None
        with self.session.get(url, params=params, stream=True, timeout=self.timeout) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        log.info("Downloaded %s -> %s", uuid, target_path)
        return target_path

    def download_tar(self, uuids: list[str], *, target_path: str, uncompressed: bool = False) -> str:
        """Download multiple files as a tar(.gz) archive using POST /data.
        Set ``uncompressed=True`` to request a .tar instead of .tar.gz.
        """
        url = f"{self.base_url}/data"
        params = {"tarfile": "true"} if uncompressed else None
        payload = {"ids": uuids}
        with self.session.post(url, params=params, json=payload, stream=True, timeout=self.timeout) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        log.info("Downloaded %d files -> %s", len(uuids), target_path)
        return target_path

    def download_manifest_for_query(self, filters: Dict[str, Any], *, manifest_path: str) -> str:
        """
        Save a TSV manifest using return_type=manifest on /files for a given filter set.
        As per GDC docs, 'filters' must be JSON-encoded in the query params.
        """
        url = f"{self.base_url}/files"
        params = {
            "filters": json.dumps(filters),
            "return_type": "manifest",
        }
        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        with open(manifest_path, "wb") as f:
            f.write(r.content)
        log.info("Wrote manifest: %s", manifest_path)
        return manifest_path
