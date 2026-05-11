from __future__ import annotations

import concurrent.futures
import datetime as _dt
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import requests
from tqdm import tqdm

from .config import TCIA_BASE_URL, TCIA_PATIENT_STUDY_FIELDS, TCIA_SERIES_FIELDS
from .utils import ensure_dir, save_json, to_csv, write_text_log

log = logging.getLogger("tcga_tools")


def _normalize_worker_count(download_workers: int | None) -> int:
    """Return a bounded worker count for parallel TCIA series downloads."""
    if download_workers is None:
        return 4
    assert download_workers >= 1, "download_workers must be >= 1"
    return int(download_workers)


class TCIAClient:
    """Thin wrapper over the public TCIA/NBIA REST API."""

    def __init__(self, base_url: str | None = None, timeout: int = 60):
        self.base_url = base_url or TCIA_BASE_URL
        self.session = requests.Session()
        self.timeout = timeout
        self.last_queries: list[dict[str, Any]] = []

    def _get_json(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        self.last_queries.append(
            {"endpoint": endpoint, "params": params or {}, "returned_count": len(data)}
        )
        return data

    def list_collections(self) -> list[dict[str, Any]]:
        return self._get_json("getCollectionValues")

    def list_modalities(self, *, collection: str | None = None) -> list[dict[str, Any]]:
        params = {"Collection": collection} if collection else None
        return self._get_json("getModalityValues", params=params)

    def get_patient_studies(
        self,
        *,
        collection: str,
        modalities: Iterable[str] | None = None,
        patient_id: str | None = None,
        study_instance_uid: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        selected_modalities = list(modalities or [None])
        for modality in selected_modalities:
            params = {"Collection": collection}
            if modality:
                params["Modality"] = modality
            if patient_id:
                params["PatientID"] = patient_id
            if study_instance_uid:
                params["StudyInstanceUID"] = study_instance_uid
            rows.extend(self._get_json("getPatientStudy", params=params))
        return rows

    def get_series(
        self,
        *,
        collection: str,
        modalities: Iterable[str] | None = None,
        patient_id: str | None = None,
        study_instance_uid: str | None = None,
        series_instance_uid: str | None = None,
        body_part: str | None = None,
        manufacturer: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if series_instance_uid:
            # The public API exposes getSeries with filters; retrieving by Series UID is
            # delegated to a collection-level query and then filtered locally.
            modalities = list(modalities or [None])
        selected_modalities = list(modalities or [None])
        for modality in selected_modalities:
            params = {"Collection": collection}
            if modality:
                params["Modality"] = modality
            if patient_id:
                params["PatientID"] = patient_id
            if study_instance_uid:
                params["StudyInstanceUID"] = study_instance_uid
            if body_part:
                params["BodyPartExamined"] = body_part
            if manufacturer:
                params["Manufacturer"] = manufacturer
            rows.extend(self._get_json("getSeries", params=params))
        if series_instance_uid:
            rows = [
                row
                for row in rows
                if row.get("SeriesInstanceUID") == series_instance_uid
            ]
        return rows

    def download_series(
        self,
        series_instance_uid: str,
        *,
        target_path: str,
        include_md5: bool = False,
    ) -> str:
        endpoint = "getImageWithMD5Hash" if include_md5 else "getImage"
        url = f"{self.base_url}/{endpoint}"
        params = {"SeriesInstanceUID": series_instance_uid}
        with self.session.get(
            url, params=params, stream=True, timeout=self.timeout
        ) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        log.info("Downloaded TCIA series %s -> %s", series_instance_uid, target_path)
        return target_path


def list_tcia_datasets(
    *,
    include_data_types: bool = False,
    as_dataframe: bool = True,
    client: TCIAClient | None = None,
):
    """List public TCIA collections and, optionally, their modalities."""
    tcia_client = client or TCIAClient()
    collections = tcia_client.list_collections()
    records: list[dict[str, Any]] = []
    for row in collections:
        collection = row.get("Collection")
        record = {
            "dataset_name": collection,
            "collection": collection,
            "source": "tcia",
        }
        if include_data_types and collection:
            modalities = tcia_client.list_modalities(collection=collection)
            record["data_types"] = [
                item.get("Modality") for item in modalities if item.get("Modality")
            ]
        records.append(record)
    df = pd.DataFrame(records)
    return df if as_dataframe else records


def list_tcia_data_types(
    *,
    dataset_name: str | None = None,
    as_dataframe: bool = True,
    client: TCIAClient | None = None,
):
    """List TCIA modalities globally or for one collection."""
    tcia_client = client or TCIAClient()
    modalities = tcia_client.list_modalities(collection=dataset_name)
    records = [
        {
            "dataset_name": dataset_name,
            "data_type": row.get("Modality"),
            "source": "tcia",
        }
        for row in modalities
        if row.get("Modality")
    ]
    df = pd.DataFrame(records)
    return df if as_dataframe else records


def download_tcia(
    dataset_name: str | Iterable[str],
    output_dir: str | Path = ".",
    *,
    datatype: str | Iterable[str] | None = None,
    annotations: Iterable[str] | None = None,
    raw: bool = False,
    log_transforms: bool = True,
    patient_id: str | None = None,
    study_instance_uid: str | None = None,
    series_instance_uid: str | None = None,
    body_part: str | None = None,
    manufacturer: str | None = None,
    include_md5: bool = False,
    download_workers: int | None = None,
) -> dict[str, Optional[Path]]:
    """Download TCIA public collections as ZIP-per-series bundles.

    TCIA exposes imaging collections and related metadata through the NBIA API.
    Unlike GDC, there is no standardized cross-collection API for rich clinical,
    molecular, pathology-report, or diagnosis tables. This function therefore
    exports:

    - ``files_metadata.csv``: series-level imaging metadata
    - ``patient_studies.csv``: patient/study metadata
    - ``clinical.csv``: alias of ``patient_studies.csv`` when requested
    """
    selected_modalities = None
    normalized_download_workers = _normalize_worker_count(download_workers)
    if datatype is not None:
        selected_modalities = (
            [datatype] if isinstance(datatype, str) else list(datatype)
        )
    selected_annotations = {item.lower() for item in annotations or []}
    unsupported_annotations = selected_annotations.difference({"clinical", "all"})
    if unsupported_annotations:
        log.warning(
            "TCIA does not expose standardized APIs for annotations %s; they will be skipped.",
            sorted(unsupported_annotations),
        )

    collections = (
        [dataset_name]
        if isinstance(dataset_name, str)
        else [str(item) for item in dataset_name]
    )
    client = TCIAClient()
    out_root = ensure_dir(output_dir)
    multi_index: dict[str, dict[str, str]] = {}
    all_series: list[pd.DataFrame] = []

    for collection in (
        tqdm(collections, desc="Collections") if len(collections) > 1 else collections
    ):
        collection_out = ensure_dir(Path(out_root) / collection)
        series_rows = client.get_series(
            collection=collection,
            modalities=selected_modalities,
            patient_id=patient_id,
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            body_part=body_part,
            manufacturer=manufacturer,
        )
        series_df = pd.DataFrame(series_rows)
        if series_df.empty:
            series_df = pd.DataFrame(columns=TCIA_SERIES_FIELDS)
        if "PatientID" in series_df.columns:
            series_df["patient"] = series_df["PatientID"]
        else:
            series_df["patient"] = pd.NA
        files_csv = to_csv(series_df, collection_out / "files_metadata.csv")

        patient_rows = client.get_patient_studies(
            collection=collection,
            modalities=selected_modalities,
            patient_id=patient_id,
            study_instance_uid=study_instance_uid,
        )
        patient_df = pd.DataFrame(patient_rows)
        if patient_df.empty:
            patient_df = pd.DataFrame(columns=TCIA_PATIENT_STUDY_FIELDS)
        patients_csv = to_csv(patient_df, collection_out / "patient_studies.csv")

        data_dir = ensure_dir(collection_out / "data")
        downloaded_paths: list[Path] = []
        if not raw:

            def _download_one(row: dict[str, Any]) -> Optional[Path]:
                series_uid = row.get("SeriesInstanceUID")
                if not series_uid:
                    return None
                target_name = f"{series_uid}.zip".replace("/", "_")
                target_path = data_dir / target_name
                if target_path.exists():
                    return target_path
                client.download_series(
                    series_uid,
                    target_path=str(target_path),
                    include_md5=include_md5,
                )
                return target_path

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=normalized_download_workers
            ) as executor:
                futures = [executor.submit(_download_one, row) for row in series_rows]
                for future in tqdm(
                    concurrent.futures.as_completed(futures),
                    total=len(futures),
                    desc=f"Downloading {collection}",
                ):
                    target_path = future.result()
                    if target_path is not None:
                        downloaded_paths.append(target_path)

        preview_json = None
        if raw:
            preview_json = save_json(
                {
                    "dataset_name": collection,
                    "source": "tcia",
                    "modalities": selected_modalities or [],
                    "series_found": int(len(series_df)),
                    "example_series": series_df.head(10).to_dict(orient="records"),
                },
                collection_out / "preview.json",
            )

        clinical_csv = None
        if "clinical" in selected_annotations or "all" in selected_annotations:
            clinical_csv = to_csv(patient_df, collection_out / "clinical.csv")

        artifacts: dict[str, Optional[Path]] = {
            "files_csv": files_csv,
            "groups_csv": None,
            "manifest_tsv": None,
            "data_dir": data_dir,
            "stats_json": None,
            "preview_json": preview_json,
            "clinical_csv": clinical_csv,
            "molecular_csv": None,
            "report_csv": None,
            "diagnosis_csv": None,
            "patient_studies_csv": patients_csv,
        }
        multi_index[collection] = {
            key: str(value) for key, value in artifacts.items() if value is not None
        }
        all_series.append(series_df)

    if all_series:
        to_csv(
            pd.concat(all_series, ignore_index=True),
            Path(out_root) / "files_metadata.all.csv",
        )

    if log_transforms:
        run_log = {
            "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
            "source": "tcia",
            "datasets": collections,
            "datatype": list(selected_modalities or []),
            "annotations": list(selected_annotations),
            "raw": raw,
            "patient_id": patient_id,
            "study_instance_uid": study_instance_uid,
            "series_instance_uid": series_instance_uid,
            "body_part": body_part,
            "manufacturer": manufacturer,
            "download_workers": normalized_download_workers,
            "queries": getattr(client, "last_queries", []),
        }
        save_json(run_log, Path(out_root) / "run_log.json")
        write_text_log(
            [
                "Source: TCIA",
                f"Datasets: {', '.join(collections)}",
                f"Datatype: {', '.join(selected_modalities or [])}",
                f"Annotations: {', '.join(sorted(selected_annotations))}",
                f"DownloadWorkers: {normalized_download_workers}",
                f"Raw={raw}",
            ],
            Path(out_root) / "run_log.txt",
        )

    if len(collections) == 1:
        return {
            key: (Path(value) if isinstance(value, str) else value)
            for key, value in multi_index[collections[0]].items()
        }
    index_path = save_json(multi_index, Path(out_root) / "multi_index.json")
    return {"multi_index_json": index_path}
