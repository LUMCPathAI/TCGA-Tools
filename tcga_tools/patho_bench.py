from __future__ import annotations

import datetime as _dt
import logging
import os
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests

from .config import HF_BASE_URL, HF_TOKEN_ENV, PATHO_BENCH_REPO_ID
from .tcia import download_tcia
from .utils import ensure_dir, save_json, to_csv, write_text_log

log = logging.getLogger("tcga_tools")


PATHO_BENCH_DATASET_REGISTRY: dict[str, dict[str, Any]] = {
    "ebrains": {
        "display_name": "EBRAINS",
        "raw_data_url": "https://doi.org/10.25493/WQ48-ZGX",
        "raw_data_source": "external",
    },
    "bracs": {
        "display_name": "BRACS",
        "raw_data_url": "https://www.bracs.icar.cnr.it/",
        "raw_data_source": "external",
    },
    "panda": {
        "display_name": "PANDA",
        "raw_data_url": "https://panda.grand-challenge.org/data/",
        "raw_data_source": "external",
    },
    "imp": {
        "display_name": "IMP",
        "raw_data_url": "https://rdm.inesctec.pt/dataset/nis-2023-008",
        "raw_data_source": "external",
    },
    "bcnb": {
        "display_name": "BCNB",
        "raw_data_url": "https://bupt-ai-cz.github.io/BCNB/",
        "raw_data_source": "external",
    },
    "cptac_brca": {
        "display_name": "CPTAC-BRCA",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-brca/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-BRCA",
    },
    "cptac_ccrcc": {
        "display_name": "CPTAC-CCRCC",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-ccrcc/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-CCRCC",
    },
    "cptac_coad": {
        "display_name": "CPTAC-COAD",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-coad/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-COAD",
    },
    "cptac_gbm": {
        "display_name": "CPTAC-GBM",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-gbm/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-GBM",
    },
    "cptac_hnsc": {
        "display_name": "CPTAC-HNSC",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-hnsc/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-HNSC",
    },
    "cptac_lscc": {
        "display_name": "CPTAC-LSCC",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-lscc/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-LSCC",
    },
    "cptac_luad": {
        "display_name": "CPTAC-LUAD",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-luad/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-LUAD",
    },
    "cptac_pda": {
        "display_name": "CPTAC-PDAC",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-pda/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-PDA",
    },
    "mut-het-rcc": {
        "display_name": "MUT-HET-RCC",
        "raw_data_url": "https://doi.org/10.25452/figshare.plus.c.5983795",
        "raw_data_source": "external",
    },
    "ovarian": {
        "display_name": "OV-Bevacizumab",
        "raw_data_url": "https://www.nature.com/articles/s41597-022-01127-6",
        "raw_data_source": "external",
    },
    "nadt": {
        "display_name": "NADT-Prostate",
        "raw_data_url": "https://www.medrxiv.org/content/10.1101/2020.09.29.20199711v1.full",
        "raw_data_source": "external",
    },
    "natbrca": {
        "display_name": "POST-NAT-BRCA",
        "raw_data_url": "https://onlinelibrary.wiley.com/doi/10.1002/cyto.a.23244",
        "raw_data_source": "external",
    },
    "boehmk_": {
        "display_name": "BOEHMK",
        "raw_data_url": "https://www.synapse.org/Synapse:syn25946117/wiki/611576",
        "raw_data_source": "external",
    },
    "mbc_": {
        "display_name": "MBC",
        "raw_data_url": "https://www.synapse.org/Synapse:syn59490671/wiki/628046",
        "raw_data_source": "external",
    },
    "sr386_": {
        "display_name": "SURGEN",
        "raw_data_url": "https://www.ebi.ac.uk/biostudies/bioimages/studies/S-BIAD1285",
        "raw_data_source": "external",
    },
    "cptac_ucec": {
        "display_name": "CPTAC-UCEC",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-ucec/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-UCEC",
    },
    "cptac_ov": {
        "display_name": "CPTAC-OV",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/cptac-ov/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "CPTAC-OV",
    },
    "visiomel": {
        "display_name": "VisioMel",
        "raw_data_url": "https://www.drivendata.org/competitions/148/visiomel-melanoma/",
        "raw_data_source": "external",
    },
    "ucla_lung": {
        "display_name": "UCLA Lung",
        "raw_data_url": "https://idr.openmicroscopy.org/webclient/?show=project-1251",
        "raw_data_source": "external",
    },
    "herroi": {
        "display_name": "HER2-Tumor-ROIs",
        "raw_data_url": "https://www.cancerimagingarchive.net/collection/her2-tumor-rois/",
        "raw_data_source": "tcia",
        "delegate_dataset_name": "HER2-Tumor-ROIs",
    },
    "dhmc_luad": {
        "display_name": "DHMC LUAD",
        "raw_data_url": "https://bmirds.github.io/LungCancer/",
        "raw_data_source": "external",
    },
    "cptac_ccrcc_dhmc": {
        "display_name": "DHMC CCRCC",
        "raw_data_url": "https://bmirds.github.io/KidneyCancer/",
        "raw_data_source": "external",
    },
    "hancook": {
        "display_name": "Hancock",
        "raw_data_url": "https://hancock.research.fau.eu/download",
        "raw_data_source": "external",
    },
    "bc_therapy": {
        "display_name": "BC Therapy",
        "raw_data_url": "https://zenodo.org/records/6337925#.Y30d1y-l1Ls",
        "raw_data_source": "external",
    },
    "cptac_all": {
        "display_name": "CPTAC-All",
        "raw_data_url": "https://www.cancerimagingarchive.net/",
        "raw_data_source": "tcia",
        "delegate_dataset_names": [
            "CPTAC-BRCA",
            "CPTAC-CCRCC",
            "CPTAC-COAD",
            "CPTAC-GBM",
            "CPTAC-HNSC",
            "CPTAC-LSCC",
            "CPTAC-LUAD",
            "CPTAC-PDA",
            "CPTAC-UCEC",
            "CPTAC-OV",
        ],
    },
    "cptac_lung": {
        "display_name": "CPTAC-Lung",
        "raw_data_url": "https://www.cancerimagingarchive.net/",
        "raw_data_source": "tcia",
        "delegate_dataset_names": ["CPTAC-LUAD", "CPTAC-LSCC"],
    },
    "crc_outcomes": {
        "display_name": "Valentino-CRC / BRAF-CRC",
        "raw_data_url": "https://www.ebi.ac.uk/biostudies/bioimages/studies/S-BIAD1407?query=czi",
        "raw_data_source": "external",
    },
    "cervical_subtype": {
        "display_name": "IMP-Cervical",
        "raw_data_url": "https://rdm.inesctec.pt/dataset/nis-2024-003",
        "raw_data_source": "external",
    },
}


def _normalize_slug(dataset_name: str) -> str:
    needle = dataset_name.strip().lower()
    for slug, info in PATHO_BENCH_DATASET_REGISTRY.items():
        aliases = {
            slug.lower(),
            str(info["display_name"]).lower(),
            str(info["display_name"]).lower().replace(" ", "_"),
            str(info["display_name"]).lower().replace(" ", "-"),
        }
        if needle in aliases:
            return slug
    raise ValueError(f"Unsupported Patho-Bench dataset: {dataset_name}")


def _parse_simple_yaml_list_map(text: str) -> dict[str, list[str]]:
    parsed: dict[str, list[str]] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            if current_key is None:
                raise ValueError("Encountered YAML list item before any key")
            parsed[current_key].append(stripped[2:].strip())
            continue
        current_key = stripped.removesuffix(":").strip()
        parsed[current_key] = []
    return {key: value for key, value in parsed.items() if value}


class PathoBenchClient:
    """Minimal client for Patho-Bench metadata files hosted on Hugging Face."""

    def __init__(
        self,
        repo_id: str | None = None,
        base_url: str | None = None,
        token: str | None = None,
        timeout: int = 60,
    ):
        self.repo_id = repo_id or PATHO_BENCH_REPO_ID
        self.base_url = base_url or HF_BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.last_queries: list[dict[str, Any]] = []
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def list_repo_tree(
        self, *, path_in_repo: str | None = None, recursive: bool = False
    ) -> list[dict[str, Any]]:
        path_suffix = f"/{path_in_repo.strip('/')}" if path_in_repo else ""
        url = f"{self.base_url}/api/datasets/{self.repo_id}/tree/main{path_suffix}"
        response = self.session.get(
            url,
            params={"recursive": str(recursive).lower()},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        self.last_queries.append(
            {
                "endpoint": "tree",
                "path_in_repo": path_in_repo,
                "recursive": recursive,
                "returned_count": len(data),
            }
        )
        return data

    def download_file(self, repo_path: str, *, target_path: Path) -> Path:
        url = f"{self.base_url}/datasets/{self.repo_id}/resolve/main/{repo_path}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(response.content)
        self.last_queries.append({"endpoint": "resolve", "repo_path": repo_path})
        return target_path

    def fetch_available_splits(self) -> dict[str, list[str]]:
        url = f"{self.base_url}/datasets/{self.repo_id}/resolve/main/available_splits.yaml"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        self.last_queries.append({"endpoint": "available_splits"})
        return _parse_simple_yaml_list_map(response.text)


def list_patho_bench_datasets(
    *,
    include_data_types: bool = False,
    as_dataframe: bool = True,
    client: PathoBenchClient | None = None,
):
    bench_client = client or PathoBenchClient(token=None)
    tasks_by_dataset = bench_client.fetch_available_splits()
    records: list[dict[str, Any]] = []
    for slug, info in PATHO_BENCH_DATASET_REGISTRY.items():
        record = {
            "dataset_name": slug,
            "display_name": info["display_name"],
            "source": "patho-bench",
            "metadata_source": f"{HF_BASE_URL}/datasets/{PATHO_BENCH_REPO_ID}",
            "raw_data_url": info["raw_data_url"],
            "raw_data_source": info["raw_data_source"],
        }
        if include_data_types:
            record["tasks"] = tasks_by_dataset.get(slug, [])
        records.append(record)
    df = pd.DataFrame(records).sort_values("dataset_name").reset_index(drop=True)
    return df if as_dataframe else df.to_dict(orient="records")


def list_patho_bench_tasks(
    dataset_name: str | None = None,
    *,
    as_dataframe: bool = True,
    client: PathoBenchClient | None = None,
):
    bench_client = client or PathoBenchClient(token=None)
    tasks_by_dataset = bench_client.fetch_available_splits()
    records: list[dict[str, Any]] = []
    datasets = (
        [_normalize_slug(dataset_name)] if dataset_name else sorted(tasks_by_dataset)
    )
    for slug in datasets:
        info = PATHO_BENCH_DATASET_REGISTRY.get(slug, {"display_name": slug})
        for task in tasks_by_dataset.get(slug, []):
            records.append(
                {
                    "dataset_name": slug,
                    "display_name": info["display_name"],
                    "task_name": task,
                    "source": "patho-bench",
                }
            )
    df = pd.DataFrame(records)
    return df if as_dataframe else df.to_dict(orient="records")


def download_patho_bench(
    dataset_name: str | Iterable[str],
    output_dir: str | Path = ".",
    *,
    task_name: str | None = None,
    download_raw_data: bool = False,
    download_workers: int | None = None,
    datatype: str | Iterable[str] | None = None,
    patient_id: str | None = None,
    study_instance_uid: str | None = None,
    series_instance_uid: str | None = None,
    body_part: str | None = None,
    manufacturer: str | None = None,
    log_transforms: bool = True,
) -> dict[str, Optional[Path]]:
    """Download Patho-Bench metadata and optionally delegate raw images.

    Patho-Bench on Hugging Face contains only benchmark metadata: per-task TSV
    splits and YAML descriptors. Raw image data lives on external dataset portals.
    For TCIA-backed Patho-Bench datasets, raw-image download can be delegated to
    the TCIA backend with ``download_raw_data=True``.
    """
    token = os.environ.get(HF_TOKEN_ENV)
    client = PathoBenchClient(token=token)
    tasks_by_dataset = client.fetch_available_splits()
    out_root = ensure_dir(output_dir)
    selected_task = task_name if task_name and task_name != "*" else None

    datasets = (
        [_normalize_slug(dataset_name)]
        if isinstance(dataset_name, str)
        else [_normalize_slug(name) for name in dataset_name]
    )
    multi_index: dict[str, dict[str, str]] = {}

    for slug in datasets:
        if slug not in tasks_by_dataset:
            raise ValueError(
                f"{slug} is not present in current Patho-Bench available_splits.yaml"
            )
        available_tasks = tasks_by_dataset[slug]
        if selected_task and selected_task not in available_tasks:
            raise ValueError(
                f"{selected_task} was not found in {slug}. Available tasks: {available_tasks}"
            )
        dataset_out = ensure_dir(Path(out_root) / slug)
        tree = client.list_repo_tree(path_in_repo=slug, recursive=True)
        files_to_download: list[str] = ["available_splits.yaml"]
        for entry in tree:
            if entry.get("type") != "file":
                continue
            repo_path = str(entry["path"])
            if not (
                repo_path.endswith(".tsv")
                or repo_path.endswith("config.yaml")
                or repo_path.endswith("about.yaml")
            ):
                continue
            if selected_task and f"/{selected_task}/" not in repo_path:
                if not repo_path.endswith("about.yaml"):
                    continue
            files_to_download.append(repo_path)

        local_paths: list[str] = []
        for repo_path in sorted(set(files_to_download)):
            target = dataset_out / repo_path
            client.download_file(repo_path, target_path=target)
            local_paths.append(str(target))

        metadata_files = pd.DataFrame(
            [
                {"repo_path": path, "local_path": str(dataset_out / path)}
                for path in sorted(set(files_to_download))
            ]
        )
        files_csv = to_csv(metadata_files, dataset_out / "files_metadata.csv")

        info = PATHO_BENCH_DATASET_REGISTRY[slug]
        raw_manifest = {
            "dataset_name": slug,
            "display_name": info["display_name"],
            "metadata_source": f"{HF_BASE_URL}/datasets/{PATHO_BENCH_REPO_ID}",
            "raw_data_source": info["raw_data_source"],
            "raw_data_url": info["raw_data_url"],
            "delegate_dataset_name": info.get("delegate_dataset_name"),
            "delegate_dataset_names": info.get("delegate_dataset_names"),
            "available_tasks": available_tasks,
            "selected_task": selected_task,
        }
        raw_manifest_path = save_json(
            raw_manifest, dataset_out / "raw_data_manifest.json"
        )

        delegated_raw_output = None
        if download_raw_data:
            if info.get("raw_data_source") == "tcia":
                delegate_dataset_names = info.get("delegate_dataset_names")
                delegate_dataset_name = (
                    delegate_dataset_names
                    if delegate_dataset_names
                    else info.get("delegate_dataset_name")
                )
                delegated_raw_output = download_tcia(
                    dataset_name=delegate_dataset_name,
                    output_dir=dataset_out / "raw_data",
                    datatype=datatype,
                    raw=False,
                    log_transforms=log_transforms,
                    patient_id=patient_id,
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    body_part=body_part,
                    manufacturer=manufacturer,
                    download_workers=download_workers,
                )
            else:
                log.warning(
                    "Patho-Bench raw image download for %s requires visiting %s",
                    slug,
                    info["raw_data_url"],
                )

        artifacts = {
            "files_csv": files_csv,
            "groups_csv": None,
            "manifest_tsv": None,
            "data_dir": dataset_out,
            "stats_json": None,
            "preview_json": None,
            "clinical_csv": None,
            "molecular_csv": None,
            "report_csv": None,
            "diagnosis_csv": None,
            "raw_data_manifest_json": raw_manifest_path,
            "delegated_raw_index_json": delegated_raw_output.get("multi_index_json")
            if isinstance(delegated_raw_output, dict)
            and "multi_index_json" in delegated_raw_output
            else None,
        }
        multi_index[slug] = {k: str(v) for k, v in artifacts.items() if v is not None}

    if log_transforms:
        run_log = {
            "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
            "source": "patho-bench",
            "datasets": datasets,
            "task_name": selected_task,
            "download_raw_data": download_raw_data,
            "download_workers": download_workers,
            "queries": client.last_queries,
        }
        save_json(run_log, Path(out_root) / "run_log.json")
        write_text_log(
            [
                "Source: Patho-Bench",
                f"Datasets: {', '.join(datasets)}",
                f"TaskName: {selected_task or '*'}",
                f"DownloadRawData: {download_raw_data}",
            ],
            Path(out_root) / "run_log.txt",
        )

    if len(datasets) == 1:
        return {
            key: (Path(value) if isinstance(value, str) else value)
            for key, value in multi_index[datasets[0]].items()
        }
    index_path = save_json(multi_index, Path(out_root) / "multi_index.json")
    return {"multi_index_json": index_path}
