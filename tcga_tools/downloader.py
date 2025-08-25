from __future__ import annotations
import datetime as _dt
import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union
import time
import random
from http.client import IncompleteRead
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from urllib3.exceptions import ProtocolError

import pandas as pd
from tqdm import tqdm

from .api import GDCClient
from .filters import Filters as F
from .config import (
    DEFAULT_FILE_FIELDS,
    FILETYPE_PREFERENCES,
)
from .utils import ensure_dir, read_env_token, to_csv, flatten_hits, save_json, write_text_log, ensure_case_sample_columns
from .annotations import build_clinical_csv, build_molecular_index, build_reports_index, build_diagnosis_csv
from .grouping import build_patient_groups
from .analytics import generate_statistics_and_visualizations

log = logging.getLogger("tcga_tools")


DATATYPE_FILTERS = {
    "wsi": {
        "filetypes": [".svs"],
        "extra": [F.EQ("experimental_strategy", "Diagnostic Slide")],
    },
    "rna-seq": {
        "filetypes": None,  # leave user filetypes
        "extra": [F.EQ("data_type", "Gene Expression Quantification")],
    },
    "cnv": {
        "filetypes": None,
        "extra": [F.EQ("data_type", "Copy Number Segment")],
    },
}

def _filetype_filters(exts: Iterable[str]) -> List[Dict]:
    """Build a filter disjunction for any of the provided filetype extensions.
    Preference order: data_format/data_type; otherwise fall back to filename suffix match via OR of equality on 'file_name' end.
    """
    clauses: List[Dict] = []
    for ext in exts:
        ext = ext.lower()
        pref = FILETYPE_PREFERENCES.get(ext, {})
        if "data_format" in pref:
            clauses.append(F.IN("data_format", pref["data_format"]))
        elif "data_type" in pref:
            clauses.append(F.IN("data_type", pref["data_type"]))
        else:
            clauses.append(F.IN("file_name", [f"*{ext}"]))
    return clauses


def _files_query_filters(project_id: str, filetypes: Iterable[str]) -> Dict:
    type_filters = _filetype_filters(filetypes)
    ft = type_filters[0] if len(type_filters) == 1 else F.OR(*type_filters)
    return F.AND(
        F.EQ("cases.project.project_id", project_id),
        ft,
    )


def _patient_column(files_df: pd.DataFrame) -> pd.DataFrame:
    # Add a convenient 'patient' column (prefer submitter_id) for easy grouping
    if "cases.submitter_id" in files_df.columns:
        files_df["patient"] = files_df["cases.submitter_id"]
    elif "cases.case_id" in files_df.columns:
        files_df["patient"] = files_df["cases.case_id"]
    else:
        files_df["patient"] = pd.NA
    return files_df


def _collect_wsi_metadata(file_paths: List[Path]) -> pd.DataFrame:
    """Best-effort extraction of WSI metadata (vendor, mpp, objective, model) via OpenSlide.
    Returns empty DataFrame if openslide is unavailable.
    """
    try:
        import openslide  # type: ignore
    except Exception:
        return pd.DataFrame()

    rows = []
    for p in file_paths:
        if not p.suffix.lower().endswith("svs"):
            continue
        try:
            slide = openslide.OpenSlide(str(p))
            props = slide.properties
            vendor = props.get("openslide.vendor") or props.get("aperio.Manufacturer")
            model = props.get("aperio.Model") or props.get("hamamatsu.DeviceModel") or props.get("openslide.vendor")
            mpp_x = props.get("openslide.mpp-x") or props.get("aperio.MPP")
            mpp_y = props.get("openslide.mpp-y") or props.get("aperio.MPP")
            obj = props.get("openslide.objective-power") or props.get("aperio.AppMag") or props.get("hamamatsu.SourceLens")
            rows.append({
                "file_name": p.name,
                "wsi.vendor": vendor,
                "wsi.model": model,
                "wsi.mpp_x": float(mpp_x) if mpp_x is not None else None,
                "wsi.mpp_y": float(mpp_y) if mpp_y is not None else None,
                "wsi.objective_power": float(obj) if obj is not None else None,
            })
            slide.close()
        except Exception:
            continue
    return pd.DataFrame(rows)


def _robust_download_single(
    client: GDCClient,
    fid: str,
    target_path: Path,
    *,
    related_files: bool = True,
    expected_size: Optional[int] = None,
    max_retries: int = 5,
    base_sleep: float = 2.0,
    skip_existing: bool = True,
) -> bool:
    """
    Try to download one file with retries and atomic rename.
    Skips if target already exists and (optionally) matches expected_size.
    """
    # Skip if already there (+ size check if we have it)
    if skip_existing and target_path.exists():
        if expected_size is None:
            log.info(f"[skip] {target_path.name} already exists (no size check)")
            return True
        else:
            try:
                if target_path.stat().st_size == expected_size:
                    log.info(f"[skip] {target_path.name} already complete ({expected_size} bytes)")
                    return True
                else:
                    log.warning(
                        f"[redo] {target_path.name} exists but size mismatch "
                        f"({target_path.stat().st_size} != {expected_size}); re-downloading."
                    )
            except Exception:
                pass

    tmp = target_path.with_suffix(target_path.suffix + ".part")
    # Clean stale tmp
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass

    for attempt in range(1, max_retries + 1):
        try:
            client.download_single(fid, target_path=str(tmp), related_files=related_files)

            # Verify size if available
            if expected_size is not None:
                got = tmp.stat().st_size
                if got != expected_size:
                    raise IOError(f"Downloaded size {got} != expected {expected_size}")

            # Atomic replace
            os.replace(tmp, target_path)
            return True

        except (ChunkedEncodingError, ProtocolError, IncompleteRead, ConnectionError, ReadTimeout) as e:
            # Network/stream hiccup: backoff and retry
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            if attempt == max_retries:
                log.error(f"[fail] {target_path.name} after {attempt} attempts: {e}")
                return False
            sleep_s = base_sleep * (2 ** (attempt - 1)) * (1.0 + random.random() * 0.25)
            log.warning(f"[retry {attempt}/{max_retries}] {target_path.name}: {e} — sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

        except Exception as e:
            # Other unexpected errors: don't loop forever; surface immediately
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            log.error(f"[error] {target_path.name}: {e}")
            return False

    return False  # Shouldn't reach here

def _run_single_dataset(
    *,
    dataset_name: str,
    filetypes: Iterable[str],
    annotations: Iterable[str] | None,
    output_dir: Path,
    tar_archives: bool,
    related_files: bool,
    manifest_also: bool,
    fields: Iterable[str],
    raw: bool,
    statistics: bool,
    visualizations: bool,
    log_transforms: bool,
    client: GDCClient,
    datatype: str | list[str] | None = None
) -> Dict[str, Optional[Path]]:

    # --- datatype handling ---
    if datatype:
        dtypes = [datatype] if isinstance(datatype, str) else list(datatype)
        dtype_filters = []
        all_filetypes = []

        for dt in dtypes:
            spec = DATATYPE_FILTERS.get(dt.lower())
            if spec is None:
                raise ValueError(f"Unsupported datatype: {dt}")
            if spec["filetypes"]:
                all_filetypes.extend(spec["filetypes"])
            dtype_filters.append(F.AND(
                F.EQ("cases.project.project_id", dataset_name),
                *spec["extra"],
            ))

        # combine across datatypes with OR
        filters = F.OR(*dtype_filters)
        if all_filetypes:
            # if datatypes specify filetypes, enforce
            filetypes = all_filetypes

        # still append filetype filters if present
        if filetypes:
            ft_filters = _filetype_filters(filetypes)
            filters = F.AND(filters, ft_filters[0] if len(ft_filters) == 1 else F.OR(*ft_filters))

    else:
        filters = _files_query_filters(dataset_name, filetypes)
        
    # Print the list of filters used
    print("FILTERS:")
    print(filters)

    # 1) Enumerate files + metadata
    hits = client.paged_query("files", filters, fields)
    files_df = flatten_hits(hits)
    files_df = _ensure_case_sample_columns(files_df) 
    files_csv = to_csv(files_df, output_dir / "files_metadata.csv")
    # 2) Write groups.csv
    groups_df = build_patient_groups(files_df)
    print("GROUPS:")
    print(groups_df)
    groups_csv = to_csv(groups_df, output_dir / "groups.csv") if not groups_df.empty else None

    # 3) Download data (unless raw)
    out_data_dir = ensure_dir(output_dir / "data")
    paths_downloaded: List[Path] = []
    manifest_path: Optional[Path] = None

    if not raw and not files_df.empty:
        id_to_name = dict(zip(files_df.get("id", []), files_df.get("file_name", [])))
        id_to_size = dict(zip(files_df.get("id", []), files_df.get("file_size", []))) if "file_size" in files_df.columns else {}

        uuids = list(id_to_name.keys())
        if tar_archives and uuids:
            tar_path = output_dir / f"{dataset_name}_files.tar.gz"
            # Optional: you can skip if tar already exists.
            if not tar_path.exists():
                client.download_tar(uuids, target_path=str(tar_path), uncompressed=False)
            else:
                log.info(f"[skip] tar exists: {tar_path}")
            paths_downloaded.append(tar_path)
        else:
            failed: List[str] = []
            for fid, fname in tqdm(id_to_name.items(), desc=f"Downloading {dataset_name}"):
                target = out_data_dir / fname
                ok = _robust_download_single(
                    client,
                    fid,
                    target,
                    related_files=related_files,
                    expected_size=id_to_size.get(fid),  # may be None
                    max_retries=5,
                    base_sleep=2.0,
                    skip_existing=True,
                )
                if ok:
                    paths_downloaded.append(target)
                else:
                    failed.append(fname)
            if failed:
                log.warning(f"{len(failed)} file(s) failed to download after retries: {failed[:5]}{'...' if len(failed)>5 else ''}")

    # 3b) Optional WSI metadata enrichment
    wsi_meta_csv: Optional[Path] = None
    if not raw:
        svs_paths = [p for p in (output_dir / "data").glob("*.svs")]
        if svs_paths:
            wsi_df = _collect_wsi_metadata(svs_paths)
            if not wsi_df.empty:
                # merge into files_df, then overwrite files_metadata.csv
                files_df = files_df.merge(wsi_df, on="file_name", how="left")
                files_csv = to_csv(files_df, output_dir / "files_metadata.csv")
                wsi_meta_csv = to_csv(wsi_df, output_dir / "wsi_metadata.csv")

    # 4) Optional: annotations CSVs
    annos_csvs: dict[str, Optional[Path]] = {
        "clinical_csv": None,
        "molecular_csv": None,
        "report_csv": None,
        "diagnosis_csv": None,
        "wsi_meta_csv": wsi_meta_csv,
    }
    case_ids = set(files_df.get("cases.case_id", []).dropna().tolist()) if "cases.case_id" in files_df.columns else set()

    if annotations:
        annos = {a.lower() for a in annotations}
        if "all" in annos:
            annos = {"clinical", "molecular", "report", "diagnosis"}
        if case_ids:
            if "clinical" in annos:
                clinical_df = build_clinical_csv(client, case_ids)
                annos_csvs["clinical_csv"] = to_csv(clinical_df, output_dir / "clinical.csv")
            if "molecular" in annos:
                mol_df = build_molecular_index(client, dataset_name, case_ids)
                annos_csvs["molecular_csv"] = to_csv(mol_df, output_dir / "molecular_index.csv")
            if "report" in annos:
                rep_df = build_reports_index(client, dataset_name, case_ids)
                annos_csvs["report_csv"] = to_csv(rep_df, output_dir / "reports_index.csv")
            if "diagnosis" in annos:
                diag_df = build_diagnosis_csv(client, case_ids)
                annos_csvs["diagnosis_csv"] = to_csv(diag_df, output_dir / "diagnosis.csv")

    # 5) Stats + Visualizations
    stats_json: Optional[Path] = None
    figs: Dict[str, Path] | None = None
    if statistics or visualizations:
        clinical_df = None
        if annos_csvs["clinical_csv"]:
            try:
                clinical_df = pd.read_csv(annos_csvs["clinical_csv"])  # type: ignore[arg-type]
            except Exception:
                clinical_df = None
        stats_json, figs = generate_statistics_and_visualizations(
            files_df=files_df,
            groups_df=groups_df,
            clinical_df=clinical_df,
            output_dir=output_dir,
            make_plots=visualizations,
        )

    # 6) Optional preview for RAW
    preview_json = None
    if raw:
        preview = {
            "dataset_name": dataset_name,
            "filetypes": list(filetypes or []),   # <= guard here
            "files_found": int(len(files_df)),
            "example_files": files_df.head(10).to_dict(orient="records"),
            "grouping_summary": groups_df["group"].value_counts(dropna=False).to_dict() if not groups_df.empty else {},
            "case_ids": sorted(list(case_ids))[:20],
        }
        preview_json = save_json(preview, output_dir / "preview.json")

    return {
        "files_csv": files_csv,
        "groups_csv": groups_csv,
        "manifest_tsv": manifest_path,
        "data_dir": output_dir / "data",
        "stats_json": stats_json,
        "preview_json": preview_json,
        **annos_csvs,
    }


def download(
    dataset_name: Union[str, Iterable[str]],
    filetypes: list[str] | None = None,
    annotations: Iterable[str] | None = None,
    output_dir: str | os.PathLike = ".",
    *,
    tar_archives: bool = False,
    related_files: bool = True,
    manifest_also: bool = True,
    fields: Iterable[str] = DEFAULT_FILE_FIELDS,
    raw: bool = False,
    statistics: bool = False,
    visualizations: bool = False,
    log_transforms: bool = True,
    datatype: str | None = None
) -> Dict[str, Optional[Path]]:
    """High-level convenience wrapper used as ``tcga_tools.Download``.

    Supports a single dataset ("TCGA-LUSC") or a list like ["TCGA-LUSC", "TCGA-LUAD"].

    Returns
    -------
    dict
        For a single dataset: artifact paths for that dataset.
        For a list: returns a JSON index at ``<out>/multi_index.json`` and per-dataset subfolders.
    """
    out_root = ensure_dir(output_dir)
    token = read_env_token()
    client = GDCClient(token=token)


    if filetypes is None and (datatype and str(datatype).lower() == "wsi"):
        filetypes = [".svs"]
    
    # Normalize dataset list
    datasets: List[str] = list(dataset_name) if isinstance(dataset_name, (list, tuple, set)) else [str(dataset_name)]

    # For multiple datasets, iterate with a progress bar
    multi_index: Dict[str, Dict[str, str]] = {}
    all_files: List[pd.DataFrame] = []
    all_groups: List[pd.DataFrame] = []

    for ds in tqdm(datasets, desc="Datasets") if len(datasets) > 1 else datasets:
        ds_out = ensure_dir(Path(out_root) / ds)
        art = _run_single_dataset(
            dataset_name=ds,
            filetypes=filetypes,
            annotations=annotations,
            output_dir=ds_out,
            tar_archives=tar_archives,
            related_files=related_files,
            manifest_also=manifest_also,
            fields=fields,
            raw=raw,
            statistics=statistics,
            visualizations=visualizations,
            log_transforms=log_transforms,
            client=client,
            datatype=datatype
        )
        # Collect for aggregation
        multi_index[ds] = {k: str(v) for k, v in art.items() if v is not None}
        try:
            all_files.append(pd.read_csv(art["files_csv"]))  # type: ignore[arg-type]
        except Exception:
            pass
        try:
            if art["groups_csv"]:
                all_groups.append(pd.read_csv(art["groups_csv"]))  # type: ignore[arg-type]
        except Exception:
            pass

    # Aggregated CSVs at root
    if all_files:
        agg_files = pd.concat(all_files, ignore_index=True)
        to_csv(agg_files, Path(out_root) / "files_metadata.all.csv")
    if all_groups:
        agg_groups = pd.concat(all_groups, ignore_index=True)
        to_csv(agg_groups, Path(out_root) / "groups.all.csv")

    # Run log
    if log_transforms:
        run_log = {
            "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
            "datasets": datasets,
            "filetypes": list(filetypes or []),
            "annotations": list(annotations or []),
            "raw": raw,
            "statistics": statistics,
            "visualizations": visualizations,
            "tar_archives": tar_archives,
            "related_files": related_files,
            "fields_requested": list(fields),
            "queries": getattr(client, "last_queries", []),
        }
        save_json(run_log, Path(out_root) / "run_log.json")
        write_text_log([
            f"Datasets: {', '.join(datasets)}",
            f"Filetypes: {', '.join(filetypes or [])}",
            f"Annotations: {', '.join(annotations or [])}",
            f"Raw={raw}, Stats={statistics}, Viz={visualizations}",
        ], Path(out_root) / "run_log.txt")

    # If single dataset, return that artifact mapping; otherwise return index json path
    if len(datasets) == 1:
        return {k: (Path(v) if isinstance(v, str) else v) for k, v in multi_index[datasets[0]].items()}
    else:
        index_path = save_json(multi_index, Path(out_root) / "multi_index.json")
        return {"multi_index_json": index_path}

from typing import Any

def _ensure_case_sample_columns(files_df: pd.DataFrame) -> pd.DataFrame:
    """
    GDC /files returns nested arrays: cases[] and cases[].samples[].
    This function derives flat, analysis-ready columns:
      - cases.case_id
      - cases.submitter_id
      - cases.project.project_id
      - cases.samples.sample_type
    If multiple items exist, we take the *first* present (typical for TCGA SVS).
    """
    if "cases" in files_df.columns:
        def first_case(v: Any) -> dict:
            if isinstance(v, list) and v:
                return v[0] or {}
            return {}

        case_series = files_df["cases"].map(first_case)

        def get_case_project_id(c: dict) -> Optional[str]:
            pr = c.get("project") or {}
            return pr.get("project_id")

        def first_sample_type(c: dict) -> Optional[str]:
            samples = c.get("samples") or []
            for s in samples:
                st = s.get("sample_type")
                if st:
                    return st
            return None

        files_df["cases.case_id"] = case_series.map(lambda c: c.get("case_id"))
        files_df["cases.submitter_id"] = case_series.map(lambda c: c.get("submitter_id"))
        files_df["cases.project.project_id"] = case_series.map(get_case_project_id)
        files_df["cases.samples.sample_type"] = case_series.map(first_sample_type)

        # Optional: drop the raw nested column to avoid confusion
        # files_df = files_df.drop(columns=["cases"])

    # Ensure a convenient 'patient' column
    if "cases.submitter_id" in files_df.columns and files_df["cases.submitter_id"].notna().any():
        files_df["patient"] = files_df["cases.submitter_id"]
    elif "cases.case_id" in files_df.columns:
        files_df["patient"] = files_df["cases.case_id"]
    else:
        files_df["patient"] = pd.NA

    return files_df