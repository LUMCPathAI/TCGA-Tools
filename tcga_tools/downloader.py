from __future__ import annotations
import datetime as _dt
import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

import pandas as pd
from tqdm import tqdm

from .api import GDCClient
from .filters import Filters as F
from .config import (
    DEFAULT_FILE_FIELDS,
    FILETYPE_PREFERENCES,
)
from .utils import ensure_dir, read_env_token, to_csv, flatten_hits, save_json, write_text_log
from .annotations import build_clinical_csv, build_molecular_index, build_reports_index, build_diagnosis_csv
from .grouping import build_patient_groups
from .analytics import generate_statistics_and_visualizations

log = logging.getLogger("tcga_tools")


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
) -> Dict[str, Optional[Path]]:
    filters = _files_query_filters(dataset_name, filetypes)

    # 1) Enumerate files + metadata
    hits = client.paged_query("files", filters, fields)
    files_df = flatten_hits(hits)
    files_df = _patient_column(files_df)
    files_csv = to_csv(files_df, output_dir / "files_metadata.csv")

    # 2) Write groups.csv
    groups_df = build_patient_groups(files_df)
    groups_csv = to_csv(groups_df, output_dir / "groups.csv") if not groups_df.empty else None

    # 3) Download data (unless raw)
    out_data_dir = ensure_dir(output_dir / "data")
    paths_downloaded: List[Path] = []
    manifest_path: Optional[Path] = None

    if not raw and not files_df.empty:
        id_to_name = dict(zip(files_df.get("id", []), files_df.get("file_name", [])))
        uuids = list(id_to_name.keys())
        if tar_archives and uuids:
            tar_path = output_dir / f"{dataset_name}_files.tar.gz"
            client.download_tar(uuids, target_path=str(tar_path), uncompressed=False)
            paths_downloaded.append(tar_path)
        else:
            for fid, fname in tqdm(id_to_name.items(), desc=f"Downloading {dataset_name}"):
                target = out_data_dir / fname
                client.download_single(fid, target_path=str(target), related_files=related_files)
                paths_downloaded.append(target)
        if manifest_also:
            manifest_path = output_dir / "gdc_manifest.tsv"
            client.download_manifest_for_query(filters, manifest_path=str(manifest_path))

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
            "filetypes": list(filetypes),
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
    filetypes: Iterable[str] = (".svs",),
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
            "filetypes": list(filetypes),
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
            f"Filetypes: {', '.join(filetypes)}",
            f"Annotations: {', '.join(annotations or [])}",
            f"Raw={raw}, Stats={statistics}, Viz={visualizations}",
        ], Path(out_root) / "run_log.txt")

    # If single dataset, return that artifact mapping; otherwise return index json path
    if len(datasets) == 1:
        return {k: (Path(v) if isinstance(v, str) else v) for k, v in multi_index[datasets[0]].items()}
    else:
        index_path = save_json(multi_index, Path(out_root) / "multi_index.json")
        return {"multi_index_json": index_path}
