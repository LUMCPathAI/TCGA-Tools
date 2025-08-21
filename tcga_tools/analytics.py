# tcga_tools/analytics.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

try:
    import matplotlib.pyplot as plt  # optional; only used when make_plots=True
except Exception:  # pragma: no cover
    plt = None  # type: ignore


def _safe_counts(series: Optional[pd.Series]) -> Dict[str, int]:
    """
    Value counts that tolerate missing/absent series.

    Parameters
    ----------
    series : pd.Series | None
        Series to count. If None or empty, returns an empty dict.
    """
    if series is None or len(series) == 0:
        return {}
    vc = series.fillna("<NA>").astype(str).value_counts(dropna=False)
    return {str(k): int(v) for k, v in vc.items()}


def _compute_survival_table(clinical_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Best-effort overall survival (OS) table from clinical annotations.

    Tries multiple likely column names. Returns a DataFrame with columns
    ['time', 'event'] if enough information is present, else None.
    """
    if clinical_df is None or clinical_df.empty:
        return None

    # Vital status
    vs = None
    for key in [
        "diagnoses.vital_status",
        "vital_status",
        "follow_ups.vital_status",
    ]:
        if key in clinical_df.columns:
            vs = clinical_df[key]
            break
    if vs is None:
        return None

    # Time-to-event proxies (days)
    dtd = None
    for key in [
        "diagnoses.days_to_death",
        "days_to_death",
    ]:
        if key in clinical_df.columns:
            dtd = pd.to_numeric(clinical_df[key], errors="coerce")
            break

    dtlf = None
    for key in [
        "diagnoses.days_to_last_follow_up",
        "days_to_last_follow_up",
        "follow_ups.days_to_last_follow_up",
    ]:
        if key in clinical_df.columns:
            dtlf = pd.to_numeric(clinical_df[key], errors="coerce")
            break

    if dtd is None and dtlf is None:
        return None

    df = pd.DataFrame({
        "vital_status": vs.astype(str).str.lower(),
        "days_to_death": dtd,
        "days_to_last_follow_up": dtlf,
    })
    df["event"] = df["vital_status"].eq("dead").astype(int)
    # Use the max of available durations as a pragmatic OS time proxy
    cols_present = [c for c in ["days_to_death", "days_to_last_follow_up"] if c in df.columns]
    df["time"] = df[cols_present].max(axis=1, skipna=True)
    df = df.dropna(subset=["time"]).reset_index(drop=True)
    if df.empty:
        return None
    return df[["time", "event"]]


# ------------------------------ plotting helpers ------------------------------

def _plot_bar(counts: Dict[str, int], title: str, out: Path):  # pragma: no cover (plots)
    if not counts or plt is None:
        return None
    labels = list(counts.keys())
    values = list(counts.values())
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.bar(range(len(labels)), values)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    return out


def _plot_km(surv_df: pd.DataFrame, out: Path):  # pragma: no cover (plots)
    if surv_df is None or surv_df.empty or plt is None:
        return None
    try:
        from lifelines import KaplanMeierFitter  # optional
    except Exception:
        return None
    kmf = KaplanMeierFitter()
    kmf.fit(surv_df["time"], event_observed=surv_df["event"])
    ax = kmf.plot(ci_show=True)
    fig = ax.get_figure()
    fig.savefig(out)
    plt.close(fig)
    return out


# ------------------------------ public API ------------------------------

def generate_statistics_and_visualizations(
    *,
    files_df: pd.DataFrame,
    groups_df: Optional[pd.DataFrame],
    clinical_df: Optional[pd.DataFrame],
    output_dir: Path,
    make_plots: bool = False,
) -> Tuple[Path, Dict[str, Path]]:
    """
    Compute dataset statistics and (optionally) save basic visualizations.

    Parameters
    ----------
    files_df : pd.DataFrame
        Flattened file metadata table (from /files hits).
    groups_df : pd.DataFrame | None
        Per-case grouping table (e.g., paired / tumor_only / normal_only).
    clinical_df : pd.DataFrame | None
        Flattened clinical annotations table (from /cases).
    output_dir : Path
        Directory to write 'stats.json' and optional 'figs/*.png'.
    make_plots : bool
        If True, also save bar plots and a KM curve (when feasible).

    Returns
    -------
    stats_path : Path
        Path to the saved JSON stats file.
    figs : dict[str, Path]
        Mapping of figure-name → output path (empty if make_plots=False or not feasible).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "n_files": int(len(files_df)),
        "n_cases": int(files_df.get("cases.case_id", pd.Series(dtype=object)).nunique()) if not files_df.empty else 0,
        "sample_type_counts": _safe_counts(files_df.get("cases.samples.sample_type")),
        "data_category_counts": _safe_counts(files_df.get("data_category")),
        "group_counts": _safe_counts(groups_df.get("group")) if groups_df is not None and not groups_df.empty else {},
    }

    # Survival summary
    surv_df = _compute_survival_table(clinical_df)
    if surv_df is not None and not surv_df.empty:
        stats["survival"] = {
            "n": int(len(surv_df)),
            "events": int(surv_df["event"].sum()),
            "time_days_min": float(surv_df["time"].min()),
            "time_days_median": float(surv_df["time"].median()),
            "time_days_max": float(surv_df["time"].max()),
        }

    stats_path = output_dir / "stats.json"
    stats_path.write_text(json.dumps(stats, indent=2, sort_keys=True))

    figs: Dict[str, Path] = {}
    if make_plots:
        figs_out = output_dir / "figs"
        figs_out.mkdir(parents=True, exist_ok=True)

        st = stats.get("sample_type_counts", {})
        if st:
            out = _plot_bar(st, "Sample type distribution", figs_out / "sample_type_distribution.png")
            if out:
                figs["sample_type_bar"] = out

        dc = stats.get("data_category_counts", {})
        if dc:
            out = _plot_bar(dc, "Data category distribution", figs_out / "data_category_distribution.png")
            if out:
                figs["data_category_bar"] = out

        if surv_df is not None and not surv_df.empty:
            out = _plot_km(surv_df, figs_out / "survival_km.png")
            if out:
                figs["survival_km"] = out

    return stats_path, figs
