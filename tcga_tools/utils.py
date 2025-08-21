from __future__ import annotations
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from .config import GDC_TOKEN_ENV

logger = logging.getLogger("tcga_tools")


def ensure_dir(path: os.PathLike | str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_env_token() -> Optional[str]:
    token = os.environ.get(GDC_TOKEN_ENV)
    if token:
        logger.info("Using token from env var %s", GDC_TOKEN_ENV)
    else:
        logger.info("No %s in environment; only open-access files can be downloaded.", GDC_TOKEN_ENV)
    return token


def to_csv(df: pd.DataFrame, path: os.PathLike | str) -> Path:
    path = Path(path)
    df.to_csv(path, index=False)
    logger.info("Wrote %s (%d rows, %d cols)", path, df.shape[0], df.shape[1])
    return path


def save_json(obj: Dict[str, Any], path: os.PathLike | str) -> Path:
    path = Path(path)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))
    logger.info("Wrote JSON %s", path)
    return path


def write_text_log(lines: Iterable[str], path: os.PathLike | str) -> Path:
    path = Path(path)
    Path(path).write_text("\n".join(lines))
    logger.info("Wrote log %s", path)
    return path


def flatten_hits(hits: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    # Flatten nested JSON to columns; explode lists into consistent columns where useful.
    return pd.json_normalize(hits, sep=".")

def ensure_case_sample_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure scalar columns for case/sample linkage exist on DataFrames coming
    from the /files endpoint, where 'cases' is often a list of dicts.

    Adds (if missing):
      - cases.case_id
      - cases.submitter_id
      - cases.project.project_id
      - cases.samples.sample_type
      - cases.samples.sample_id
    """
    cols = set(df.columns)

    # 1) Rename patterns like 'cases.0.case_id' -> 'cases.case_id'
    renames: Dict[str, str] = {}
    for c in list(cols):
        m = re.match(r"^cases\.\d+\.(.+)$", c)
        if m:
            renames[c] = f"cases.{m.group(1)}"
    if renames:
        df = df.rename(columns=renames)
        cols = set(df.columns)

    # 2) If already present as scalar columns, nothing to do.
    if "cases.submitter_id" in cols or "cases.case_id" in cols:
        return df

    # 3) If we only have a raw 'cases' list column, extract first element.
    if "cases" in cols:
        def first_case(obj):
            if isinstance(obj, list) and obj:
                return obj[0] or {}
            if isinstance(obj, dict):
                return obj
            return {}

        c = df["cases"].map(first_case)
        df["cases.case_id"] = c.map(lambda x: x.get("case_id"))
        df["cases.submitter_id"] = c.map(lambda x: x.get("submitter_id"))
        df["cases.project.project_id"] = c.map(lambda x: (x.get("project") or {}).get("project_id"))

        def first_sample(d):
            s = d.get("samples")
            if isinstance(s, list) and s:
                return s[0] or {}
            return {}

        s = c.map(first_sample)
        df["cases.samples.sample_type"] = s.map(lambda x: x.get("sample_type"))
        df["cases.samples.sample_id"]   = s.map(lambda x: x.get("sample_id"))

    return df