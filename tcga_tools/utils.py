from __future__ import annotations
import json
import logging
import os
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

def flatten_hits(hits: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    # Flatten nested JSON to columns; explode lists into consistent columns where useful.
    return pd.json_normalize(hits, sep=".")