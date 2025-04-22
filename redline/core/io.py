"""
File‑loading & schema‑validation helpers.
All functions are pure – no Streamlit or logging side‑effects.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import yaml

SCHEMA_PATH = Path(__file__).parent.parent.parent / "config" / "schema.yaml"


class SchemaError(RuntimeError):
    """Raised when a required logical column is missing."""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_schema() -> Dict[str, Dict]:
    with SCHEMA_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


_SCHEMA = _load_schema()


def _std_cols(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> pd.DataFrame:
    rename = {}
    for canon, candidates in mapping.items():
        for raw in candidates:
            if raw in df.columns:
                rename[raw] = canon
                break
        else:
            raise SchemaError(f"Missing required column '{canon}' (accepted {candidates})")
    return df.rename(columns=rename)


def read_file(path: str | Path, key: str) -> Tuple[pd.DataFrame, Dict]:
    """
    Load CSV/XLSX -> DataFrame with canonical headings + type coercion.

    Args:
        path: file path
        key: one of 'supplier' | 'raw' | 'billing'

    Returns:
        df, meta (dict with checksum & row‑count)
    """
    path = Path(path)
    use_arrow = {"dtype_backend": "pyarrow"}

    if path.suffix.lower() in {".csv", ".txt"}:
        df = pd.read_csv(path, **use_arrow)
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, engine="openpyxl", **use_arrow)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    cfg = _SCHEMA[key]
    df = _std_cols(df, cfg["columns"])

    # numeric coercion
    for col in cfg["numeric"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64[pyarrow]")

    return df, {"checksum": _sha256(path), "rows": len(df), "file": str(path)} 