"""
Business logic: three comparisons, no side‑effects.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .flags import Status, evaluate


def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    return df.groupby(by, as_index=False)[src].sum().rename(columns={src: tgt})


def _compare(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: List[str],
    l_col: str,
    r_col: str,
    warn: float,
    fail: float,
) -> pd.DataFrame:
    df = left.merge(right, on=on, how="outer")
    df.fillna(0, inplace=True)
    df["delta_mb"] = df[l_col] - df[r_col]
    df["status"] = df["delta_mb"].apply(lambda d: evaluate(d, warn, fail).name)
    return df


def run(
    supplier: pd.DataFrame,
    raw: pd.DataFrame,
    billing: pd.DataFrame,
    cfg: Dict,
) -> Dict[str, pd.DataFrame]:
    # Pre‑compute billed MB
    billing["billed_mb"] = billing["bundle_mb"] + billing["excess_mb"]

    # Aggregations
    sup_realm = _agg(supplier, ["realm"], "data_mb", "supplier_mb")
    raw_realm = _agg(raw, ["realm"], "data_mb", "raw_mb")
    raw_cust = _agg(raw, ["customer_code", "realm"], "data_mb", "raw_mb")
    bill_realm = _agg(billing, ["realm"], "billed_mb", "customer_billed_mb")
    bill_cust = _agg(billing, ["customer_code", "realm"], "billed_mb", "customer_billed_mb")

    t = cfg["tolerances"]
    # Comparisons
    sup_vs_cust = _compare(
        sup_realm,
        bill_realm,
        ["realm"],
        "supplier_mb",
        "customer_billed_mb",
        t["realm_warn_mb"],
        t["realm_fail_mb"],
    )
    raw_vs_cust = _compare(
        raw_cust,
        bill_cust,
        ["customer_code", "realm"],
        "raw_mb",
        "customer_billed_mb",
        t["customer_warn_mb"],
        t["customer_fail_mb"],
    )
    sup_vs_raw = _compare(
        sup_realm,
        raw_realm,
        ["realm"],
        "supplier_mb",
        "raw_mb",
        t["realm_warn_mb"],
        t["realm_fail_mb"],
    )
    return {
        "supplier_vs_customer": sup_vs_cust,
        "raw_vs_customer": raw_vs_cust,
        "supplier_vs_raw": sup_vs_raw,
    } 