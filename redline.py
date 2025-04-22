#!/usr/bin/env python3
"""
Redline — SIM‑Bundle Reconciliation (single‑file edition)
--------------------------------------------------------
* Upload three files (Supplier, iONLINE Raw, Customer Billing)
* Compares usage in absolute MB
* Shows data‑frames + lets you download a 4‑sheet Excel summary
"""

from __future__ import annotations

import datetime as dt
import io
import tempfile
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st
import xlsxwriter

# ----------------------------------------------------------------------
# Config  – tweak here if Finance changes tolerances / column names
# ----------------------------------------------------------------------
TOLERANCE_REALM_WARN_MB = 5
TOLERANCE_REALM_FAIL_MB = 20
TOLERANCE_CUST_WARN_MB = 10
TOLERANCE_CUST_FAIL_MB = 50

SCHEMA: Dict[str, Dict[str, List[str]]] = {
    "supplier": {
        "realm": ["realm"],
        "sim": ["sim_subscription", "subscription", "sim"],
        "data_mb": ["data_usage_mb", "data_mb", "usage_mb"],
    },
    "raw": {
        "customer": ["customer_code", "customer"],
        "realm": ["realm"],
        "sim": ["sim_subscription", "subscription", "sim"],
        "data_mb": ["data_usage_mb", "data_mb"],
    },
    "billing": {
        "customer": ["customer_code", "customer"],
        "realm": ["realm"],
        "sim": ["sim_subscription", "subscription", "sim"],
        "bundle_mb": ["bundle_mb", "bundle"],
        "excess_mb": ["excess_mb", "excess"],
    },
}

# ----------------------------------------------------------------------
# Helper utils
# ----------------------------------------------------------------------
def _std_cols(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> pd.DataFrame:
    """Rename first matching raw column to canonical name."""
    rename = {}
    for canon, candidates in mapping.items():
        for c in candidates:
            if c in df.columns:
                rename[c] = canon
                break
        else:
            raise ValueError(f"Missing required column '{canon}' (accepted {candidates})")
    return df.rename(columns=rename)


def _load_df(upload, key: str) -> pd.DataFrame:
    if upload is None:
        st.stop()
    df = pd.read_excel(upload) if upload.name.lower().endswith(("xls", "xlsx")) else pd.read_csv(upload)
    df.columns = [c.strip().lower() for c in df.columns]
    df = _std_cols(df, SCHEMA[key])
    # numeric coercion
    for col in [c for c in df.columns if c.endswith("_mb")]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def _status(delta: float, warn: int, fail: int) -> str:
    d = abs(delta)
    if d >= fail:
        return "FAIL"
    if d >= warn:
        return "WARN"
    return "OK"


def _agg(df: pd.DataFrame, group_cols: List[str], src: str, tgt: str) -> pd.DataFrame:
    return df.groupby(group_cols, as_index=False)[src].sum().rename(columns={src: tgt})


def _compare(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: List[str],
    lcol: str,
    rcol: str,
    warn: int,
    fail: int,
) -> pd.DataFrame:
    cmp = left.merge(right, on=on, how="outer").fillna(0.0)
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"] = cmp["delta_mb"].apply(lambda d: _status(d, warn, fail))
    return cmp


def _excel_report(tables: Dict[str, pd.DataFrame]) -> bytes:
    """Return a BytesIO Excel workbook with conditional formatting."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        styles = {
            "OK": wb.add_format({"bg_color": "#C6EFCE"}),
            "WARN": wb.add_format({"bg_color": "#FFEB9C"}),
            "FAIL": wb.add_format({"bg_color": "#FFC7CE"}),
        }
        for sheet, df in tables.items():
            df.to_excel(xl, sheet_name=sheet[:31], index=False)
            ws = xl.sheets[sheet[:31]]
            status_col = df.columns.get_loc("status")
            for key, fmt in styles.items():
                ws.conditional_format(
                    1,
                    status_col,
                    len(df),
                    status_col,
                    {"type": "text", "criteria": "containing", "value": key, "format": fmt},
                )
    buf.seek(0)
    return buf.read()


# ----------------------------------------------------------------------
# Streamlit UI  (Tally‑style)
# ----------------------------------------------------------------------
st.set_page_config(page_title="Redline — SIM‑Bundle Reconciliation", layout="centered")

st.title("Redline — Multi Usage Reconciliation")
st.caption(
    "Validate that MNO usage, iONLINE raw data, and customer billing all align."
)

# --- Upload widgets ---------------------------------------------------
col_sup, col_raw = st.columns(2)
with col_sup:
    sup_file = st.file_uploader("Supplier usage  (.xlsx / .csv)", key="sup")
with col_raw:
    raw_file = st.file_uploader("iONLINE raw usage  (.xlsx / .csv)", key="raw")

bill_file = st.file_uploader("Customer billing  (.xlsx / .csv)", key="bill")

run = st.button("Run reconciliation", disabled=not all([sup_file, raw_file, bill_file]))

if run:
    try:
        sup = _load_df(sup_file, "supplier")
        raw = _load_df(raw_file, "raw")
        bill = _load_df(bill_file, "billing")
    except Exception as e:
        st.error(f"File error: {e}")
        st.stop()

    # prep billed_mb
    bill["billed_mb"] = bill["bundle_mb"] + bill["excess_mb"]

    # aggregations
    sup_realm = _agg(sup, ["realm"], "data_mb", "supplier_mb")
    raw_realm = _agg(raw, ["realm"], "data_mb", "raw_mb")
    raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
    bill_realm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
    bill_cust = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

    # comparisons
    sup_vs_cust = _compare(
        sup_realm,
        bill_realm,
        ["realm"],
        "supplier_mb",
        "customer_billed_mb",
        TOLERANCE_REALM_WARN_MB,
        TOLERANCE_REALM_FAIL_MB,
    )
    raw_vs_cust = _compare(
        raw_cust,
        bill_cust,
        ["customer", "realm"],
        "raw_mb",
        "customer_billed_mb",
        TOLERANCE_CUST_WARN_MB,
        TOLERANCE_CUST_FAIL_MB,
    )
    sup_vs_raw = _compare(
        sup_realm,
        raw_realm,
        ["realm"],
        "supplier_mb",
        "raw_mb",
        TOLERANCE_REALM_WARN_MB,
        TOLERANCE_REALM_FAIL_MB,
    )

    # show tables
    st.subheader("Supplier vs Customer (realm)")
    st.dataframe(sup_vs_cust)
    st.subheader("Raw vs Customer (customer+realm)")
    st.dataframe(raw_vs_cust)
    st.subheader("Supplier vs Raw (realm)")
    st.dataframe(sup_vs_raw)

    # Excel download
    excel_bytes = _excel_report(
        {
            "supplier_vs_customer": sup_vs_cust,
            "raw_vs_customer": raw_vs_cust,
            "supplier_vs_raw": sup_vs_raw,
        }
    )
    st.download_button(
        "Download Excel report",
        data=excel_bytes,
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
