#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
------------------------------------------------------------------
* Upload three files  (Supplier, iONLINE Raw, Customer Billing)
* Uses every row (no date slicing yet)
* Reconciles usage across carriers, realms, and customers
* Shows on-screen deltas + lets you download a 5-sheet Excel report
"""

from __future__ import annotations

import datetime as dt
import io
import re
from typing import Dict, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ───────────────────────────────────────────────────────────────
# Config  – edit tolerances / column aliases here
# ───────────────────────────────────────────────────────────────
TOLERANCE_REALM_WARN_MB = 5
TOLERANCE_REALM_FAIL_MB = 20
TOLERANCE_CUST_WARN_MB = 10
TOLERANCE_CUST_FAIL_MB = 50

SCHEMA: Dict[str, Dict[str, List[str]]] = {
    "supplier": {
        "carrier": ["carrier"],
        "realm": ["realm"],
        "subs_qty": ["subscription_qty", "subscription", "subs_qty", "qty"],
        "data_mb": ["total_mb", "data_mb", "usage_mb"],
    },
    "raw": {
        "date": ["date"],  # retained for future filters
        "msisdn": ["msisdn"],
        "sim": ["sim_serial", "sim"],
        "customer": ["customer_code", "customer"],
        "realm": ["realm"],
        "carrier": ["carrier"],
        "data_mb": ["total_usage_(mb)", "total_usage_mb", "usage_mb", "data_mb"],
        "status": ["status"],
    },
    "billing": {
        "customer": ["customer_co", "customer_code", "customer"],
        "product": ["product/service", "product_service", "product"],
        "qty": ["qty", "quantity"],
        # realm / bundle_mb / excess_mb derived later
    },
}

REALM_REGEX = re.compile(r"([A-Za-z]{2}\s?\d+)$", re.I)          # last token “ZA 3”, “US1”
TOTAL_ROW_REGEX = re.compile(r"grand\s+total", re.I)             # supplier summary rows


# ───────────────────────────────────────────────────────────────
# Helper utilities
# ───────────────────────────────────────────────────────────────
def _auto_header_row(path_or_buf) -> int:
    """Return the row idx where column A first contains 'Customer'."""
    hdr_scan = pd.read_excel(path_or_buf, header=None, nrows=12, engine="openpyxl")
    idx = hdr_scan[hdr_scan.iloc[:, 0].astype(str).str.contains("Customer", case=False, na=False)].index
    if not len(idx):
        raise ValueError("Could not auto-detect header row in billing file.")
    return int(idx[0])


def _std_cols(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Rename first matching alias to canonical name **and drop all other aliases**.
    Guarantees no duplicate canonical columns.
    """
    rename, drop = {}, []
    for canon, aliases in mapping.items():
        chosen = None
        for col in aliases:
            if col in df.columns:
                if chosen is None:
                    rename[col] = canon
                    chosen = col
                else:
                    drop.append(col)  # duplicate alias → drop
        # existing canon column (no alias rename) counts as chosen
        if canon in df.columns and canon not in rename.values():
            chosen = canon
        if chosen is None:
            raise ValueError(f"Missing required column '{canon}' (aliases={aliases})")
    df = df.rename(columns=rename).drop(columns=drop)
    return df


def _coerce_numeric(series: pd.Series) -> pd.Series:
    """Turn '1,024.00' or '-' into float MB."""
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .replace("-", 0)
        .replace("", 0)
        .astype(float)
        .fillna(0.0)
    )


# ─────────────  Billing  ─────────────
def load_billing(upload) -> pd.DataFrame:
    hdr = _auto_header_row(upload)
    df = pd.read_excel(upload, header=hdr, engine="openpyxl")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["billing"])

    df["qty"] = _coerce_numeric(df["qty"])

    # realm
    df["realm"] = (
        df["product"]
        .astype(str)
        .str.lower()
        .str.extract(REALM_REGEX, expand=False)
        .str.lower()
    )
    df["bundle_mb"] = np.where(df["product"].str.contains("bundle", case=False, na=False), df["qty"], 0.0)
    df["excess_mb"] = np.where(df["product"].str.contains("excess", case=False, na=False), df["qty"], 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# ─────────────  Raw Usage  ─────────────
def load_raw(upload) -> pd.DataFrame:
    df = pd.read_excel(upload, engine="openpyxl")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["raw"])

    df["data_mb"] = _coerce_numeric(df["data_mb"])
    df["realm"] = df["realm"].str.lower()
    df["carrier"] = df["carrier"].str.upper().fillna("UNKNOWN")
    return df


# ─────────────  Supplier Usage  ─────────────
def load_supplier(upload) -> pd.DataFrame:
    df = (
        pd.read_excel(upload, engine="openpyxl")
        if upload.name.lower().endswith(("xls", "xlsx"))
        else pd.read_csv(upload)
    )
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["supplier"])

    df = df[~df["realm"].astype(str).str.match(TOTAL_ROW_REGEX)]   # drop summary
    df["carrier"] = df["carrier"].str.upper()
    df["realm"] = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]


# ─────────────  Aggregation & Compare  ─────────────
def _agg(df: pd.DataFrame, group_cols: List[str], src: str, tgt: str) -> pd.DataFrame:
    return df.groupby(group_cols, as_index=False)[src].sum().rename(columns={src: tgt})


def _status(delta: float, warn: int, fail: int) -> str:
    d = abs(delta)
    if d >= fail:
        return "FAIL"
    if d >= warn:
        return "WARN"
    return "OK"


def compare(
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


# ─────────────  Excel Report  ─────────────
def excel_report(tables: Dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        col_fmt = {"OK": "#C6EFCE", "WARN": "#FFEB9C", "FAIL": "#FFC7CE"}
        styles = {k: wb.add_format({"bg_color": v}) for k, v in col_fmt.items()}

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


# ───────────────────────────────────────────────────────────────
# Streamlit UI
# ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Redline", layout="centered")

# simple CSS tweak for breathing-room
st.markdown(
    """
    <style>
    div.stFileUploader > label {margin-bottom: 0.4rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Cross-check MNO usage, iONLINE raw data, and customer billing.")

col1, col2 = st.columns(2)
with col1:
    sup_file = st.file_uploader("Supplier usage (.xlsx / .csv)", key="sup")
with col2:
    raw_file = st.file_uploader("iONLINE raw usage (.xlsx)", key="raw")
bill_file = st.file_uploader("Customer billing (.xlsx)", key="bill")

run = st.button("Run", disabled=not all([sup_file, raw_file, bill_file]))

if run:
    try:
        sup = load_supplier(sup_file)
        raw = load_raw(raw_file)
        bill = load_billing(bill_file)
    except Exception as e:
        st.error(f"File error: {e}")
        st.stop()

    # ═══ Aggregations ════════════════════════════════════════════
    sup_realm = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
    raw_realm = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
    raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
    bill_realm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
    bill_cust = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

    # ═══ Comparisons ════════════════════════════════════════════
    sup_vs_raw = compare(
        sup_realm,
        raw_realm,
        ["carrier", "realm"],
        "supplier_mb",
        "raw_mb",
        TOLERANCE_REALM_WARN_MB,
        TOLERANCE_REALM_FAIL_MB,
    )
    sup_vs_cust = compare(
        sup_realm,
        bill_realm,
        ["realm"],
        "supplier_mb",
        "customer_billed_mb",
        TOLERANCE_REALM_WARN_MB,
        TOLERANCE_REALM_FAIL_MB,
    )
    raw_vs_cust = compare(
        raw_cust,
        bill_cust,
        ["customer", "realm"],
        "raw_mb",
        "customer_billed_mb",
        TOLERANCE_CUST_WARN_MB,
        TOLERANCE_CUST_FAIL_MB,
    )

    # ═══ UI Output ══════════════════════════════════════════════
    st.subheader("Supplier vs Raw  (carrier + realm)")
    st.dataframe(sup_vs_raw)
    st.subheader("Supplier vs Customer Billing  (realm)")
    st.dataframe(sup_vs_cust)
    st.subheader("Raw vs Customer Billing  (customer + realm)")
    st.dataframe(raw_vs_cust)

    # Excel download
    excel_bytes = excel_report(
        {
            "supplier_vs_raw": sup_vs_raw,
            "supplier_vs_customer": sup_vs_cust,
            "raw_vs_customer": raw_vs_cust,
        }
    )
    st.download_button(
        "Download Excel report",
        data=excel_bytes,
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
