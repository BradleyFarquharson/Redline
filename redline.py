#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
------------------------------------------------------------------
Upload three files → reconcile Supplier, iONLINE Raw, and Customer Billing
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

# ──────────────────────────── Config
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
        "date": ["date"],
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
    },
}

REALM_REGEX = re.compile(r"([A-Za-z]{2}\s?\d+)$", re.I)   # e.g. 'ZA 3', 'US1'
TOTAL_ROW_REGEX = re.compile(r"grand\s+total", re.I)


# ──────────────────────────── Helpers
def _auto_header_row(buf) -> int:
    df = pd.read_excel(buf, header=None, nrows=12, engine="openpyxl")
    idx = df[df.iloc[:, 0].astype(str).str.contains("Customer", case=False, na=False)].index
    if not len(idx):
        raise ValueError("Billing: couldn’t detect header row.")
    return int(idx[0])


def _std_cols(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Rename exactly one column to each canonical name, drop all other aliases.
    Ensures the canonical column exists and is unique.
    """
    for canon, aliases in mapping.items():
        # gather all cols that are either canonical already or in alias list
        hits = [c for c in df.columns if c == canon or c in aliases]
        if not hits:
            raise ValueError(f"Required column '{canon}' (aliases={aliases}) missing.")
        keep = hits[0]                           # first hit survives
        if keep != canon:                        # rename if needed
            df = df.rename(columns={keep: canon})
        # drop every other alias duplicate
        drop_cols = [c for c in hits if c != keep and c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
    return df


def _coerce_numeric(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": 0, "": 0})
        .astype(float)
        .fillna(0.0)
    )


# ─────────────  Loaders
def load_billing(up) -> pd.DataFrame:
    hdr = _auto_header_row(up)
    df = pd.read_excel(up, header=hdr, engine="openpyxl")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["billing"])

    df["qty"] = _coerce_numeric(df["qty"])
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


def load_raw(up) -> pd.DataFrame:
    df = pd.read_excel(up, engine="openpyxl")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["raw"])
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    df["realm"] = df["realm"].str.lower()
    df["carrier"] = df["carrier"].str.upper().fillna("UNKNOWN")
    return df


def load_supplier(up) -> pd.DataFrame:
    df = (
        pd.read_excel(up, engine="openpyxl")
        if up.name.lower().endswith(("xls", "xlsx"))
        else pd.read_csv(up)
    )
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, SCHEMA["supplier"])

    df = df[~df["realm"].astype(str).str.match(TOTAL_ROW_REGEX)]
    df["carrier"] = df["carrier"].str.upper()
    df["realm"] = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]


# ─────────────  Core logic
def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    return df.groupby(by, as_index=False)[src].sum().rename(columns={src: tgt})


def _status(delta: float, warn: int, fail: int) -> str:
    d = abs(delta)
    return "FAIL" if d >= fail else "WARN" if d >= warn else "OK"


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


def excel_report(tabs: Dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        styles = {k: wb.add_format({"bg_color": v}) for k, v in
                  {"OK": "#C6EFCE", "WARN": "#FFEB9C", "FAIL": "#FFC7CE"}.items()}
        for name, df in tabs.items():
            df.to_excel(xl, sheet_name=name[:31], index=False)
            ws = xl.sheets[name[:31]]
            col = df.columns.get_loc("status")
            for key, fmt in styles.items():
                ws.conditional_format(1, col, len(df), col,
                                      {"type": "text", "criteria": "containing", "value": key, "format": fmt})
    buf.seek(0)
    return buf.read()


# ──────────────────────────── Streamlit UI
st.set_page_config(page_title="Redline", layout="centered")
st.markdown(
    """
    <style>
    div.stFileUploader {margin-bottom: 1.3rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Cross-check MNO usage, iONLINE raw data, and customer billing.")

c1, c2 = st.columns(2)
with c1:
    sup_file = st.file_uploader("Supplier file", key="sup")
with c2:
    raw_file = st.file_uploader("Raw usage file", key="raw")
bill_file = st.file_uploader("Billing file", key="bill")

if st.button("Run", disabled=not all([sup_file, raw_file, bill_file])):
    try:
        sup = load_supplier(sup_file)
        raw = load_raw(raw_file)
        bill = load_billing(bill_file)
    except Exception as e:
        st.error(f"File error: {e}")
        st.stop()

    # Aggregations
    sup_realm = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
    raw_realm = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
    raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
    bill_realm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
    bill_cust = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

    # Comparisons
    sup_vs_raw = compare(sup_realm, raw_realm,
                         ["carrier", "realm"], "supplier_mb", "raw_mb",
                         TOLERANCE_REALM_WARN_MB, TOLERANCE_REALM_FAIL_MB)
    sup_vs_cust = compare(sup_realm, bill_realm,
                          ["realm"], "supplier_mb", "customer_billed_mb",
                          TOLERANCE_REALM_WARN_MB, TOLERANCE_REALM_FAIL_MB)
    raw_vs_cust = compare(raw_cust, bill_cust,
                          ["customer", "realm"], "raw_mb", "customer_billed_mb",
                          TOLERANCE_CUST_WARN_MB, TOLERANCE_CUST_FAIL_MB)

    # Display
    st.subheader("Supplier vs Raw  (carrier + realm)")
    st.dataframe(sup_vs_raw)
    st.subheader("Supplier vs Customer Billing  (realm)")
    st.dataframe(sup_vs_cust)
    st.subheader("Raw vs Customer Billing  (customer + realm)")
    st.dataframe(raw_vs_cust)

    # Excel download
    st.download_button(
        "Download Excel report",
        excel_report(
            {
                "supplier_vs_raw": sup_vs_raw,
                "supplier_vs_customer": sup_vs_cust,
                "raw_vs_customer": raw_vs_cust,
            }
        ),
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
