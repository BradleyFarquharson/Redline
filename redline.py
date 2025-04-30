#!/usr/bin/env python3
"""
Redline — SIM‑Bundle Reconciliation v2.1
=======================================
Cross‑checks Supplier summaries, iONLINE raw logs and Customer Billing, then
shows three delta tables plus a formatted Excel file.  This revision merges the
refactor in the previous canvas with the most robust pieces of the legacy draft
(_std_cols, _coerce_numeric, NA‑safe aggregation, merge‑key validation,
auto‑width columns).
"""
from __future__ import annotations

import datetime as dt
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ──────────────────────────────── Configuration
@dataclass(frozen=True)
class Threshold:  # comparison thresholds per grouping level
    warn: int
    fail: int


@dataclass(frozen=True)
class Config:
    REALM: Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    REGEX_REALM: Final[re.Pattern] = re.compile(r"([A-Za-z]{2}\s?\d+)$", re.I)
    REGEX_TOTAL: Final[re.Pattern] = re.compile(r"grand\s+total", re.I)

    SCHEMA: Final[dict[str, dict[str, list[str]]]] = {
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

    AUTO_WIDTH: Final[bool] = True  # autosize columns in Excel (slower)


CFG = Config()

# ──────────────────────────────── Helpers

def _seek_start(buf):
    try:
        buf.seek(0)
    except Exception:
        pass


# smarter duplicate‑aware column normaliser
def _std_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    cols_to_drop: list[str] = []
    current = df.columns.tolist()

    for canon, aliases in mapping.items():
        hits = [c for c in current if c.strip().lower().replace(" ", "_") in {canon, *aliases}]
        if not hits:
            raise ValueError(f"Required column '{canon}' (aliases={aliases}) missing.")
        keep = hits[0]
        if keep.strip().lower().replace(" ", "_") != canon:
            df = df.rename(columns={keep: canon})
        cols_to_drop.extend(hits[1:])
    if cols_to_drop:
        df = df.drop(columns=list(set(cols_to_drop)))
    return df


# robust numeric coercion
def _coerce_numeric(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    cleaned = (
        s.fillna("").astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0).astype(float)


def _categorise(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("category")


def _assert_keys(df: pd.DataFrame, keys: List[str], side: str = "df") -> None:
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{side} missing merge key(s): {missing}")


# aggregation that preserves rows with NaNs in the group keys
def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    miss = [c for c in by if c not in df.columns]
    if miss:
        raise ValueError(f"Aggregation key(s) missing: {miss}")
    if src not in df.columns:
        raise ValueError(f"Aggregation source '{src}' missing")

    df_filled = df.copy()
    for col in by:
        df_filled[col] = df_filled[col].fillna("<nan>").astype(str)

    return (
        df_filled.groupby(by, as_index=False, observed=True)[src]
        .sum()
        .rename(columns={src: tgt})
    )


# status binning
def _status_series(delta: pd.Series, th: Threshold) -> pd.Series:
    bins = [-np.inf, th.warn, th.fail, np.inf]
    return pd.cut(delta.abs(), bins=bins, labels=["OK", "WARN", "FAIL"]).astype("category")


# ──────────────────────────────── Loaders

def _load_excel_or_csv(buf, **read_kwargs) -> pd.DataFrame:
    name = buf.name.lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **read_kwargs)
    if name.endswith(".csv"):
        return pd.read_csv(buf, **read_kwargs)
    raise ValueError(f"Unsupported file type: {buf.name}")


def load_supplier(buf) -> pd.DataFrame:
    with st.spinner("Reading Supplier…"):
        df = _load_excel_or_csv(buf)
        _seek_start(buf)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["supplier"])

    df = df[~df["realm"].astype(str).str.match(CFG.REGEX_TOTAL, na=False)]
    df["carrier"] = df["carrier"].str.upper()
    df["realm"] = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    _categorise(df, ["carrier", "realm"])
    return df[["carrier", "realm", "data_mb"]]


def load_raw(buf) -> pd.DataFrame:
    with st.spinner("Reading Raw usage…"):
        df = _load_excel_or_csv(buf)
        _seek_start(buf)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["raw"])
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    df["realm"] = df["realm"].str.lower()
    df["carrier"] = df["carrier"].str.upper().fillna("UNKNOWN")
    _categorise(df, ["customer", "realm", "carrier"])
    return df


def _detect_header_row(buf) -> int:
    tmp = pd.read_excel(buf, header=None, nrows=12, engine="openpyxl")
    hit = tmp[tmp.iloc[:, 0].astype(str).str.contains("customer", case=False, na=False)]
    if hit.empty:
        raise ValueError("Billing: header row not found (looking for 'Customer').")
    return int(hit.index[0])


def load_billing(buf) -> pd.DataFrame:
    hdr = _detect_header_row(buf)
    _seek_start(buf)
    with st.spinner("Reading Billing…"):
        df = _load_excel_or_csv(buf, header=hdr)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"])

    df["qty"] = _coerce_numeric(df["qty"])
    df["realm"] = (
        df["product"].astype(str).str.lower().str.extract(CFG.REGEX_REALM, expand=False)
    )
    df["bundle_mb"] = 0.0
    df["excess_mb"] = 0.0
    is_bundle = df["product"].astype(str).str.contains("bundle", case=False, na=False)
    is_excess = df["product"].astype(str).str.contains("excess", case=False, na=False)
    df.loc[is_bundle, "bundle_mb"] = df.loc[is_bundle, "qty"]
    df.loc[is_excess & ~is_bundle, "excess_mb"] = df.loc[is_excess & ~is_bundle, "qty"]
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    _categorise(df, ["customer", "realm"])
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# ──────────────────────────────── Comparison / Excel

def compare(left: pd.DataFrame, right: pd.DataFrame, on: List[str], lcol: str, rcol: str, th: Threshold) -> pd.DataFrame:
    _assert_keys(left, on, "left")
    _assert_keys(right, on, "right")
    for c, side in ((lcol, "left"), (rcol, "right")):
        if c not in (left if side == "left" else right).columns:
            raise ValueError(f"{side} missing '{c}' column")

    cmp = left.merge(right, on=on, how="outer", copy=False).fillna(0.0)
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"] = _status_series(cmp["delta_mb"], th)
    _categorise(cmp, ["status"])
    return cmp


def create_excel(tabs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        cell_fmt = {
            k: wb.add_format({"bg_color": v, "font_color": c})
            for k, v, c in (
                ("OK", "#C6EFCE", "#006100"),
                ("WARN", "#FFEB9C", "#9C6500"),
                ("FAIL", "#FFC7CE", "#9C0006"),
            )
        }

        for name, df in tabs.items():
            sheet = name[:31]
            df.to_excel(xl, sheet_name=sheet, index=False)
            ws = xl.sheets[sheet]
            if "status" in df.columns:
                col = df.columns.get_loc("status")
                n = len(df)
                for key, fmt in cell_fmt.items():
                    ws.conditional_format(1, col, n, col, {
                        "type": "text", "criteria": "containing", "value": key, "format": fmt,
                    })
            if CFG.AUTO_WIDTH:
                for i, col_name in enumerate(df.columns):
                    width = max(df[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    ws.set_column(i, i, width)
    buf.seek(0)
    return buf.read()


# ──────────────────────────────── Streamlit UI
st.set_page_config(page_title="Redline", layout="centered")

st.markdown(
    """
    <style>
    div[data-testid="stFileDropzone"] > div > span {visibility:hidden;}
    div[data-testid="stFileDropzone"]::before {
        content:"Drop or browse…";position:absolute;top:45%;left:50%;transform:translate(-50%,-50%);font-size:0.9rem;color:white;}
    .block-container{padding-top:1.4rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redline — Multi‑Source Usage Reconciliation")
st.caption("Supplier vs Raw vs Billing usage integrity check.")

sup_col, raw_col, bill_col = st.columns(3)
with sup_col:
    f_sup = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"], key="sup")
with raw_col:
    f_raw = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"], key="raw")
with bill_col:
    f_bill = st.file_uploader("Billing file", type=["csv", "xls", "xlsx"], key="bill")

run = st.button("Run Reconciliation", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    try:
        sup = load_supplier(f_sup)
        raw = load_raw(f_raw)
        bill = load_billing(f_bill)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    # ── Aggregations
    sup_realm = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
    sup_realm_tot = _agg(sup, ["realm"], "data_mb", "supplier_mb")

    raw_realm = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
    raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")

    bill_realm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
    bill_cust = _agg(b
