#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation  v2.4
----------------------------------------
Compares Supplier summaries, iONLINE raw logs and Customer Billing,
shows three delta tables + a formatted Excel download.
"""
from __future__ import annotations

import datetime as dt
import io
import re
from dataclasses import dataclass
from typing import Final, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ───────────────────────── Config
@dataclass(frozen=True)
class Threshold:
    warn: int
    fail: int


@dataclass(frozen=True)
class Config:
    REALM:    Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    BILLING_HEADER_ROW: Final[int] = 4  # 0-indexed

    # Regex:  “… - ZA 3”  OR  “: ZA 3”  at line end
    REGEX_REALM: Final[re.Pattern]  = re.compile(r'(?:\s-\s|:\s)([A-Za-z]{2}\s?\d+)$', re.I)
    REGEX_TOTAL: Final[re.Pattern]  = re.compile(r"grand\s+total", re.I)

    SCHEMA: Final[dict[str, dict[str, list[str]]]] = {
        "supplier": {
            "carrier":  ["carrier"],
            "realm":    ["realm"],
            "subs_qty": ["subscription_qty", "subscription", "subs_qty", "qty"],
            "data_mb":  ["total_mb", "data_mb", "usage_mb"],
        },
        "raw": {
            "date":     ["date"],
            "msisdn":   ["msisdn"],
            "sim":      ["sim_serial", "sim"],
            "customer": ["customer_code", "customer"],
            "realm":    ["realm"],
            "carrier":  ["carrier"],
            "data_mb":  ["total_usage_(mb)", "total_usage_mb", "usage_mb", "data_mb"],
            "status":   ["status"],
        },
        "billing": {
            "customer": ["customer_co", "customer_code", "customer"],
            "product":  ["product/service", "product_service", "product"],
            "qty":      ["qty", "quantity"],
        },
    }

    AUTO_WIDTH: Final[bool] = True


CFG = Config()

# ───────────────────────── Helper utils
def _seek_start(buf):          # ensure buffer pointer at 0
    try:
        buf.seek(0)
    except Exception:
        pass


def _std_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    drops: list[str] = []

    for canon, aliases in mapping.items():
        norm = lambda s: s.strip().lower().replace(" ", "_")
        targets = {norm(canon), *map(norm, aliases)}
        hits = [c for c in df.columns if norm(c) in targets]

        if not hits:
            raise ValueError(f"Required column '{canon}' (aliases={aliases}) missing")

        keep = hits[0]
        if keep != canon:
            if canon in df.columns:
                drops.append(keep)
            else:
                df = df.rename(columns={keep: canon})
        drops.extend(h for h in hits[1:] if h != canon)
    return df.drop(columns=list(set(drops)))


def _coerce_numeric(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
        .astype(float)
        .fillna(0.0)
    )


def _categorise(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("category")


# ───────────────────────── Generic reader
def _read_any(buf, **kw) -> pd.DataFrame:
    _seek_start(buf)
    name = getattr(buf, "name", "").lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **kw)
    if name.endswith(".csv"):
        return pd.read_csv(buf, encoding_errors="replace", **kw)
    raise ValueError(f"Unsupported file type: {name or '<buffer>'}")


# ───────────────────────── Loaders  (cached)
def _load_supplier(buf) -> pd.DataFrame:
    df = _read_any(buf)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["supplier"])

    df = df[~df["realm"].astype(str).str.match(CFG.REGEX_TOTAL, na=False)]
    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    _categorise(df, ["carrier", "realm"])
    return df[["carrier", "realm", "data_mb"]]


def _load_raw(buf) -> pd.DataFrame:
    df = _read_any(buf)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["raw"])

    df["data_mb"]  = _coerce_numeric(df["data_mb"])
    df["realm"]    = df["realm"].astype(str).str.lower()
    df["carrier"]  = df["carrier"].astype(str).str.upper().fillna("UNKNOWN")
    df["customer"] = df["customer"].astype(str).fillna("<nan>")

    _categorise(df, ["customer", "realm", "carrier"])
    return df[["customer", "realm", "carrier", "data_mb"]]


def _load_billing(buf) -> pd.DataFrame:
    _seek_start(buf)
    df = _read_any(buf, header=CFG.BILLING_HEADER_ROW)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"])

    df["qty"]      = _coerce_numeric(df["qty"])
    df["customer"] = df["customer"].astype(str).fillna("<nan>")

    prod = df["product"].astype(str)
    df["realm"] = prod.str.extract(CFG.REGEX_REALM, expand=False).str.lower().fillna("<nan>")

    df["bundle_mb"] = 0.0
    df["excess_mb"] = 0.0
    is_bundle = prod.str.contains("bundle", case=False, na=False)
    is_excess = prod.str.contains("excess", case=False, na=False)
    df.loc[is_bundle,                 "bundle_mb"] = df.loc[is_bundle, "qty"]
    df.loc[is_excess & ~is_bundle,    "excess_mb"] = df.loc[is_excess & ~is_bundle, "qty"]

    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    _categorise(df, ["customer", "realm"])
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# cache wrappers
@st.cache_data(show_spinner="Reading supplier file…")
def load_supplier(buf): return _load_supplier(buf)

@st.cache_data(show_spinner="Reading raw usage file…")
def load_raw(buf): return _load_raw(buf)

@st.cache_data(show_spinner="Reading billing file…")
def load_billing(buf): return _load_billing(buf)


# ───────────────────────── Aggregation / Comparison
def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    return (
        df.groupby(by, as_index=False, observed=True)[src]
        .sum()
        .rename(columns={src: tgt})
    )


def _status_series(delta: pd.Series, th: Threshold) -> pd.Series:
    bins = [0, th.warn, th.fail, np.inf]
    labels = ["OK", "WARN", "FAIL"]
    return pd.cut(delta.abs(), bins=bins, labels=labels, right=False, include_lowest=True).astype("category")


def compare(left: pd.DataFrame, right: pd.DataFrame,
            on: List[str], lcol: str, rcol: str, th: Threshold) -> pd.DataFrame:
    cmp = left.merge(right, on=on, how="outer").fillna(0.0)
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])

    cmp["delta_mb"]  = cmp[lcol] - cmp[rcol]
    cmp["pct_delta"] = np.where(cmp[rcol] == 0, np.nan,
                                cmp["delta_mb"] / cmp[rcol] * 100)
    cmp["status"]    = _status_series(cmp["delta_mb"], th)
    _categorise(cmp, ["status"])
    return cmp[on + [lcol, rcol, "delta_mb", "pct_delta", "status"]]


# ───────────────────────── Excel builder
def create_excel(tabs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        colour = {"OK": "#C6EFCE", "WARN": "#FFEB9C", "FAIL": "#FFC7CE"}
        text   = {"OK": "#006100", "WARN": "#9C6500", "FAIL": "#9C0006"}
        fmt = {k: wb.add_format({"bg_color": v, "font_color": text[k], "bold": True})
               for k, v in colour.items()}
        numfmt = wb.add_format({"num_format": "#,##0.00"})

        for name, df in tabs.items():
            if df.empty: continue
            df.to_excel(xl, sheet_name=name[:31], index=False)
            ws = xl.sheets[name[:31]]

            # conditional formatting on status
            if "status" in df.columns:
                col = df.columns.get_loc("status")
                for k, f in fmt.items():
                    ws.conditional_format(1, col, len(df), col,
                                          {"type": "cell", "criteria": "==", "value": f'"{k}"', "format": f})

            # numeric formatting + auto width
            for i, c in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[c]):
                    ws.set_column(i, i, None, numfmt)
                if CFG.AUTO_WIDTH:
                    width = min(max(len(str(c)), df[c].astype(str).str.len().max()) + 2, 60)
                    ws.set_column(i, i, width)

    buf.seek(0)
    return buf.read()


# ───────────────────────── UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")
st.markdown(
    """
    <style>
    div[data-testid="stFileDropzone"] > div > span {visibility:hidden;}
    div[data-testid="stFileDropzone"]::before {
        content:"Drop or browse…";
        position:absolute; top:45%; left:50%;
        transform:translate(-50%,-50%); font-size:0.9rem; color:white;
    }
    .block-container {padding-top:1.2rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Compares Supplier, Raw Usage and Customer Billing data.")

c1, c2, c3 = st.columns(3)
with c1: f_sup  = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"])
with c2: f_raw  = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"])
with c3: f_bill = st.file_uploader("Billing file",   type=["csv", "xls", "xlsx"])

if st.button("Run Reconciliation", disabled=not all((f_sup, f_raw, f_bill)), type="primary"):
    try:
        sup  = load_supplier(f_sup)
        raw  = load_raw(f_raw)
        bill = load_billing(f_bill)

        sup_realm      = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
        sup_realm_tot  = _agg(sup, ["realm"], "data_mb", "supplier_mb")
        raw_realm      = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
        raw_cust       = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
        bill_realm     = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
        bill_cust      = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

        sup_vs_raw  = compare(sup_realm,     raw_realm,
                              ["carrier", "realm"],
                              "supplier_mb", "raw_mb", CFG.REALM)
        sup_vs_cust = compare(sup_realm_tot, bill_realm,
                              ["realm"],
                              "supplier_mb", "customer_billed_mb", CFG.REALM)
        raw_vs_cust = compare(raw_cust,      bill_cust,
                              ["customer", "realm"],
                              "raw_mb",      "customer_billed_mb", CFG.CUSTOMER)

        st.subheader("Supplier vs Raw (carrier + realm)")
        st.dataframe(sup_vs_raw,  use_container_width=True)
        st.subheader("Supplier vs Billing (realm)")
        st.dataframe(sup_vs_cust, use_container_width=True)
        st.subheader("Raw vs Billing (customer + realm)")
        st.dataframe(raw_vs_cust, use_container_width=True)

        st.download_button(
            "⬇️ Download Excel report",
            create_excel({
                "Supplier_vs_Raw":      sup_vs_raw,
                "Supplier_vs_Customer": sup_vs_cust,
                "Raw_vs_Customer":      raw_vs_cust,
            }),
            file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(f"Reconciliation failed: {e}")

st.markdown("---")
st.caption(dt.datetime.now().strftime("Generated %A %B %d, %Y %I:%M %p"))
