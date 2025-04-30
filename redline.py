#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
==================================
Upload the three source files (Supplier, Raw usage, Customer billing),
hit •Run• and download a single Excel workbook with the three comparison tabs.
"""

from __future__ import annotations
import datetime as dt, io, re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ───────────────────────────────  CONFIG & CONSTANTS
@dataclass(frozen=True)
class _Th:
    warn: int
    fail: int


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)
    CUSTOMER: _Th = _Th(10, 50)
    BILL_HDR: int = 4           # header row (0-index) in Billing file
    REALM_RX: re.Pattern = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I)

    SCHEMA: dict[str, dict[str, List[str]]] = field(default_factory=lambda: {
        "supplier": {
            "carrier":  ["carrier"],
            "realm":    ["realm"],
            "subs_qty": ["subscription_qty", "subscription", "qty"],
            "data_mb":  ["total_mb", "usage_mb", "total_usage_mb",
                         "total_usage_(mb)", "total_usage", "data_mb"],
        },
        "raw": {
            "date":     ["date"],
            "msisdn":   ["msisdn"],
            "sim":      ["sim_serial", "sim"],
            "customer": ["customer_code", "customer"],
            "realm":    ["realm"],
            "carrier":  ["carrier"],
            "data_mb":  ["total_usage_(mb)", "total_usage_mb", "usage_mb",
                         "data_mb", "total_mb"],
            "status":   ["status"],
        },
        "billing": {
            "customer": ["customer_co", "customer_code", "customer"],
            "product":  ["product/service", "product_service", "product"],
            "qty":      ["qty", "quantity"],
        },
    })


CFG = _Cfg()

# ───────────────────────────────  SIMPLE HELPERS
def _n(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def _coerce_numeric(s: pd.Series) -> pd.Series:
    cleaned = (s.fillna("")
                 .astype(str)
                 .str.replace(",", "", regex=False)
                 .replace({"-": "0", "": "0"}))
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0).astype(float)


def _std_cols(df: pd.DataFrame, mapping: dict[str, List[str]], fname: str) -> pd.DataFrame:
    df = df.copy()
    for canon, aliases in mapping.items():
        hits = [c for c in df.columns if _n(c) in {_n(canon), *map(_n, aliases)}]
        if not hits:
            raise ValueError(f"Required column '{canon}' not found in {fname}")
        keep, dupes = hits[0], hits[1:]
        df.rename(columns={keep: canon}, inplace=True)
        if dupes:
            df.drop(columns=dupes, inplace=True)
    return df


def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    return (df.assign(**{c: df[c].fillna("<nan>").astype("category") for c in by})
              .groupby(by, as_index=False, observed=True)[src]
              .sum()
              .rename(columns={src: tgt}))


def _status(delta: pd.Series, th: _Th) -> pd.Series:
    bins, labels = [0, th.warn, th.fail, np.inf], ["OK", "WARN", "FAIL"]
    return pd.cut(delta.abs(), bins=bins, labels=labels,
                  right=False, include_lowest=True).astype("category")


def _read_any(buf, fname: str, **kw) -> pd.DataFrame:
    ext = Path(fname).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, engine="openpyxl", **kw)
    if ext == ".csv":
        return pd.read_csv(buf, encoding_errors="replace", **kw)
    raise ValueError(f"Unsupported file type: {ext} for {fname}")

# ───────────────────────────────  LOADERS
def _load_supplier(buf, fname: str) -> pd.DataFrame:
    df = _read_any(buf, fname)
    df.columns = [_n(c) for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["supplier"], fname)
    df = df[~df["realm"].astype(str).str.contains(r"grand\s+total", case=False, na=False)]
    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]


def _load_raw(buf, fname: str) -> pd.DataFrame:
    df = _read_any(buf, fname)
    df.columns = [_n(c) for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["raw"], fname)
    for col in ("carrier", "realm", "customer"):
        df[col] = df[col].astype(str).str.strip().replace({"": "<nan>", "nan": "<nan>"})
    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "carrier", "realm", "data_mb"]]


def _load_billing(buf, fname: str) -> pd.DataFrame:
    df = _read_any(buf, fname, header=CFG.BILL_HDR)
    df.columns = [_n(c) for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"], fname)
    df["qty"]      = _coerce_numeric(df["qty"])
    df["customer"] = (df["customer"].astype(str)
                                   .str.strip()
                                   .replace({"": "<nan>", "nan": "<nan>"}))
    df["realm"] = (df["product"].astype(str)
                   .str.extract(CFG.REALM_RX)[0]
                   .str.lower()
                   .fillna("<nan>"))
    is_bundle = df["product"].str.contains("bundle", case=False, na=False)
    is_excess = df["product"].str.contains("excess", case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"], 0.0)
    df["excess_mb"] = np.where(is_excess, df["qty"], 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# ───────────────────────────────  COMPARISON
def _compare(l: pd.DataFrame, r: pd.DataFrame,
             keys: List[str], lcol: str, rcol: str, th: _Th) -> pd.DataFrame:
    cmp = (l.merge(r, on=keys, how="outer")
             .fillna({lcol: 0.0, rcol: 0.0}))
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp


# ───────────────────────────────  STREAMLIT UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown("""
<style>
.main           {padding-left:0!important; padding-right:0!important;}
.block-container{max-width:640px!important; margin:0 auto; padding-top:1.5rem;}
/* hide spinners / warnings while running */
.css-1v0mbdj,.stAlert,.stSpinner,.stProgress{display:none!important;}
</style>
""", unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload **Supplier**, **Raw-usage** & **Customer-billing** files, then click **Run**.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file", type=("csv", "xls", "xlsx"), key="sup")
f_raw  = c2.file_uploader("Raw usage file", type=("csv", "xls", "xlsx"), key="raw")
f_bill = st.file_uploader("Billing file", type=("csv", "xls", "xlsx"), key="bill")

busy   = st.session_state.get("busy", False)
clicked = st.button("Run", disabled=busy or not all((f_sup, f_raw, f_bill)))

# ─── RUN BUTTON HANDLER ──────────────────────────────────────────────────────
if clicked:
    try:
        st.session_state["busy"] = True

        sup  = _load_supplier(f_sup, f_sup.name)
        raw  = _load_raw(f_raw, f_raw.name)
        bill = _load_billing(f_bill, f_bill.name)

        # aggregates
        sup_rlm   = _agg(sup,  ["carrier", "realm"],       "data_mb",    "supplier_mb")
        sup_tot   = _agg(sup,  ["realm"],                  "data_mb",    "supplier_mb")
        raw_rlm   = _agg(raw,  ["carrier", "realm"],       "data_mb",    "raw_mb")
        raw_cust  = _agg(raw,  ["customer", "realm"],      "data_mb",    "raw_mb")
        bill_rlm  = _agg(bill, ["realm"],                  "billed_mb",  "customer_billed_mb")
        bill_cust = _agg(bill, ["customer", "realm"],      "billed_mb",  "customer_billed_mb")

        # comparisons
        tab1 = _compare(sup_rlm,  raw_rlm,   ["carrier","realm"],
                        "supplier_mb", "raw_mb",               CFG.REALM)
        tab2 = _compare(sup_tot,   bill_rlm,  ["realm"],
                        "supplier_mb", "customer_billed_mb",   CFG.REALM)
        tab3 = _compare(raw_cust,  bill_cust,["customer","realm"],
                        "raw_mb",      "customer_billed_mb",   CFG.CUSTOMER)

        # generate Excel workbook in-memory
        wb = io.BytesIO()
        with pd.ExcelWriter(wb, engine="xlsxwriter") as xl:
            for nm, df in {"Supplier_vs_Raw": tab1,
                           "Supplier_vs_Cust": tab2,
                           "Raw_vs_Cust":      tab3}.items():
                df.to_excel(xl, sheet_name=nm[:31], index=False)
        wb.seek(0)

        # persist bytes for next rerun
        st.session_state["recon_xlsx"] = wb.getvalue()
        st.toast("✅ Reconciliation finished – scroll down to download", icon="🎉")

    except ValueError as e:
        st.error(f"File problem: {e}")
    except Exception as e:
        st.error(f"Reconciliation failed: {e}")
    finally:
        st.session_state["busy"] = False

# ─── DOWNLOAD BUTTON (always visible once we have bytes) ─────────────────────
if "recon_xlsx" in st.session_state:
    st.download_button(
        "⬇️  Download reconciliation workbook",
        st.session_state["recon_xlsx"],
        file_name = f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime       = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
