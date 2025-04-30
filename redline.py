#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
Upload Supplier, Raw-usage & Customer-billing files, click •Run•,
and download a single Excel workbook with three comparison tabs.
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

# ───────────────────────────── CONFIG
@dataclass(frozen=True)
class _Th:
    warn: int
    fail: int


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)
    CUSTOMER: _Th = _Th(10, 50)
    BILL_HDR: int = 4          # fallback row index if auto-detection fails
    REALM_RX: re.Pattern = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I)

    SCHEMA: dict[str, dict[str, List[str]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier":  ["carrier"],
                "realm":    ["realm"],
                "data_mb":  ["total_mb", "usage_mb", "total_usage_mb",
                             "total_usage_(mb)", "total_usage", "data_mb"],
            },
            "raw": {
                "customer": ["customer_code", "customer"],
                "carrier":  ["carrier"],
                "realm":    ["realm"],
                "data_mb":  ["total_usage_(mb)", "total_usage_mb", "usage_mb",
                             "data_mb", "total_mb"],
            },
            "billing": {
                "customer": ["customer_code", "customer"],
                "product":  ["product/service", "product_service", "product"],
                "qty":      ["qty", "quantity"],
            },
        }
    )


CFG = _Cfg()

# ───────────────────────────── HELPERS
def _n(col: str) -> str:
    return col.strip().lower().replace(" ", "_")


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
            if canon == "customer":          # we’ll inject later if absent
                continue
            raise ValueError(f"Required column '{canon}' not found in {fname}")
        keep, dupes = hits[0], hits[1:]
        df.rename(columns={keep: canon}, inplace=True)
        if dupes:
            df.drop(columns=dupes, inplace=True)
    return df


def _assert_cols(df: pd.DataFrame, cols: List[str], tag: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{tag} missing columns: {miss}")


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
    raise ValueError(f"Unsupported file type {ext} for {fname}")

# ───────────────────────────── LOADERS
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
    if "customer" not in df.columns:
        st.warning(f"No 'customer' column in {fname} — using <nan>")
        df["customer"] = "<nan>"
    for col in ("carrier", "realm", "customer"):
        df[col] = df[col].astype(str).str.strip().replace({"": "<nan>", "nan": "<nan>"})
    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    _assert_cols(df, ["customer", "realm", "carrier", "data_mb"], fname)
    return df[["customer", "carrier", "realm", "data_mb"]]


# ------------- NEW  auto-header detection for billing file -------------
def _find_hdr_row(buf, fname: str, max_rows: int = 12) -> int:
    """scan first *max_rows* rows and return the first that looks like a header"""
    sniff = pd.read_excel(buf, header=None, nrows=max_rows, engine="openpyxl")
    buf.seek(0)
    needed = {"customer_code", "customer", "qty", "product", "product/service"}
    for idx, row in sniff.iterrows():
        cols = {_n(str(c)) for c in row.values if str(c) != "nan"}
        if len(needed.intersection(cols)) >= 2:   # good enough
            return idx
    st.warning(f"Couldn’t auto-detect header in {fname}, falling back to row {CFG.BILL_HDR+1}")
    return CFG.BILL_HDR
# -----------------------------------------------------------------------


def _load_billing(buf, fname: str) -> pd.DataFrame:
    hdr = _find_hdr_row(buf, fname)
    df  = _read_any(buf, fname, header=hdr)
    df.columns = [_n(c) for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"], fname)

    if "customer" not in df.columns:
        st.warning(f"No 'customer' column in {fname} — using <nan>")
        df["customer"] = "<nan>"

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

    _assert_cols(df, ["customer", "realm", "billed_mb"], fname)
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]

# ───────────────────────────── COMPARISON
def _compare(l: pd.DataFrame, r: pd.DataFrame,
             keys: List[str], lcol: str, rcol: str, th: _Th) -> pd.DataFrame:
    cmp = (l.merge(r, on=keys, how="outer")
             .fillna({lcol: 0.0, rcol: 0.0}))
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp


# ───────────────────────────── STREAMLIT UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown("""
<style>
.main           {padding-left:0!important; padding-right:0!important;}
.block-container{max-width:640px!important; margin:0 auto; padding-top:1.5rem;}
</style>
""", unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload **Supplier**, **Raw-usage** & **Customer-billing** files, then click **Run**.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file", type=("csv","xls","xlsx"), key="sup")
f_raw  = c2.file_uploader("Raw usage file", type=("csv","xls","xlsx"), key="raw")
f_bill = st.file_uploader("Billing file",  type=("csv","xls","xlsx"), key="bill")

busy = st.session_state.get("busy", False)
run_clicked = st.button("Run", disabled=busy or not all((f_sup, f_raw, f_bill)))

if run_clicked:
    st.session_state["busy"] = True
    with st.spinner("Reconciling… hang tight"):
        try:
            sup  = _load_supplier(f_sup,  f_sup.name)
            raw  = _load_raw(    f_raw,  f_raw.name)
            bill = _load_billing(f_bill, f_bill.name)

            sup_rlm   = _agg(sup,  ["carrier","realm"], "data_mb",   "supplier_mb")
            sup_tot   = _agg(sup,  ["realm"],           "data_mb",   "supplier_mb")
            raw_rlm   = _agg(raw,  ["carrier","realm"], "data_mb",   "raw_mb")
            raw_cust  = _agg(raw,  ["customer","realm"],"data_mb",   "raw_mb")
            bill_rlm  = _agg(bill, ["realm"],           "billed_mb", "customer_billed_mb")
            bill_cust = _agg(bill, ["customer","realm"],"billed_mb", "customer_billed_mb")

            wb = io.BytesIO()
            with pd.ExcelWriter(wb, engine="xlsxwriter") as xl:
                _compare(sup_rlm, raw_rlm,  ["carrier","realm"],
                         "supplier_mb","raw_mb",              CFG.REALM).to_excel(xl,"Supplier_vs_Raw",index=False)
                _compare(sup_tot,  bill_rlm, ["realm"],
                         "supplier_mb","customer_billed_mb",  CFG.REALM).to_excel(xl,"Supplier_vs_Cust",index=False)
                _compare(raw_cust,bill_cust,["customer","realm"],
                         "raw_mb","customer_billed_mb",       CFG.CUSTOMER).to_excel(xl,"Raw_vs_Cust",index=False)
            wb.seek(0)
            st.session_state["recon_bytes"] = wb.getvalue()
            st.success("Reconciliation finished ✔")
        except Exception as e:
            st.error(f"Reconciliation failed: {e}")
    st.session_state["busy"] = False

if "recon_bytes" in st.session_state:
    st.download_button(
        "⬇️ Download reconciliation workbook",
        st.session_state["recon_bytes"],
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
