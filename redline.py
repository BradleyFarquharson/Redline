#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
…(docstring unchanged)…
"""
from __future__ import annotations

import csv                # ▍NEW  (fast CSV delimiter sniff)
import datetime as dt
import io
import re
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Sequence

from zipfile import BadZipFile       # ▍explicit corrupt-xlsx catch

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ------------------------------------------------------------------ CONFIG
pd.set_option("mode.data_manager", "pyarrow", errors="ignore")   # ▍halve RAM in 2.2+

@dataclass(frozen=True)
class _Th:
    warn: int
    fail: int                 # |Δ| ≤ warn → OK,  warn < |Δ| ≤ fail → WARN,  else FAIL


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)
    CUSTOMER: _Th = _Th(10, 50)

    REALM_RX: re.Pattern = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I)

    BILL_HDR_SCAN_ROWS: int = 8

    SCHEMA: dict[str, dict[str, list[str]]] = field(default_factory=lambda: {
        "supplier": {
            "carrier":  ["carrier"],
            "realm":    ["realm"],
            "subs_qty": ["subscription_qty", "subscription", "qty"],
            "data_mb":  ["total_mb", "usage_mb",
                         "total_usage_mb", "total_usage_(mb)", "total_usage"],
        },
        "raw": {
            "customer": ["customer_code", "customer"],
            "carrier":  ["carrier"],
            "realm":    ["realm"],
            "data_mb":  ["total_usage_(mb)", "total_usage_mb",
                         "usage_mb", "data_mb", "total_mb"],
        },
        "billing": {
            "customer": ["customer_code", "customer_co", "customer"],
            "product":  ["product/service", "product_service", "product"],
            "qty":      ["qty", "quantity"],
        },
    })


CFG = _Cfg()

# single global alias-lookup
ALIASES = {
    re.sub(r"\s+", "_", alias.strip().lower()): canon
    for cat in CFG.SCHEMA.values()
    for canon, aliases in cat.items()
    for alias in [canon, *aliases]
}

# ------------------------------------------------------------------ HELPERS
# include % and whitespace so “-0.5 %”  →  -0.5   •  “1 234,55”  →  1234.55
_NRX = re.compile(r"[^\d.\-%\s]")

def _n(c: str) -> str:
    return re.sub(r"\s+", "_", c.strip().lower())

def _coerce_numeric(s: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            s.astype(str)
             .str.replace(_NRX, "", regex=True)
             .str.replace(r"\s+", "", regex=True)        # drop thin-spaces
             .str.replace(",", ".", regex=False),        # 1 234,5 → 1234.5
            errors="coerce",
        )
        .fillna(0.0)
        .astype(float)
    )

def _status(delta: pd.Series, th: _Th) -> pd.Series:
    a = delta.abs().to_numpy()
    return pd.Series(np.select([a <= th.warn, a <= th.fail],
                               ["OK", "WARN"], "FAIL"),
                     dtype="category")

def _std_cols(df: pd.DataFrame, cat: str, file_name: str) -> pd.DataFrame:
    df = df.rename(columns=lambda c: ALIASES.get(_n(c), _n(c))).copy()
    for canon in CFG.SCHEMA[cat]:
        if canon not in df.columns:
            # ▍raw & billing “customer” is optional – inject <nan>
            if canon == "customer":
                df["customer"] = "<nan>"
            else:
                raise ValueError(f"Required column '{canon}' not found in {file_name}")
    return df

def _find_hdr_row(buf_bytes: bytes) -> int:
    sniff = pd.read_excel(io.BytesIO(buf_bytes),
                          usecols="A:K", nrows=CFG.BILL_HDR_SCAN_ROWS)
    sniff.columns = [_n(c) for c in sniff.columns]
    needed = set(CFG.SCHEMA["billing"])
    a = sniff.fillna("").astype(str).applymap(_n).values
    hits = np.isin(a, list(needed))
    return int(np.argmax(hits.any(1))) if hits.any() else 0

# ------------------------------------------------------------------ I/O
def _read_any(buf: IO[bytes], name: str, **kw) -> pd.DataFrame:
    ext = Path(name).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, **kw)
    if ext == ".csv":
        head = buf.peek(1024)
        try:
            delim = csv.Sniffer().sniff(head.decode("utf-8", "ignore")).delimiter
        except csv.Error:
            delim = ","
        return pd.read_csv(buf, sep=delim, **kw)
    raise ValueError(f"Unsupported file type {ext} for {name!s}")

_hash_bytes = lambda b: hash(b.getvalue())        # ▍stable cache key

@st.cache_data(hash_funcs={io.BytesIO: _hash_bytes}, show_spinner=False)
def _load_supplier(buf: IO[bytes], name: str) -> pd.DataFrame:
    df = _std_cols(_read_any(buf, name), "supplier", name)
    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]

@st.cache_data(hash_funcs={io.BytesIO: _hash_bytes}, show_spinner=False)
def _load_raw(buf: IO[bytes], name: str) -> pd.DataFrame:
    df = _std_cols(_read_any(buf, name), "raw", name)

    for col in ("customer", "carrier", "realm"):
        df[col] = df[col].astype(str).str.strip().replace({"": "<nan>", "nan": "<nan>"})
    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "carrier", "realm", "data_mb"]]

@st.cache_data(hash_funcs={io.BytesIO: _hash_bytes}, show_spinner=False)
def _load_billing(buf: IO[bytes], name: str) -> pd.DataFrame:
    b = buf.getvalue()
    hdr = _find_hdr_row(b)
    df = _std_cols(_read_any(io.BytesIO(b), name, header=hdr), "billing", name)

    df["qty"]      = _coerce_numeric(df["qty"])
    df["customer"] = df["customer"].astype(str).str.strip().replace({"": "<nan>", "nan": "<nan>"})
    df["realm"]    = (df["product"].astype(str)
                                  .str.extract(CFG.REALM_RX)[0]
                                  .str.lower()
                                  .fillna("<nan>"))

    is_bundle = df["product"].str.contains("bundle", case=False, na=False)
    is_excess = df["product"].str.contains("excess", case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"], 0.0)
    df["excess_mb"] = np.where(is_excess, df["qty"], 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]

# ------------------------------------------------------------------ BUSINESS
def _agg(df: pd.DataFrame, by: list[str], src: str, tgt: str) -> pd.DataFrame:
    return (df.assign(**{c: df[c].fillna("<nan>").astype("category") for c in by})
              .groupby(by, as_index=False, observed=True, sort=False)[src]
              .sum()
              .rename(columns={src: tgt}))

def _compare(l: pd.DataFrame, r: pd.DataFrame,
             keys: Sequence[str], lcol: str, rcol: str, th: _Th) -> pd.DataFrame:
    cmp = l.merge(r, on=list(keys), how="outer").fillna({lcol: 0., rcol: 0.})
    cmp[lcol], cmp[rcol] = map(_coerce_numeric, (cmp[lcol], cmp[rcol]))
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp

# ------------------------------------------------------------------ STREAMLIT
st.set_page_config(page_title="Redline Reconciliation", layout="centered")
st.markdown(
    "<style>.block-container{max-width:640px!important;margin:0 auto;padding-top:1.5rem;}</style>",
    unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload Supplier · Raw-usage · Customer-billing, then click **Run**.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file", type=("csv","xls","xlsx"), key="sup")
f_raw  = c2.file_uploader("Raw-usage file", type=("csv","xls","xlsx"), key="raw")
f_bill = st.file_uploader("Billing file", type=("csv","xls","xlsx"), key="bill")

run = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    with st.spinner("Reconciling …"):
        try:
            sup  = _load_supplier(f_sup,  f_sup.name)
            raw  = _load_raw(f_raw,      f_raw.name)
            bill = _load_billing(f_bill, f_bill.name)

            # --- aggregate
            sup_rlm   = _agg(sup,  ["carrier","realm"], "data_mb", "supplier_mb")
            sup_tot   = _agg(sup,  ["realm"],           "data_mb", "supplier_mb")
            raw_rlm   = _agg(raw,  ["carrier","realm"], "data_mb", "raw_mb")
            raw_cust  = _agg(raw,  ["customer","realm"],"data_mb", "raw_mb")
            bill_rlm  = _agg(bill, ["realm"],           "billed_mb","customer_billed_mb")
            bill_cust = _agg(bill, ["customer","realm"],"billed_mb","customer_billed_mb")

            tab1 = _compare(sup_rlm, raw_rlm, ["carrier","realm"],
                            "supplier_mb", "raw_mb", CFG.REALM)
            tab2 = _compare(sup_tot, bill_rlm, ["realm"],
                            "supplier_mb", "customer_billed_mb", CFG.REALM)
            tab3 = _compare(raw_cust, bill_cust, ["customer","realm"],
                            "raw_mb", "customer_billed_mb", CFG.CUSTOMER)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
                def _sheet(df: pd.DataFrame, name: str):
                    df.to_excel(xl, sheet_name=name[:31], index=False)
                    ws = xl.sheets[name[:31]]
                    ws.freeze_panes(1, 0)                     # ▍freeze header
                    ws.autofilter(0, 0, len(df), df.shape[1]-1)

                    for i, col in enumerate(df.columns):
                        ws.set_column(i, i, max(10, int(df[col].astype(str).str.len().max())+2))
                    stat_col = df.columns.get_loc("status")
                    last = len(df)+1
                    fmt_fail = xl.book.add_format({"bg_color":"#F8696B","font_color":"#FFF"})
                    fmt_warn = xl.book.add_format({"bg_color":"#FFEB84"})
                    ws.conditional_format(1,stat_col,last,stat_col,
                                          {"type":"text","criteria":"containing",
                                           "value":"FAIL","format":fmt_fail})
                    ws.conditional_format(1,stat_col,last,stat_col,
                                          {"type":"text","criteria":"containing",
                                           "value":"WARN","format":fmt_warn})

                _sheet(tab1, "Supplier_vs_Raw")
                _sheet(tab2, "Supplier_vs_Cust")
                _sheet(tab3, "Raw_vs_Cust")

            buf.seek(0)
            st.download_button("⬇️ Download reconciliation workbook",
                               data=buf.getvalue(),
                               file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Done!")
        except (ValueError, KeyError, BadZipFile) as e:
            st.error(str(e))
        except Exception:
            st.error("Reconciliation failed — see trace ↓")
            with st.expander("Trace"):
                st.code(traceback.format_exc())
