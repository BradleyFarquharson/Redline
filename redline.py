#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
Upload Supplier, Raw-usage and Customer-billing files, click **Run**
and download a single Excel workbook with the three comparison tabs.
"""
from __future__ import annotations
import datetime as dt, io, re, traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ═══════════════════════════  CONFIG  ════════════════════════════
@dataclass(frozen=True)
class _Th:          # warn / fail thresholds (MB)
    warn: int
    fail: int


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)
    CUSTOMER: _Th = _Th(10, 50)

    BILL_HDR: int = 4                       # fallback header row
    REALM_RX: re.Pattern = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I)

    SCHEMA: dict[str, dict[str, List[str]]] = field(
        default_factory=lambda: {
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
            },
            "billing": {
                "customer": ["customer_code", "customer"],
                "product":  ["product/service", "product_service", "product"],
                "qty":      ["qty", "quantity"],
            },
        }
    )


CFG = _Cfg()

# ══════════════════════════  HELPERS  ════════════════════════════
def _n(s: str) -> str:          # snake-case
    return s.strip().lower().replace(" ", "_")


RX_NUM = re.compile(r"[^\d.\-]")
def _coerce_numeric(s: pd.Series) -> pd.Series:
    return (pd.to_numeric(
                s.astype(str).str.replace(RX_NUM, "", regex=True),
                errors="coerce")
            .fillna(0.0)
            .astype(float))


# build one reverse-alias map
_ALIAS = { _n(alias): canon
           for grp in CFG.SCHEMA.values()
           for canon, aliases in grp.items()
           for alias in (canon, *aliases) }


def _std_cols(df: pd.DataFrame, mapping: dict[str, List[str]],
              file_name: str) -> pd.DataFrame:
    df = df.rename(columns=lambda c: _ALIAS.get(_n(c), _n(c)))
    missing = [k for k in mapping if k not in df.columns]
    if missing:
        raise ValueError(f"Required column '{missing[0]}' not found "
                         f"in “{file_name}”")
    return df


def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    return (df.assign(**{c: df[c].fillna("<nan>").astype("category") for c in by})
              .groupby(by, sort=False, observed=True)[src]
              .sum()
              .reset_index()
              .rename(columns={src: tgt}))


def _status(delta: pd.Series, th: _Th) -> pd.Series:
    a = delta.abs().to_numpy()
    return pd.Series(np.select([a < th.warn, a < th.fail],
                               ["OK", "WARN"], "FAIL"),
                     dtype="category")


def _read_any(buf: io.BytesIO, file_name: str, **kw) -> pd.DataFrame:
    ext = Path(file_name).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, engine=None, **kw)
    if ext == ".csv":
        return pd.read_csv(buf, sep=None, engine="python", **kw)
    raise ValueError(f"Unsupported file type “{ext}” for {file_name}")

# ── find header row in billing export ────────────────────────────
@st.cache_data(hash_funcs={io.BytesIO: lambda _: None})
def _find_header_row(buf: io.BytesIO, file_name: str,
                     max_scan: int = 30) -> int:
    aliases = {_n(a) for a in CFG.SCHEMA["billing"]["customer"]}
    peek = pd.read_excel(buf, nrows=max_scan, header=None, engine=None)
    buf.seek(0)                        # rewind for real read
    hits = peek.fillna("").astype(str).applymap(_n).isin(aliases)
    if hits.values.any():
        return hits.any(axis=1).idxmax()
    return CFG.BILL_HDR

# ══════════════════════════  LOADERS  ════════════════════════════
def _load_supplier(buf, name: str) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, CFG.SCHEMA["supplier"], name)
    df = df[~df["realm"].astype(str)
              .str.contains(r"grand\s+total", case=False, na=False)]
    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]


def _load_raw(buf, name: str) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, CFG.SCHEMA["raw"], name)
    for c in ("carrier", "realm", "customer"):
        df[c] = df[c].astype(str).str.strip().replace({"": "<nan>", "nan": "<nan>"})
    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "carrier", "realm", "data_mb"]]


def _load_billing(buf, name: str) -> pd.DataFrame:
    hdr = _find_header_row(buf, name)
    df  = _read_any(buf, name, header=hdr)
    df  = _std_cols(df, CFG.SCHEMA["billing"], name)

    df["qty"]      = _coerce_numeric(df["qty"])
    df["customer"] = (df["customer"].astype(str)
                                   .str.strip()
                                   .replace({"": "<nan>", "nan": "<nan>"}))
    df["realm"] = (df["product"].astype(str)
                   .str.extract(CFG.REALM_RX)[0]
                   .str.lower()
                   .fillna("<nan>"))

    is_bundle = df["product"].str.contains("bundle",  case=False, na=False)
    is_excess = df["product"].str.contains("excess",  case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"], 0.0)
    df["excess_mb"] = np.where(is_excess, df["qty"], 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]

# ═════════════════════════  COMPARISON  ═════════════════════════
def _compare(l: pd.DataFrame, r: pd.DataFrame, keys: List[str],
             lcol: str, rcol: str, th: _Th) -> pd.DataFrame:
    cmp = (l.merge(r, on=keys, how="outer")
             .fillna({lcol: 0.0, rcol: 0.0}))
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp

# ══════════════════════════  STREAMLIT UI  ══════════════════════
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown("""
<style>
.block-container{max-width:640px !important;margin:0 auto;}
/* keep Streamlit’s main spinner visible, hide only noisy progress bars */
.stProgress{display:none!important;}
</style>""", unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files and click **Run**.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file",  type=("csv","xls","xlsx"))
f_raw  = c2.file_uploader("Raw-usage file", type=("csv","xls","xlsx"))
f_bill = st.file_uploader("Billing file",   type=("csv","xls","xlsx"))

run_btn = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run_btn:
    with st.spinner("Processing …"):
        try:
            sup  = _load_supplier(f_sup,  f_sup.name)
            raw  = _load_raw(f_raw,      f_raw.name)
            bill = _load_billing(f_bill, f_bill.name)

            # aggregates
            sup_rlm   = _agg(sup,  ["carrier","realm"], "data_mb",   "supplier_mb")
            sup_tot   = _agg(sup,  ["realm"],           "data_mb",   "supplier_mb")
            raw_rlm   = _agg(raw,  ["carrier","realm"], "data_mb",   "raw_mb")
            raw_cust  = _agg(raw,  ["customer","realm"],"data_mb",   "raw_mb")
            bill_rlm  = _agg(bill, ["realm"],           "billed_mb", "customer_billed_mb")
            bill_cust = _agg(bill, ["customer","realm"],"billed_mb", "customer_billed_mb")

            # comparisons
            tab1 = _compare(sup_rlm, raw_rlm, ["carrier","realm"],
                            "supplier_mb", "raw_mb",             CFG.REALM)
            tab2 = _compare(sup_tot, bill_rlm, ["realm"],
                            "supplier_mb", "customer_billed_mb", CFG.REALM)
            tab3 = _compare(raw_cust, bill_cust, ["customer","realm"],
                            "raw_mb", "customer_billed_mb",      CFG.CUSTOMER)

            # workbook
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
                {"Supplier_vs_Raw":  tab1,
                 "Supplier_vs_Cust": tab2,
                 "Raw_vs_Cust":      tab3}\
                 .items()
                for name, df in {"Supplier_vs_Raw":tab1,
                                 "Supplier_vs_Cust":tab2,
                                 "Raw_vs_Cust":tab3}.items():
                    df.to_excel(xl, sheet_name=name[:31], index=False)
            buf.seek(0)

            st.download_button("⬇️ Download reconciliation workbook",
                               data=buf.read(),
                               file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Reconciliation finished – grab the file above.")
        except Exception as err:
            st.error(f"Reconciliation failed: {err}")
            with st.expander("Details"):
                st.code(traceback.format_exc())
