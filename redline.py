#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
==================================
Upload the three source files (Supplier • Raw usage • Customer billing),
click **Run** and download a single Excel workbook (three comparison tabs).
"""

from __future__ import annotations
import datetime as dt, io, re, traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ───────────────────────── Config & constants ──────────────────────────
@dataclass(frozen=True)
class Threshold:
    warn: int   # MB delta ⇒ WARN
    fail: int   # MB delta ⇒ FAIL


@dataclass(frozen=True)
class Config:
    REALM:        Threshold = Threshold(5, 20)
    CUSTOMER:     Threshold = Threshold(10, 50)

    # the first row to try if automatic header sniff fails
    BILL_FALLBACK_HDR: int = 4

    # pattern that extracts a realm like “ZA 3” from the product string
    REALM_RX: re.Pattern = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I)

    # column aliases per file type
    SCHEMA: dict[str, dict[str, list[str]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier":  ["carrier"],
                "realm":    ["realm"],
                "subs_qty": ["subscription_qty", "subscription", "qty"],
                "data_mb":  ["total_mb", "usage_mb", "total_usage_mb",
                             "total_usage_(mb)", "total_usage", "data_mb"],
            },
            # raw usage export
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
            # Intacct billing dump
            "billing": {
                "customer_code": ["customer_code"],
                "customer_name": ["customer"],          # NEW
                "product":       ["product/service", "product_service",
                                   "product"],
                "qty":           ["qty", "quantity"],
            },
        }
    )


CFG = Config()

# build a reverse alias map once ------------------------------------------------
_ALIAS = {re.sub(r"\s+", "_", k.lower()): canon
          for section in CFG.SCHEMA.values()
          for canon, aliases in section.items()
          for k in [canon, *aliases]}

# ───────────────────────── Helper utilities ────────────────────────────
def _n(col: str) -> str:
    """snake-case a header."""
    return re.sub(r"\s+", "_", col.strip().lower())


_RX_NOT_NUM = re.compile(r"[^\d\-.]")
def _coerce_numeric(s: pd.Series) -> pd.Series:
    """Robust numeric conversion (commas, blanks, dashes → 0)."""
    return (pd.to_numeric(
                s.astype(str).str.replace(_RX_NOT_NUM, "", regex=True),
                errors="coerce")
              .fillna(0.0)
              .astype(float))


def _std_cols(df: pd.DataFrame,
              mapping: dict[str, list[str]],
              file_name: str) -> pd.DataFrame:
    """Rename columns to canonical names & be sure required ones exist."""
    # 1-pass rename through the global alias map
    df = df.rename(columns=lambda c: _ALIAS.get(_n(c), _n(c)))

    # if duplicate names remain, keep first occurrence
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

    missing = [c for c in mapping if c not in df.columns]
    if missing:
        raise ValueError(f"Required column '{missing[0]}' not found in "
                         f“{file_name}”")
    return df


def _agg(df: pd.DataFrame, by: list[str], src: str, tgt: str) -> pd.DataFrame:
    """Sum `src` by *by* → rename to *tgt*."""
    return (df.assign(**{c: df[c].fillna("<nan>").astype("category")
                         for c in by})
              .groupby(by, as_index=False, observed=True, sort=False)[src]
              .sum()
              .rename(columns={src: tgt}))


def _status(delta: pd.Series, th: Threshold) -> pd.Series:
    """‘OK’ | ‘WARN’ | ‘FAIL’ based on absolute delta."""
    a = delta.abs().to_numpy()
    return pd.Series(np.select([a < th.warn,
                                a < th.fail],
                               ["OK", "WARN"], "FAIL"),
                     dtype="category")

# ───────────── File readers & header sniffers (cached) ─────────────────
def _read_any(buf: IO[bytes], file_name: str, **kw) -> pd.DataFrame:
    ext = Path(file_name).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, **kw)
    if ext == ".csv":
        return pd.read_csv(buf, sep=None, engine="python",
                           encoding_errors="replace", **kw)
    raise ValueError(f"Unsupported file type: {ext!s}")


@st.cache_data(show_spinner=False)
def _find_header_row(buf: IO[bytes], file_name: str) -> int:
    """Return the row index containing *any* alias of the billing schema."""
    buf_bytes = buf.getvalue()         # single IO hit
    sniff = pd.read_excel(io.BytesIO(buf_bytes), nrows=8, header=None)
    wanted = {_n(a) for a in
              sum(CFG.SCHEMA["billing"].values(), [])}
    hits = sniff.astype(str).applymap(_n).isin(wanted).to_numpy()
    if hits.any():
        return int(np.where(hits.any(axis=1))[0][0])
    return CFG.BILL_FALLBACK_HDR


# ───────────── Loaders ─────────────────────────────────────────────────
def _load_supplier(buf, name) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, CFG.SCHEMA["supplier"], name)

    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    df = df[~df["realm"].str.contains(r"grand\s+total", case=False, na=False)]
    return df[["carrier", "realm", "data_mb"]]


def _load_raw(buf, name) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, CFG.SCHEMA["raw"], name)

    for c in ("carrier", "realm", "customer"):
        df[c] = (df[c].astype(str)
                          .str.strip()
                          .replace({"": "<nan>", "nan": "<nan>"}))

    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "carrier", "realm", "data_mb"]]


def _load_billing(buf, name) -> pd.DataFrame:
    hdr = _find_header_row(buf, name)
    df  = _read_any(buf, name, header=hdr)
    df  = _std_cols(df, CFG.SCHEMA["billing"], name)

    df["qty"] = _coerce_numeric(df["qty"])

    df["customer_code"] = (df["customer_code"].astype(str)
                                               .str.strip()
                                               .replace({"": "<nan>", "nan": "<nan>"}))
    df["customer_name"] = df["customer_name"].astype(str).str.strip()

    # realm extraction from product description
    df["realm"] = (df["product"].astype(str)
                              .str.extract(CFG.REALM_RX)[0]
                              .str.lower()
                              .fillna("<nan>"))

    is_bundle = df["product"].str.contains("bundle", case=False, na=False)
    is_excess = df["product"].str.contains("excess", case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"], 0.)
    df["excess_mb"] = np.where(is_excess, df["qty"], 0.)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    return df[["customer_code", "customer_name", "realm",
               "bundle_mb", "excess_mb", "billed_mb"]]

# ───────────── Comparison helper ───────────────────────────────────────
def _compare(l: pd.DataFrame, r: pd.DataFrame,
             keys: list[str], lcol: str, rcol: str,
             th: Threshold) -> pd.DataFrame:
    cmp = (l.merge(r, on=keys, how="outer")
             .fillna({lcol: 0., rcol: 0.}))

    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp

# ───────────────────────────── Streamlit UI ────────────────────────────
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown("""
<style>
.block-container{max-width:640px !important;margin:0 auto;padding-top:1.5rem;}
</style>
""", unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files, click **Run**, download the Excel workbook.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file", type=("csv","xls","xlsx"), key="sup")
f_raw  = c2.file_uploader("Raw-usage file", type=("csv","xls","xlsx"), key="raw")
f_bill = st.file_uploader("Billing file", type=("csv","xls","xlsx"), key="bill")

run = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    with st.spinner("Running reconciliation …"):
        try:
            # ─── Load
            sup   = _load_supplier(f_sup,  f_sup.name)
            raw   = _load_raw     (f_raw,  f_raw.name)
            bill  = _load_billing (f_bill, f_bill.name)

            # ─── Aggregations
            sup_car_realm = _agg(sup,  ["carrier","realm"], "data_mb", "supplier_mb")
            sup_realm     = _agg(sup,  ["realm"],           "data_mb", "supplier_mb")

            raw_car_realm = _agg(raw, ["carrier","realm"],  "data_mb", "raw_mb")
            raw_cust      = _agg(raw, ["customer","realm"], "data_mb", "raw_mb")

            bill_realm    = _agg(bill, ["realm"],           "billed_mb", "customer_billed_mb")
            bill_cust     = _agg(bill, ["customer_code","realm"],
                                 "billed_mb", "customer_billed_mb")

            # ─── Comparisons
            tab1 = _compare(sup_car_realm, raw_car_realm,
                            ["carrier","realm"], "supplier_mb", "raw_mb",
                            CFG.REALM)

            tab2 = _compare(sup_realm, bill_realm,
                            ["realm"], "supplier_mb", "customer_billed_mb",
                            CFG.REALM)

            tab3 = _compare(raw_cust, bill_cust,
                            ["customer","realm"], "raw_mb", "customer_billed_mb",
                            CFG.CUSTOMER)

            # replace customer code with human-readable name
            name_lookup = (bill[["customer_code","customer_name"]]
                               .drop_duplicates())
            tab3 = (tab3
                    .merge(name_lookup, left_on="customer",
                                      right_on="customer_code",
                                      how="left")
                    .drop(columns=["customer", "customer_code"])
                    .rename(columns={"customer_name": "customer"}))

            # ─── Excel workbook
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
                for n, df in {"Supplier_vs_Raw": tab1,
                              "Supplier_vs_Cust": tab2,
                              "Raw_vs_Cust":      tab3}.items():
                    df.to_excel(xl, sheet_name=n[:31], index=False)
            buf.seek(0)

        except Exception as e:
            stack = traceback.format_exc()
            st.error(f"Reconciliation failed: {e}")
            with st.expander("Details"):
                st.code(stack, language="python")
        else:
            st.success("Done — download below.")
            st.download_button(
                "⬇️ Download reconciliation workbook",
                data=buf.getvalue(),
                file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
