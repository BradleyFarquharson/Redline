#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
Upload the three source files (Supplier, Raw-usage, Customer-billing),
click **Run** and download a single Excel workbook with three comparison
tabs.
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


# ───────────────────────── CONFIG ──────────────────────────────────────────
@dataclass(frozen=True)
class _Th:
    warn: int   # MB delta at which a WARN is raised
    fail: int   # MB delta at which a FAIL is raised


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)      # thresholds per-realm
    CUSTOMER: _Th = _Th(10, 50)     # thresholds per-customer/realm

    BILL_HDR_MAX_SCAN: int = 12     # how many rows to sniff for header

    REALM_RX: re.Pattern = re.compile(
        r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I
    )  # e.g. “… – ZA 3”

    # Canonical ↔︎ alias mapping  (add new aliases here, no other code tweak needed)
    SCHEMA: dict[str, dict[str, list[str]]] = field(
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
                "customer": ["customer_code", "customer code", "customercode",
                             "customer id", "customerid", "customer"],
                "realm":    ["realm"],
                "carrier":  ["carrier"],
                "data_mb":  ["total_usage_(mb)", "total_usage_mb", "usage_mb",
                             "data_mb", "total_mb"],
                "status":   ["status"],
            },
            "billing": {
                "customer": ["customer_code", "customer code", "customer"],
                "product":  ["product/service", "product_service", "product"],
                "qty":      ["qty", "quantity"],
            },
        }
    )


CFG = _Cfg()


# ────────────────────────── HELPERS ────────────────────────────────────────
def _n(s: str) -> str:
    """Very cheap snake-case."""
    return s.strip().lower().replace(" ", "_")


# Build one reverse-alias map so we never loop aliases again
_ALIAS: dict[str, str] = {
    _n(alias): canon
    for cat in CFG.SCHEMA.values()
    for canon, aliases in cat.items()
    for alias in (canon, *aliases)
}


RX_NUM = re.compile(r"[^\d.\-]")


def _coerce_numeric(s: pd.Series) -> pd.Series:
    """Fast numeric coercion stripping any non-digit/./- chars."""
    return (
        pd.to_numeric(s.astype(str).str.replace(RX_NUM, "", regex=True),
                      errors="coerce")
        .fillna(0.0)
        .astype(float)
    )


def _std_cols(df: pd.DataFrame,
              mapping: dict[str, list[str]],
              file_name: str) -> pd.DataFrame:
    """Rename columns to canonical names & ensure required ones exist."""
    df = df.rename(columns=lambda c: _ALIAS.get(_n(c), c))

    # drop duplicate columns that may now have identical names
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

    missing = [c for c in mapping if c not in df.columns]
    if missing:
        raise ValueError(
            f"Required column '{missing[0]}' not found in {file_name}"
        )
    return df


def _agg(df: pd.DataFrame, by: list[str], src: str,
         tgt: str) -> pd.DataFrame:
    return (
        df.assign(**{c: df[c].fillna("<nan>").astype("category") for c in by})
          .groupby(by, as_index=False, observed=True, sort=False)[src]
          .sum()
          .rename(columns={src: tgt})
    )


def _status(delta: pd.Series, th: _Th) -> pd.Series:
    a = delta.abs().to_numpy()
    out = np.select([a < th.warn, a < th.fail], ["OK", "WARN"], "FAIL")
    return pd.Series(out, dtype="category")


def _read_any(buf: IO[bytes], name: str, **kw) -> pd.DataFrame:
    ext = Path(name).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, **kw)
    if ext == ".csv":
        return pd.read_csv(buf, sep=None, engine="python",
                           encoding_errors="replace", **kw)
    raise ValueError(f"Unsupported file type: {ext}")


# ────────────────── BILLING HEADER AUTO-DETECT (cached) ────────────────────
@st.cache_data(show_spinner=False)
def _find_header_row(buf: bytes, name: str) -> int:
    sniff = pd.read_excel(io.BytesIO(buf), nrows=CFG.BILL_HDR_MAX_SCAN,
                          header=None)
    a = sniff.fillna("").astype(str).applymap(_n).values
    needed = set(
        _n(x) for x in CFG.SCHEMA["billing"]["customer"] +
        CFG.SCHEMA["billing"]["product"] + CFG.SCHEMA["billing"]["qty"]
    )
    hits = np.isin(a, list(needed))
    return int(np.argmax(hits.any(1))) if hits.any() else 0


# ───────────────────────── LOADERS ─────────────────────────────────────────
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
    for col in ("carrier", "realm", "customer"):
        df[col] = (
            df[col].astype(str).str.strip()
              .replace({"": "<nan>", "nan": "<nan>"})
        )
    df["carrier"] = df["carrier"].str.upper()
    df["realm"]   = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "carrier", "realm", "data_mb"]]


def _load_billing(buf, name: str) -> pd.DataFrame:
    raw_bytes = buf.read()
    hdr = _find_header_row(raw_bytes, name)
    df = _read_any(io.BytesIO(raw_bytes), name, header=hdr)
    df = _std_cols(df, CFG.SCHEMA["billing"], name)

    df["qty"]      = _coerce_numeric(df["qty"])
    df["customer"] = (
        df["customer"].astype(str).str.strip()
          .replace({"": "<nan>", "nan": "<nan>"})
    )
    df["realm"] = (
        df["product"].astype(str).str.extract(CFG.REALM_RX)[0]
          .str.lower().fillna("<nan>")
    )
    is_bundle = df["product"].str.contains("bundle", case=False, na=False)
    is_excess = df["product"].str.contains("excess", case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"], 0.0)
    df["excess_mb"] = np.where(is_excess, df["qty"], 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# ─────────────────────── COMPARISON ───────────────────────────────────────
def _compare(l: pd.DataFrame, r: pd.DataFrame,
             keys: list[str], lcol: str, rcol: str,
             th: _Th) -> pd.DataFrame:
    cmp = (
        l.merge(r, on=keys, how="outer")
          .fillna({lcol: 0.0, rcol: 0.0})
    )
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"]   = _status(cmp["delta_mb"], th)
    return cmp


# ─────────────────────── STREAMLIT UI ─────────────────────────────────────
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown(
    """
<style>
.block-container{max-width:640px!important;margin:0 auto;padding-top:1.5rem;}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload Supplier, Raw-usage & Customer-billing files, then click **Run**.")

c1, c2 = st.columns(2)
f_sup  = c1.file_uploader("Supplier file", type=("csv", "xls", "xlsx"), key="sup")
f_raw  = c2.file_uploader("Raw-usage file", type=("csv", "xls", "xlsx"), key="raw")
f_bill = st.file_uploader("Billing file", type=("csv", "xls", "xlsx"), key="bill")

run_btn = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run_btn:
    with st.spinner("Running reconciliation …"):
        try:
            sup  = _load_supplier(f_sup,  f_sup.name)
            raw  = _load_raw     (f_raw,  f_raw.name)
            bill = _load_billing (f_bill, f_bill.name)

            # aggregates
            sup_rlm   = _agg(sup,  ["carrier", "realm"],  "data_mb",    "supplier_mb")
            sup_tot   = _agg(sup,  ["realm"],             "data_mb",    "supplier_mb")
            raw_rlm   = _agg(raw,  ["carrier", "realm"],  "data_mb",    "raw_mb")
            raw_cust  = _agg(raw,  ["customer", "realm"], "data_mb",    "raw_mb")
            bill_rlm  = _agg(bill, ["realm"],             "billed_mb",  "customer_billed_mb")
            bill_cust = _agg(bill, ["customer", "realm"], "billed_mb",  "customer_billed_mb")

            # comparisons
            tab1 = _compare(sup_rlm,  raw_rlm,   ["carrier", "realm"],
                            "supplier_mb", "raw_mb",             CFG.REALM)
            tab2 = _compare(sup_tot,   bill_rlm, ["realm"],
                            "supplier_mb", "customer_billed_mb", CFG.REALM)
            tab3 = _compare(raw_cust,  bill_cust, ["customer", "realm"],
                            "raw_mb",      "customer_billed_mb", CFG.CUSTOMER)

            # Excel output
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
                for nm, df in {
                    "Supplier_vs_Raw":  tab1,
                    "Supplier_vs_Cust": tab2,
                    "Raw_vs_Cust":      tab3,
                }.items():
                    df.to_excel(xl, sheet_name=nm[:31], index=False)
            buf.seek(0)

            st.success("Reconciliation complete.")
            st.download_button("⬇️ Download reconciliation workbook",
                               data=buf.read(),
                               file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:  # show trace in an expander — dev friendly
            st.error(f"Reconciliation failed: {e}")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())
