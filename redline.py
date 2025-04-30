#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation  v2.5.4
==========================================

Upload:
    1. Supplier summary  (xls/xlsx/csv)
    2. iONLINE raw usage (xls/xlsx/csv)
    3. Customer billing  (xls/xlsx/csv)

Click **Run** → get a single workbook with three sheets:
    • Supplier vs Raw (carrier+realm)
    • Supplier vs Customer (realm)
    • Raw vs Customer (customer+realm)

The sheet cells are traffic-lighted with the thresholds below.
"""

from __future__ import annotations

import datetime as dt
import io
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ──────────────────────────────── Config & thresholds
@dataclass(frozen=True)
class Threshold:
    warn: int
    fail: int


@dataclass(frozen=True)
class Config:
    REALM:    Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    # Header row (0-based) for the billing file – row 5 in Excel
    BILLING_HEADER_ROW: Final[int] = 4

    # Regex to yank realm from “Product/Service” in billing
    REGEX_REALM: Final[re.Pattern] = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\w+)", re.I)

    # Canonical schema (aliases)
    SCHEMA: Final[dict[str, dict[str, list[str]]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier": ["carrier"],
                "realm":   ["realm"],
                "data_mb": ["total_mb", "usage_mb", "data_mb"],
            },
            "raw": {
                "date":     ["date"],
                "msisdn":   ["msisdn"],
                "sim":      ["sim_serial", "sim"],
                "customer": ["customer_code", "customer"],
                "realm":    ["realm"],
                "carrier":  ["carrier"],
                "data_mb":  ["total_usage_(mb)", "total_usage_mb", "usage_mb", "data_mb"],
            },
            "billing": {
                "customer": ["customer_code", "customer"],
                "product":  ["product/service", "product_service", "product"],
                "qty":      ["qty", "quantity"],
            },
        }
    )

    AUTO_WIDTH: Final[bool] = True   # auto-size cols in XLSX


CFG = Config()  # ──────────────────────────────────────────────────────────────


# ──────────────────────────────── Generic helpers
def _seek_start(buf):
    try:
        buf.seek(0)
    except Exception:
        pass


def _clean_key(s: pd.Series, lower: bool = True) -> pd.Series:
    """
    Collapse *all* exotic whitespace, strip, and normalise unicode so
    "abc " == "abc" == "ABC".
    """
    out = (
        s.fillna("")
        .astype(str)
        .apply(lambda x: unicodedata.normalize("NFKC", x))
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return out.str.lower() if lower else out


def _coerce_numeric(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    cleaned = (
        s.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0).astype(float)


def _std_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    cols2drop = []
    lookups = {k: {k, *(a.lower().replace(" ", "_") for a in v)}
               for k, v in mapping.items()}

    for canon, aliases in lookups.items():
        hits = [c for c in df.columns if c.lower().replace(" ", "_") in aliases]
        if not hits:
            raise ValueError(f"Missing required column '{canon}'.")
        keep = hits[0]
        if keep != canon:
            df = df.rename(columns={keep: canon})
        cols2drop.extend(hits[1:])

    if cols2drop:
        df = df.drop(columns=cols2drop)
    return df


def _assert_keys(df: pd.DataFrame, keys: List[str], name="DataFrame"):
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{name} missing column(s): {missing}")


def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    _assert_keys(df, by + [src], "agg() input")
    # fill NaNs in keys *after* converting to object to avoid Categorical errors
    filled = df.copy()
    for k in by:
        if filled[k].isnull().any():
            filled[k] = filled[k].astype(object).fillna("<nan>")
    return (
        filled.groupby(by, as_index=False, observed=True)[src]
        .sum()
        .rename(columns={src: tgt})
    )


def _status_series(delta: pd.Series, th: Threshold) -> pd.Series:
    absd = delta.abs()
    bins = [0, th.warn, th.fail, np.inf]
    labels = ["OK", "WARN", "FAIL"]
    return pd.cut(absd, bins=bins, labels=labels, right=False,
                  include_lowest=True).astype("category")


# ──────────────────────────────── File loaders
def _load_excel_or_csv(buf, **kw) -> pd.DataFrame:
    _seek_start(buf)
    name = getattr(buf, "name", "file").lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **kw)
    return pd.read_csv(buf, encoding_errors="replace", **kw)


def load_supplier(buf) -> pd.DataFrame:
    df = _load_excel_or_csv(buf)
    df.columns = _clean_key(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["supplier"])

    df["carrier"] = _clean_key(df["carrier"], lower=False).str.upper()
    df["realm"] = _clean_key(df["realm"])
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    _assert_keys(df, ["carrier", "realm", "data_mb"], "Supplier")
    return df[["carrier", "realm", "data_mb"]]


def load_raw(buf) -> pd.DataFrame:
    df = _load_excel_or_csv(buf)
    df.columns = _clean_key(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["raw"])

    df["carrier"] = _clean_key(df["carrier"], lower=False).str.upper().replace("", "<nan>")
    df["realm"]   = _clean_key(df["realm"]).replace("", "<nan>")
    df["customer"] = _clean_key(df["customer"]).replace("", "<nan>")
    df["data_mb"]  = _coerce_numeric(df["data_mb"])

    _assert_keys(df, ["carrier", "realm", "customer", "data_mb"], "Raw usage")
    return df[["carrier", "realm", "customer", "data_mb"]]


def load_billing(buf) -> pd.DataFrame:
    df = _load_excel_or_csv(buf, header=CFG.BILLING_HEADER_ROW)
    df.columns = _clean_key(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["billing"])

    df["customer"] = _clean_key(df["customer"]).replace("", "<nan>")
    df["qty"] = _coerce_numeric(df["qty"])

    # derive realm from product
    prod = df["product"].astype(str)
    df["realm"] = prod.str.extract(CFG.REGEX_REALM)[0]
    df["realm"] = _clean_key(df["realm"]).replace("", "<nan>")

    # classify bundle / excess
    is_bundle = prod.str.contains("bundle", case=False, na=False)
    is_excess = prod.str.contains("excess", case=False, na=False)
    df["bundle_mb"] = df["qty"].where(is_bundle, 0.0)
    df["excess_mb"] = df["qty"].where(is_excess & ~is_bundle, 0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    _assert_keys(df, ["customer", "realm", "billed_mb"], "Billing")
    return df[["customer", "realm", "billed_mb"]]


# ──────────────────────────────── Compare & Excel
def compare(left: pd.DataFrame, right: pd.DataFrame, on: List[str],
            lcol: str, rcol: str, th: Threshold) -> pd.DataFrame:
    _assert_keys(left, on + [lcol], "compare:left")
    _assert_keys(right, on + [rcol], "compare:right")
    cmp = pd.merge(left, right, on=on, how="outer", copy=False)

    cmp[lcol] = _coerce_numeric(cmp[lcol].fillna(0.0))
    cmp[rcol] = _coerce_numeric(cmp[rcol].fillna(0.0))
    for k in on:
        if cmp[k].isnull().any():
            cmp[k] = cmp[k].astype(object).fillna("<nan>")

    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    denom = cmp[[lcol, rcol]].max(axis=1).replace(0, np.nan)
    cmp["pct_delta"] = (cmp["delta_mb"] / denom) * 100
    cmp["pct_delta"] = cmp["pct_delta"].round(6).fillna(0)
    cmp["status"] = _status_series(cmp["delta_mb"], th)

    order = on + [lcol, rcol, "delta_mb", "pct_delta", "status"]
    return cmp[order]


def create_excel(tabs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        fmt = {
            k: wb.add_format({"bg_color": c, "font_color": t, "bold": True})
            for k, c, t in [
                ("OK",   "#C6EFCE", "#006100"),
                ("WARN", "#FFEB9C", "#9C6500"),
                ("FAIL", "#FFC7CE", "#9C0006"),
            ]
        }
        numfmt = wb.add_format({"num_format": "0.00"})

        for sheet, df in tabs.items():
            df.to_excel(xl, sheet_name=sheet[:31], index=False)
            ws = xl.sheets[sheet[:31]]

            if "status" in df.columns:
                col = df.columns.get_loc("status")
                n = len(df)
                for k, f in fmt.items():
                    ws.conditional_format(1, col, n, col, {
                        "type": "cell", "criteria": "==", "value": f'"{k}"', "format": f,
                    })

            for i, c in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[c]):
                    ws.set_column(i, i, None, numfmt)
                if CFG.AUTO_WIDTH:
                    width = min(60, max(len(str(c)), df[c].astype(str).str.len().max()) + 2)
                    ws.set_column(i, i, width)

    buf.seek(0)
    return buf.read()


# ──────────────────────────────── Streamlit UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files and click **Run** to download the result.")

col_sup, col_raw = st.columns(2)
with col_sup:
    f_sup = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"], key="sup")
with col_raw:
    f_raw = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"], key="raw")

f_bill = st.file_uploader("Billing file", type=["csv", "xls", "xlsx"], key="bill")

run = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    with st.spinner("Processing…"):
        try:
            sup  = load_supplier(f_sup)
            raw  = load_raw(f_raw)
            bill = load_billing(f_bill)

            # ── Aggregations
            sup_car_realm = _agg(sup,  ["carrier", "realm"],          "data_mb", "supplier_mb")
            sup_realm     = _agg(sup,  ["realm"],                     "data_mb", "supplier_mb")
            raw_car_realm = _agg(raw,  ["carrier", "realm"],          "data_mb", "raw_mb")
            raw_cust      = _agg(raw,  ["customer", "realm"],         "data_mb", "raw_mb")
            bill_realm    = _agg(bill, ["realm"],                     "billed_mb", "customer_billed_mb")
            bill_cust     = _agg(bill, ["customer", "realm"],         "billed_mb", "customer_billed_mb")

            # ── Comparisons
            cmp_sup_raw   = compare(sup_car_realm, raw_car_realm, ["carrier", "realm"],
                                    "supplier_mb", "raw_mb", CFG.REALM)
            cmp_sup_bill  = compare(sup_realm,     bill_realm,    ["realm"],
                                    "supplier_mb", "customer_billed_mb", CFG.REALM)
            cmp_raw_bill  = compare(raw_cust,      bill_cust,     ["customer", "realm"],
                                    "raw_mb", "customer_billed_mb", CFG.CUSTOMER)

        except Exception as e:
            st.error(f"Reconciliation failed: {e}")
            st.stop()

    report = create_excel({
        "Supplier_vs_Raw":      cmp_sup_raw,
        "Supplier_vs_Customer": cmp_sup_bill,
        "Raw_vs_Customer":      cmp_raw_bill,
    })

    st.download_button(
        "⬇️  Download reconciliation workbook",
        data=report,
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
