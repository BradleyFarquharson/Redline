#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation  v2.5
Upload three source files (supplier, raw usage, customer billing) → one
reconciliation workbook.  Zero intermediate tables, zero noise.
"""
from __future__ import annotations

import datetime as dt
import io
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Final, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ─────────────────────────── Config
@dataclass(frozen=True)
class Threshold:
    warn: int
    fail: int


@dataclass(frozen=True)
class Config:
    REALM: Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    BILLING_HEADER_ROW: Final[int] = 4

    REGEX_REALM: Final[re.Pattern] = re.compile(
        r"(?:-|/|\s)([A-Za-z]{2})\s*0?(\d+)", re.I
    )

    SCHEMA: dict[str, dict[str, list[str]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier": ["carrier"],
                "realm": ["realm"],
                "data_mb": ["total_mb", "data_mb", "usage_mb"],
            },
            "raw": {
                "customer": ["customer_code", "customer"],
                "realm": ["realm"],
                "carrier": ["carrier"],
                "data_mb": ["total_usage_mb", "usage_mb", "data_mb"],
            },
            "billing": {
                "customer": ["customer_code", "customer"],
                "product": ["product/service", "product"],
                "qty": ["qty", "quantity"],
            },
        }
    )

    AUTO_WIDTH: Final[bool] = True


CFG = Config()

# ─────────────────────────── helpers
def _norm(s: pd.Series, *, lower: bool = True) -> pd.Series:
    s = (
        s.fillna("")
        .astype(str)
        .apply(lambda x: unicodedata.normalize("NFKC", x))
        .str.replace(r"[\u00A0\u200B-\u200D]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return s.str.lower() if lower else s


def _std_cols(df, mapping):
    df = df.copy()
    cols = df.columns.tolist()
    drops = []
    for canon, aliases in mapping.items():
        targets = {canon.lower().replace(" ", "_"), *(a.lower().replace(" ", "_") for a in aliases)}
        hits = [c for c in cols if c.lower().replace(" ", "_") in targets]
        if not hits:
            raise ValueError(f"required column '{canon}' not found")
        keep = hits[0]
        if keep != canon:
            df.rename(columns={keep: canon}, inplace=True)
        drops.extend(h for h in hits[1:] if h != canon)
    if drops:
        df.drop(columns=list(set(drops)), inplace=True)
    return df


def _coerce_numeric(s):  # -> float Series
    cleaned = (
        s.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0).astype(float)


def _agg(df, by, src, tgt):
    df_f = df.copy()
    for col in by:
        df_f[col] = df_f[col].fillna("<nan>")
    out = (
        df_f.groupby(by, as_index=False, observed=True)[src]
        .sum()
        .rename(columns={src: tgt})
    )
    return out


def _status_series(delta, th):
    absd = delta.abs()
    bins, labels = [0, th.warn, th.fail, np.inf], ["OK", "WARN", "FAIL"]
    return pd.cut(absd, bins=bins, labels=labels, right=False, include_lowest=True)


# ─────────────────────────── File loaders
def _read(buf, **kw):
    buf.seek(0)
    name = getattr(buf, "name", "").lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **kw)
    if name.endswith(".csv"):
        return pd.read_csv(buf, encoding_errors="replace", **kw)
    raise ValueError("unsupported file type")


def load_supplier(buf):
    df = _read(buf)
    df.columns = _norm(pd.Series(df.columns))
    df = _std_cols(df, CFG.SCHEMA["supplier"])
    df["carrier"] = _norm(df["carrier"], lower=False).str.upper()
    df["realm"] = _norm(df["realm"])
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["carrier", "realm", "data_mb"]]


def load_raw(buf):
    df = _read(buf)
    df.columns = _norm(pd.Series(df.columns))
    df = _std_cols(df, CFG.SCHEMA["raw"])
    df["carrier"] = _norm(df["carrier"], lower=False).str.upper()
    df["realm"] = _norm(df["realm"]).replace("", "<nan>")
    df["customer"] = _norm(df["customer"], lower=False)
    df["data_mb"] = _coerce_numeric(df["data_mb"])
    return df[["customer", "realm", "carrier", "data_mb"]]


def load_billing(buf):
    df = _read(buf, header=CFG.BILLING_HEADER_ROW)
    df.columns = _norm(pd.Series(df.columns))
    df = _std_cols(df, CFG.SCHEMA["billing"])
    df["customer"] = _norm(df["customer"], lower=False)
    df["qty"] = _coerce_numeric(df["qty"])

    # realm extraction
    realm = df["product"].astype(str).str.extract(CFG.REGEX_REALM, expand=True)
    df["realm"] = _norm(realm[0].fillna("") + realm[1].fillna("")).replace("", "<nan>")

    prod = df["product"].astype(str)
    is_bundle = prod.str.contains("bundle", case=False, na=False)
    is_excess = prod.str.contains("excess", case=False, na=False)

    df["bundle_mb"] = 0.0
    df.loc[is_bundle, "bundle_mb"] = df.loc[is_bundle, "qty"]
    df["excess_mb"] = 0.0
    df.loc[is_excess & ~is_bundle, "excess_mb"] = df.loc[is_excess & ~is_bundle, "qty"]
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    return df[["customer", "realm", "billed_mb"]]


# ─────────────────────────── Compare
def compare(left, right, on, lcol, rcol, th):
    cmp = pd.merge(left, right, on=on, how="outer", copy=False)
    cmp[lcol] = _coerce_numeric(cmp[lcol].fillna(0.0))
    cmp[rcol] = _coerce_numeric(cmp[rcol].fillna(0.0))
    for k in on:
        cmp[k] = cmp[k].fillna("<nan>")
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"] = _status_series(cmp["delta_mb"], th)
    return cmp[on + [lcol, rcol, "delta_mb", "status"]]


# ─────────────────────────── Excel generator
def make_excel(tabs):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        fmt = {
            "OK": wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"}),
            "WARN": wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500"}),
            "FAIL": wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"}),
        }
        num_fmt = wb.add_format({"num_format": "0.00"})
        for name, df in tabs.items():
            df.to_excel(xl, sheet_name=name[:31], index=False)
            ws = xl.sheets[name[:31]]
            if "status" in df.columns:
                col = df.columns.get_loc("status")
                n = len(df)
                for k, f in fmt.items():
                    ws.conditional_format(1, col, n, col,
                                          {"type": "cell", "criteria": "==", "value": f'"{k}"', "format": f})
            for i, c in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[c]):
                    ws.set_column(i, i, None, num_fmt)
                if CFG.AUTO_WIDTH:
                    width = min(max(df[c].astype(str).str.len().max(), len(c)) + 2, 60)
                    ws.set_column(i, i, width)
    buf.seek(0)
    return buf.read()


# ─────────────────────────── Streamlit UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")
st.markdown(
    """
    <style>
      section.main > div { max-width: 1100px !important; margin:auto; }
      button[kind="primary"] {background:#111; color:#fff; border:1px solid #555;}
    </style>""",
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files, hit **Run**, download the Excel.")

c1, c2 = st.columns(2)
with c1:
    f_sup = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"])
with c2:
    f_raw = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"])

f_bill = st.file_uploader("Billing file", type=["csv", "xls", "xlsx"])

if st.button("Run", type="primary", disabled=not all((f_sup, f_raw, f_bill))):
    with st.spinner("Reconciling …"):
        try:
            sup = load_supplier(f_sup)
            raw = load_raw(f_raw)
            bill = load_billing(f_bill)

            sup_r = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
            sup_tot = _agg(sup, ["realm"], "data_mb", "supplier_mb")
            raw_r = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
            raw_c = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
            bill_r = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
            bill_c = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

            excel = make_excel(
                {
                    "Supplier_vs_Raw": compare(sup_r, raw_r, ["carrier", "realm"], "supplier_mb", "raw_mb", CFG.REALM),
                    "Supplier_vs_Customer": compare(sup_tot, bill_r, ["realm"], "supplier_mb", "customer_billed_mb", CFG.REALM),
                    "Raw_vs_Customer": compare(raw_c, bill_c, ["customer", "realm"], "raw_mb", "customer_billed_mb", CFG.CUSTOMER),
                }
            )
        except Exception as e:
            st.error(f"Reconciliation failed: {e}")
            st.stop()

    st.success("Done — download below")
    st.download_button(
        "⬇️  Download Excel",
        data=excel,
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
