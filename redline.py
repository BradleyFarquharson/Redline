#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation  v2.5
----------------------------------------
Minimal Streamlit UI (upload → run → download).
Outputs an Excel workbook with a dashboard Summary tab and three
detail tabs:
    • Supplier_vs_Raw
    • Supplier_vs_Customer
    • Raw_vs_Customer
"""
from __future__ import annotations
import datetime as dt, io, re
from dataclasses import dataclass, field
from typing import Final, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ───────────────── Threshold model
@dataclass(frozen=True, slots=True)
class Threshold:
    warn: int
    fail: int

# ───────────────── Configuration
@dataclass(frozen=True, slots=True)
class Config:
    REALM:    Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    BILLING_HEADER_ROW: Final[int] = 4
    REGEX_REALM:  Final[re.Pattern] = re.compile(r'(?:\s-\s|:\s)([A-Za-z]{2}\s?\d+)$', re.I)
    REGEX_TOTAL:  Final[re.Pattern] = re.compile(r'grand\s+total', re.I)

    # canonical-name → aliases
    SCHEMA: Final[dict[str, dict[str, list[str]]]] = field(default_factory=lambda: {
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
    })
    AUTO_WIDTH: Final[bool] = True


CFG = Config()               # singleton

# ───────────────── Helper utils
def _seek_start(buf):  # rewind pointer safely
    try: buf.seek(0)
    except Exception: pass


def _std_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy(); drops=[]
    norm = lambda s: s.strip().lower().replace(" ", "_")
    for canon, aliases in mapping.items():
        hits = [c for c in df.columns if norm(c) in {norm(canon), *map(norm, aliases)}]
        if not hits:
            raise ValueError(f"Required column '{canon}' missing")
        keep = hits[0]
        if keep != canon:
            if canon in df.columns:
                drops.append(keep)
            else:
                df = df.rename(columns={keep: canon})
        drops.extend(h for h in hits[1:] if h != canon)
    df = df.drop(columns=list(set(drops)))
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _to_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str), errors="coerce").fillna(0.0)


def _categorise(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("category")


def _read_any(buf, **kw) -> pd.DataFrame:
    _seek_start(buf)
    name = getattr(buf, "name", "").lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **kw)
    if name.endswith(".csv"):
        return pd.read_csv(buf, encoding_errors="replace", **kw)
    raise ValueError(f"Unsupported file type: {name or '<buffer>'}")

# ───────────────── Loaders
@st.cache_data(show_spinner="Reading supplier file…")
def load_supplier(buf):
    df = _read_any(buf)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["supplier"])
    df = df[~df["realm"].astype(str).str.match(CFG.REGEX_TOTAL, na=False)]
    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"]   = df["realm"].astype(str).str.lower()
    df["data_mb"] = _to_float(df["data_mb"])
    _categorise(df, ["carrier", "realm"])
    return df[["carrier", "realm", "data_mb"]]


@st.cache_data(show_spinner="Reading raw usage file…")
def load_raw(buf):
    df = _read_any(buf)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["raw"])
    df["data_mb"]  = _to_float(df["data_mb"])
    df["realm"]    = df["realm"].astype(str).str.lower()
    df["carrier"]  = df["carrier"].astype(str).str.upper().fillna("UNKNOWN")
    df["customer"] = df["customer"].astype(str).fillna("<nan>")
    _categorise(df, ["customer", "realm", "carrier"])
    return df[["customer", "realm", "carrier", "data_mb"]]


@st.cache_data(show_spinner="Reading billing file…")
def load_billing(buf):
    df = _read_any(buf, header=CFG.BILLING_HEADER_ROW)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"])
    df["qty"]      = _to_float(df["qty"])
    df["customer"] = df["customer"].astype(str).fillna("<nan>")

    prod = df["product"].astype(str)
    df["realm"] = prod.str.extract(CFG.REGEX_REALM, expand=False).str.lower().fillna("<nan>")

    df["bundle_mb"] = df["excess_mb"] = 0.0
    is_bundle = prod.str.contains("bundle", case=False, na=False)
    is_excess = prod.str.contains("excess", case=False, na=False)
    df.loc[is_bundle,               "bundle_mb"] = df.loc[is_bundle, "qty"]
    df.loc[is_excess & ~is_bundle,  "excess_mb"] = df.loc[is_excess & ~is_bundle, "qty"]

    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]
    _categorise(df, ["customer", "realm"])
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]

# ───────────────── Aggregation / comparison
def _agg(df, by: List[str], src: str, tgt: str):
    return (df.groupby(by, as_index=False, observed=True)[src].sum()
              .rename(columns={src: tgt}))


def _status_series(delta: pd.Series, th: Threshold):
    bins   = [0, th.warn, th.fail, np.inf]
    labels = ["OK", "WARN", "FAIL"]
    return pd.cut(delta.abs(), bins=bins, labels=labels,
                  right=False, include_lowest=True).astype("category")


def compare(left, right, on: List[str], lcol: str, rcol: str, th: Threshold):
    cmp = left.merge(right, on=on, how="outer")
    cmp[lcol] = _to_float(cmp[lcol])
    cmp[rcol] = _to_float(cmp[rcol])
    cmp["delta_mb"]  = cmp[lcol] - cmp[rcol]
    cmp["pct_delta"] = np.where(cmp[rcol] == 0, np.nan,
                                cmp["delta_mb"] / cmp[rcol] * 100)
    cmp["status"]    = _status_series(cmp["delta_mb"], th)
    _categorise(cmp, ["status"])
    return cmp[on + [lcol, rcol, "delta_mb", "pct_delta", "status"]]

# ───────────────── Summary tab
def make_summary(sup_vs_raw, sup_vs_cust, raw_vs_cust) -> pd.DataFrame:
    rows = []
    for name, df in [("Supplier vs Raw", sup_vs_raw),
                     ("Supplier vs Customer", sup_vs_cust),
                     ("Raw vs Customer", raw_vs_cust)]:
        counts = df["status"].value_counts().reindex(["OK", "WARN", "FAIL"]).fillna(0).astype(int)
        total_abs_mb = df["delta_mb"].abs().sum()
        rows.append({
            "Comparison":     name,
            "OK":   counts.get("OK",   0),
            "WARN": counts.get("WARN", 0),
            "FAIL": counts.get("FAIL", 0),
            "Total |Δ| MB":   round(total_abs_mb, 2),
        })
    return pd.DataFrame(rows)

# ───────────────── Excel builder
def create_excel(tabs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        colours = {"OK": "#C6EFCE", "WARN": "#FFEB9C", "FAIL": "#FFC7CE"}
        txt     = {"OK": "#006100", "WARN": "#9C6500", "FAIL": "#9C0006"}
        fmt     = {k: wb.add_format({"bg_color": v, "font_color": txt[k], "bold": True})
                   for k, v in colours.items()}
        numfmt = wb.add_format({"num_format": "#,##0.00"})

        for name, df in tabs.items():
            if df.empty: continue
            df.to_excel(xl, sheet_name=name[:31], index=False)
            ws = xl.sheets[name[:31]]
            if "status" in df.columns:
                col = df.columns.get_loc("status")
                for k, f in fmt.items():
                    ws.conditional_format(1, col, len(df), col,
                                          {"type": "cell", "criteria": "==",
                                           "value": f'"{k}"', "format": f})
            for i, c in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[c]):
                    ws.set_column(i, i, None, numfmt)
                if CFG.AUTO_WIDTH:
                    width = min(max(len(str(c)),
                                    df[c].astype(str).str.len().max()) + 2, 60)
                    ws.set_column(i, i, width)
    buf.seek(0)
    return buf.read()

# ───────────────── UI
st.set_page_config(page_title="Redline Reconciliation", layout="centered")
st.markdown(
    """
    <style>
    div[data-testid="stFileDropzone"] > div > span {visibility:hidden;}
    div[data-testid="stFileDropzone"]::before {
        content:"Drop or browse…"; position:absolute; top:45%; left:50%;
        transform:translate(-50%,-50%); font-size:0.9rem; color:white;
    }
    .block-container {padding-top:1.3rem;}
    </style>
    """, unsafe_allow_html=True)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files and click **Run** to download the reconciliation workbook.")

# Uploaders: 2-column + full-width
c1, c2 = st.columns(2, gap="medium")
with c1:
    f_supplier = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"], key="sup")
with c2:
    f_raw = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"], key="raw")

st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)  # spacer
f_billing = st.file_uploader("Billing file", type=["csv", "xls", "xlsx"], key="bill")

st.markdown("<div style='height:1.0rem'></div>", unsafe_allow_html=True)  # spacer

run = st.button("Run Reconciliation", disabled=not all((f_supplier, f_raw, f_billing)), type="primary")

# ─── Main block
if run:
    try:
        sup  = load_supplier(f_supplier)
        raw  = load_raw(f_raw)
        bill = load_billing(f_billing)

        sup_realm     = _agg(sup, ["carrier","realm"], "data_mb", "supplier_mb")
        sup_realm_tot = _agg(sup, ["realm"],           "data_mb", "supplier_mb")
        raw_realm     = _agg(raw, ["carrier","realm"], "data_mb", "raw_mb")
        raw_cust      = _agg(raw, ["customer","realm"],"data_mb", "raw_mb")
        bill_realm    = _agg(bill,["realm"],           "billed_mb","customer_billed_mb")
        bill_cust     = _agg(bill,["customer","realm"],"billed_mb","customer_billed_mb")

        sup_vs_raw  = compare(sup_realm,     raw_realm,
                              ["carrier","realm"],
                              "supplier_mb","raw_mb", CFG.REALM)
        sup_vs_cust = compare(sup_realm_tot, bill_realm,
                              ["realm"],
                              "supplier_mb","customer_billed_mb", CFG.REALM)
        raw_vs_cust = compare(raw_cust,      bill_cust,
                              ["customer","realm"],
                              "raw_mb","customer_billed_mb", CFG.CUSTOMER)

        summary_df = make_summary(sup_vs_raw, sup_vs_cust, raw_vs_cust)

        excel_bytes = create_excel({
            "Summary":               summary_df,
            "Supplier_vs_Raw":       sup_vs_raw,
            "Supplier_vs_Customer":  sup_vs_cust,
            "Raw_vs_Customer":       raw_vs_cust,
        })

        st.success("Reconciliation complete – download your workbook below.")
        st.download_button(
            "⬇️ Download Excel report",
            data=excel_bytes,
            file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(f"Reconciliation failed: {e}")

st.markdown("---")
st.caption(dt.datetime.now().strftime("Generated %A %B %d, %Y %I:%M %p"))
