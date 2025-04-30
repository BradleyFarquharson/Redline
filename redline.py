#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation
==================================
Upload the three source files (Supplier, Raw usage, Customer billing),
hit •Run• and download a single Excel workbook with the three comparison
tabs.
"""
from __future__ import annotations

import datetime as dt
import io
import re
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, List

import numpy as np
import pandas as pd
import streamlit as st
import xlsxwriter

# ─────────────────────────── CONFIG ────────────────────────────
@dataclass(frozen=True)
class _Th:                       # thresholds in MB
    warn: int                   # |Δ| ≤ warn  → OK
    fail: int                   # warn < |Δ| ≤ fail → WARN, else FAIL


@dataclass(frozen=True)
class _Cfg:
    REALM:    _Th = _Th(5, 20)
    CUSTOMER: _Th = _Th(10, 50)

    REALM_RX: re.Pattern = re.compile(
        r"(?<=\s-\s)([A-Za-z]{2}\s?\d+)", re.I
    )                           # pull "ZA 3" etc. out of product string

    BILL_HDR_SCAN_ROWS: int = 8  # max rows to scan for billing header

    # canonical → aliases
    SCHEMA: dict[str, dict[str, List[str]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier":  ["carrier"],
                "realm":    ["realm"],
                "subs_qty": ["subscription_qty", "subscription", "qty"],
                "data_mb":  [
                    "total_mb",
                    "usage_mb",
                    "total_usage_mb",
                    "total_usage_(mb)",
                    "total_usage",
                    "data_mb",
                ],
            },
            "raw": {
                "date":     ["date"],
                "msisdn":   ["msisdn"],
                "sim":      ["sim_serial", "sim"],
                "customer": ["customer_code", "customer"],
                "realm":    ["realm"],
                "carrier":  ["carrier"],
                "data_mb":  [
                    "total_usage_(mb)",
                    "total_usage_mb",
                    "usage_mb",
                    "data_mb",
                    "total_mb",
                ],
                "status":   ["status"],
            },
            "billing": {
                "customer": ["customer_code", "customer_co", "customer"],
                "product":  ["product/service", "product_service", "product"],
                "qty":      ["qty", "quantity"],
            },
        }
    )


CFG = _Cfg()

# reverse-lookup once (O(aliases)) → O(n) header resolution
ALIASES: dict[str, str] = {
    re.sub(r"\s+", "_", v.strip().lower()): canon
    for cat in CFG.SCHEMA.values()
    for canon, alist in cat.items()
    for v in [canon, *alist]
}


# ────────────────────────── HELPERS ────────────────────────────
_NRX = re.compile(r"[^\d.\-]")


def _n(col: str) -> str:
    """normalise header → snake_case"""
    return re.sub(r"\s+", "_", col.strip().lower())


def _coerce_numeric(s: pd.Series) -> pd.Series:
    """Single-pass numeric coercion; dashes/empty → 0"""
    out = (
        pd.to_numeric(
            s.astype(str).str.replace(_NRX, "", regex=True), errors="coerce"
        )
        .fillna(0.0)
        .astype(float)
    )
    return out


def _status(delta: pd.Series, th: _Th) -> pd.Series:
    """Vectorised OK/WARN/FAIL"""
    a = delta.abs().to_numpy()
    return pd.Series(
        np.select([a <= th.warn, a <= th.fail], ["OK", "WARN"], "FAIL"),
        dtype="category",
    )


def _std_cols(df: pd.DataFrame, cat: str, file_name: str) -> pd.DataFrame:
    """Rename columns via global alias map."""
    df = df.rename(columns=lambda c: ALIASES.get(_n(c), _n(c))).copy()
    for canon in CFG.SCHEMA[cat]:
        if canon not in df.columns:
            raise ValueError(
                f"Required column '{canon}' not found in {file_name}"
            )
    return df


def _find_hdr_row(buf_bytes: bytes) -> int:
    """Scan first N rows of billing sheet to locate header."""
    sniff = pd.read_excel(io.BytesIO(buf_bytes), nrows=CFG.BILL_HDR_SCAN_ROWS)
    sniff.columns = [_n(c) for c in sniff.columns]

    needed = set(CFG.SCHEMA["billing"])
    a = sniff.fillna("").astype(str).applymap(_n).values
    hits = np.isin(a, list(needed))
    return int(np.argmax(hits.any(1))) if hits.any() else 0


# ────────────────────────── FILE READERS ───────────────────────
def _read_any(buf: IO[bytes], name: str, **kw) -> pd.DataFrame:
    ext = Path(name).suffix.lower()
    buf.seek(0)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(buf, **kw)
    if ext == ".csv":
        return pd.read_csv(buf, sep=None, engine="python", **kw)
    raise ValueError(f"Unsupported file type {ext!s} for {name}")


@st.cache_data(hash_funcs={io.BytesIO: lambda _: None}, show_spinner=False)
def _load_supplier(buf: IO[bytes], name: str) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, "supplier", name)

    df["carrier"] = df["carrier"].astype(str).str.upper()
    df["realm"] = df["realm"].astype(str).str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    return df[["carrier", "realm", "data_mb"]]


@st.cache_data(hash_funcs={io.BytesIO: lambda _: None}, show_spinner=False)
def _load_raw(buf: IO[bytes], name: str) -> pd.DataFrame:
    df = _read_any(buf, name)
    df = _std_cols(df, "raw", name)

    for col in ("carrier", "realm", "customer"):
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"": "<nan>", "nan": "<nan>"})
        )
    df["carrier"] = df["carrier"].str.upper()
    df["realm"] = df["realm"].str.lower()
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    return df[["customer", "carrier", "realm", "data_mb"]]


@st.cache_data(hash_funcs={io.BytesIO: lambda _: None}, show_spinner=False)
def _find_header_row(buf: io.BytesIO, file_name: str,
                     max_scan: int = 30) -> int:
    """
    Return the row-index whose *normalised* cells contain at least one
    of the customer aliases – that is the actual header row.
    """
    aliases = { _n(a) for a in CFG.SCHEMA["billing"]["customer"] }
    # read the first `max_scan` rows *without* header
    peek = pd.read_excel(buf, nrows=max_scan, header=None, engine=None)
    buf.seek(0)                                    # rewind for the real read

    norm = peek.fillna("").astype(str).applymap(_n)
    hits = norm.apply(lambda col: col.isin(aliases))
    if not hits.values.any():
        # fall back to row CFG.BILL_HDR and let std_cols raise if needed
        return CFG.BILL_HDR

    return hits.any(1).idxmax()           # first row containing an alias


def _load_billing(buf, file_name: str) -> pd.DataFrame:
    """Load and process billing data (Customer Detail export)."""
    hdr = _find_header_row(buf, file_name)
    df  = _read_any(buf, file_name, header=hdr)    # second, clean read
    df.columns = [_n(c) for c in df.columns]
    df = _std_cols(df, CFG.SCHEMA["billing"], file_name)

    # --- (rest identical) ---------------------------------------------
    df["qty"] = _coerce_numeric(df["qty"])
    if (df["qty"] < 0).any():
        st.warning(f"Negative values found in 'qty' for {file_name}")

    df["customer"] = (df["customer"].astype(str)
                                   .str.strip()
                                   .replace({"": "<nan>", "nan": "<nan>"}))

    df["realm"] = (df["product"].astype(str)
                               .str.extract(CFG.REALM_RX)[0]
                               .str.lower()
                               .fillna("<nan>"))

    unmatched = df["realm"].eq("<nan>").sum()
    if unmatched:
        st.warning(f"{unmatched} products did not match realm pattern "
                   f"in {file_name}")

    is_bundle = df["product"].str.contains("bundle",  case=False, na=False)
    is_excess = df["product"].str.contains("excess",  case=False, na=False) & ~is_bundle
    df["bundle_mb"] = np.where(is_bundle, df["qty"],   0.0)
    df["excess_mb"] = np.where(is_excess, df["qty"],   0.0)
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    return df[["customer", "realm",
               "bundle_mb", "excess_mb", "billed_mb"]]

# ────────────────────────── AGG & COMPARE ──────────────────────
def _agg(df: pd.DataFrame, by: list[str], src: str, tgt: str) -> pd.DataFrame:
    return (
        df.assign(**{c: df[c].fillna("<nan>").astype("category") for c in by})
        .groupby(by, as_index=False, observed=True, sort=False)[src]
        .sum()
        .rename(columns={src: tgt})
    )


def _compare(
    l: pd.DataFrame, r: pd.DataFrame, keys: list[str], lcol: str, rcol: str, th: _Th
) -> pd.DataFrame:
    cmp = l.merge(r, on=keys, how="outer").fillna({lcol: 0.0, rcol: 0.0})
    cmp[lcol] = _coerce_numeric(cmp[lcol])
    cmp[rcol] = _coerce_numeric(cmp[rcol])
    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"] = _status(cmp["delta_mb"], th)
    return cmp


# ────────────────────────── STREAMLIT UI ──────────────────────
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
st.caption(
    "Upload **Supplier**, **Raw-usage** & **Customer-billing** files, then click **Run**."
)

c1, c2 = st.columns(2)
f_sup = c1.file_uploader("Supplier", type=("csv", "xls", "xlsx"), key="sup")
f_raw = c2.file_uploader("Raw usage", type=("csv", "xls", "xlsx"), key="raw")
f_bill = st.file_uploader("Customer billing", type=("csv", "xls", "xlsx"), key="bill")

run = st.button("Run", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    with st.spinner("Running reconciliation …"):
        try:
            sup = _load_supplier(f_sup, f_sup.name)
            raw = _load_raw(f_raw, f_raw.name)
            bill = _load_billing(f_bill, f_bill.name)

            # ── aggregates
            sup_rlm = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
            sup_tot = _agg(sup, ["realm"], "data_mb", "supplier_mb")
            raw_rlm = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
            raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")
            bill_rlm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
            bill_cust = _agg(
                bill, ["customer", "realm"], "billed_mb", "customer_billed_mb"
            )

            # ── comparisons
            tab1 = _compare(
                sup_rlm, raw_rlm, ["carrier", "realm"], "supplier_mb", "raw_mb", CFG.REALM
            )
            tab2 = _compare(
                sup_tot,
                bill_rlm,
                ["realm"],
                "supplier_mb",
                "customer_billed_mb",
                CFG.REALM,
            )
            tab3 = _compare(
                raw_cust,
                bill_cust,
                ["customer", "realm"],
                "raw_mb",
                "customer_billed_mb",
                CFG.CUSTOMER,
            )

            # ── write Excel
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
                def _sheet(df: pd.DataFrame, name: str):
                    df.to_excel(xl, sheet_name=name[:31], index=False)

                    ws = xl.sheets[name[:31]]
                    # auto-width
                    for i, col in enumerate(df.columns):
                        width = max(10, int(df[col].astype(str).str.len().max()) + 2)
                        ws.set_column(i, i, width)

                    # conditional formatting on status
                    status_col = df.columns.get_loc("status")
                    last_row = len(df) + 1
                    ws.conditional_format(
                        1,
                        status_col,
                        last_row,
                        status_col,
                        {
                            "type": "text",
                            "criteria": "containing",
                            "value": "FAIL",
                            "format": xl.book.add_format(
                                {"bg_color": "#F8696B", "font_color": "#FFFFFF"}
                            ),
                        },
                    )
                    ws.conditional_format(
                        1,
                        status_col,
                        last_row,
                        status_col,
                        {
                            "type": "text",
                            "criteria": "containing",
                            "value": "WARN",
                            "format": xl.book.add_format(
                                {"bg_color": "#FFEB84", "font_color": "#000000"}
                            ),
                        },
                    )

                _sheet(tab1, "Supplier_vs_Raw")
                _sheet(tab2, "Supplier_vs_Cust")
                _sheet(tab3, "Raw_vs_Cust")

            buf.seek(0)

            st.download_button(
                "⬇️ Download reconciliation workbook",
                data=buf.getvalue(),
                file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.success("Reconciliation complete!")
        except (ValueError, KeyError) as e:
            st.error(f"Error: {e}")
        except Exception as e:
            st.error("Reconciliation failed.")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())
