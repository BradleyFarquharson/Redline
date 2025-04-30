#!/usr/bin/env python3
"""
Redline — SIM-Bundle Reconciliation  v2.4
========================================
*   Upload three source files (supplier usage, iONLINE raw usage, customer
    billing), hit **Run**, download one reconciled workbook.
*   No intermediate tables are shown in Streamlit – the UI is just
    upload → process → download.
*   Fixes
        • realm extractor regex (billing) – far more forgiving  
        • missing-realm fallback (raw) – tries APN when Realm column blank  
        • stronger normaliser (_norm) to squash non-breaking spaces etc.
*   Minor UX polish – centred layout, dark Run button, no footer timestamp.
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

# ──────────────────────────────── Configuration
@dataclass(frozen=True)
class Threshold:  # comparison thresholds per grouping level
    warn: int
    fail: int


@dataclass(frozen=True)
class Config:
    REALM: Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    # Billing file header row (0-based index)
    BILLING_HEADER_ROW: Final[int] = 4

    # -------- Regex --------
    # much looser: looks for " ZA 3", "ZA3", "za 03", etc. after a separator
    REGEX_REALM: Final[re.Pattern] = re.compile(
        r"(?:-|/|\s)([A-Za-z]{2})\s*0?(\d+)", re.I
    )
    REGEX_TOTAL: Final[re.Pattern] = re.compile(r"grand\s+total", re.I)

    # -------- Column aliases --------
    SCHEMA: dict[str, dict[str, list[str]]] = field(
        default_factory=lambda: {
            "supplier": {
                "carrier": ["carrier"],
                "realm": ["realm", "apn"],
                "subs_qty": ["subscription_qty", "subscription", "subs_qty", "qty"],
                "data_mb": ["total_mb", "data_mb", "usage_mb"],
            },
            "raw": {
                "date": ["date"],
                "msisdn": ["msisdn"],
                "sim": ["sim_serial", "sim"],
                "customer": ["customer_code", "customer"],
                "realm": ["realm"],
                "apn": ["apn"],
                "carrier": ["carrier"],
                "data_mb": [
                    "total_usage_(mb)",
                    "total_usage_mb",
                    "usage_mb",
                    "data_mb",
                ],
                "status": ["status"],
            },
            "billing": {
                "customer": ["customer_co", "customer_code", "customer"],
                "product": ["product/service", "product_service", "product"],
                "qty": ["qty", "quantity"],
            },
        }
    )

    AUTO_WIDTH: Final[bool] = True


CFG = Config()

# ───────────────────────────── Normaliser


def _norm(
    s: pd.Series,
    *,
    lower: bool = True,
) -> pd.Series:
    """
    Unicode-normalise, strip, squeeze whitespace, optionally lowercase.
    Replaces NBSP / zero-width chars with space so duplicates collapse.
    """
    s = (
        s.fillna("")
        .astype(str)
        .apply(lambda x: unicodedata.normalize("NFKC", x))
        .str.replace(r"[\u00A0\u200B-\u200D]", " ", regex=True)  # invisible spaces
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return s.str.lower() if lower else s


# ─────────────────────────────── Column helpers


def _seek_start(buf):
    try:
        buf.seek(0)
    except Exception:
        pass


def _std_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    to_drop: list[str] = []
    cols = df.columns.tolist()

    for canon, aliases in mapping.items():
        targets = {canon.lower().replace(" ", "_")}
        targets.update(a.lower().replace(" ", "_") for a in aliases)

        hits = [
            c for c in cols if c.lower().replace(" ", "_") in targets  # type: ignore
        ]
        if not hits:
            raise ValueError(f"required column '{canon}' not found")

        keep = hits[0]
        if keep != canon:
            if canon in df.columns:
                to_drop.append(keep)
            else:
                df.rename(columns={keep: canon}, inplace=True)
                cols = [canon if c == keep else c for c in cols]

        to_drop.extend(h for h in hits[1:] if h != canon)

    if to_drop:
        df.drop(columns=list(set(to_drop)), inplace=True)
    return df


def _coerce_numeric(s: pd.Series) -> pd.Series:
    cleaned = (
        s.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0).astype(float)


def _categorise(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("category")


def _assert_keys(df: pd.DataFrame, keys: List[str], df_name="DataFrame"):
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{df_name} missing {missing}")


# ─────────────────────────────── Aggregation helpers


def _agg(df: pd.DataFrame, by: List[str], src: str, tgt: str) -> pd.DataFrame:
    _assert_keys(df, by + [src])
    df_filled = df.copy()
    for col in by:
        if df_filled[col].isnull().any():
            df_filled[col] = df_filled[col].astype(object).fillna("<nan>")

    out = (
        df_filled.groupby(by, as_index=False, observed=True)[src]
        .sum()
        .rename(columns={src: tgt})
    )
    return out


def _status_series(delta: pd.Series, th: Threshold) -> pd.Series:
    absd = delta.abs()
    bins = [0, th.warn, th.fail, np.inf]
    labels = ["OK", "WARN", "FAIL"]
    return pd.cut(
        absd, bins=bins, labels=labels, right=False, include_lowest=True
    ).astype("category")


# ─────────────────────────────── File readers


def _read(buf, **kw) -> pd.DataFrame:
    _seek_start(buf)
    name = getattr(buf, "name", "").lower()
    if name.endswith((".xls", ".xlsx")):
        return pd.read_excel(buf, engine="openpyxl", **kw)
    if name.endswith(".csv"):
        return pd.read_csv(buf, encoding_errors="replace", **kw)
    raise ValueError("unsupported file type")


# -------- supplier --------
def load_supplier(buf) -> pd.DataFrame:
    st.info("↻ Loading supplier file …")
    df = _read(buf)
    df.columns = _norm(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["supplier"])

    df = df[~df["realm"].astype(str).str.match(CFG.REGEX_TOTAL, na=False)]
    df["carrier"] = _norm(df["carrier"], lower=False).str.upper()
    df["realm"] = _norm(df["realm"])
    df["data_mb"] = _coerce_numeric(df["data_mb"])

    _categorise(df, ["carrier", "realm"])
    return df[["carrier", "realm", "data_mb"]]


# -------- raw usage --------
def load_raw(buf) -> pd.DataFrame:
    st.info("↻ Loading raw-usage file …")
    df = _read(buf)
    df.columns = _norm(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["raw"])

    df["data_mb"] = _coerce_numeric(df["data_mb"])
    df["realm"] = _norm(df["realm"])
    df["carrier"] = _norm(df["carrier"], lower=False).str.upper()
    df["customer"] = _norm(df["customer"], lower=False)

    # Fallback: if Realm empty but APN present, extract possible realm code
    if "apn" in df.columns:
        mask = df["realm"].eq("") | df["realm"].eq("<nan>")
        if mask.any():
            df.loc[mask, "realm"] = (
                _norm(
                    df.loc[mask, "apn"]
                    .str.extract(r"\b([A-Za-z]{2}\d{0,2})\b", expand=False)
                )
                .replace("", "<nan>")
            )

    df["realm"].replace("", "<nan>", inplace=True)

    _categorise(df, ["customer", "realm", "carrier", "status"])
    return df[["customer", "realm", "carrier", "data_mb"]]


# -------- billing --------
def load_billing(buf) -> pd.DataFrame:
    st.info("↻ Loading billing file …")
    df = _read(buf, header=CFG.BILLING_HEADER_ROW)
    df.columns = _norm(pd.Series(df.columns), lower=True)
    df = _std_cols(df, CFG.SCHEMA["billing"])

    df["qty"] = _coerce_numeric(df["qty"])
    df["customer"] = _norm(df["customer"], lower=False)

    # realm extractor
    extracted = df["product"].astype(str).str.extract(CFG.REGEX_REALM, expand=True)
    realm = extracted[0].fillna("") + extracted[1].fillna("")
    df["realm"] = _norm(realm).replace("", "<nan>")

    miss = (df["realm"] == "<nan>").sum()
    if miss:
        st.warning(f"Billing rows without recognisable realm: {miss}")

    # derive bundle / excess
    product_str = df["product"].astype(str)
    is_bundle = product_str.str.contains("bundle", case=False, na=False)
    is_excess = product_str.str.contains("excess", case=False, na=False)

    df["bundle_mb"] = 0.0
    df.loc[is_bundle, "bundle_mb"] = df.loc[is_bundle, "qty"]
    df["excess_mb"] = 0.0
    df.loc[is_excess & ~is_bundle, "excess_mb"] = df.loc[is_excess & ~is_bundle, "qty"]
    df["billed_mb"] = df["bundle_mb"] + df["excess_mb"]

    _categorise(df, ["customer", "realm"])
    return df[["customer", "realm", "bundle_mb", "excess_mb", "billed_mb"]]


# ─────────────────────────────── Compare


def compare(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: List[str],
    lcol: str,
    rcol: str,
    th: Threshold,
) -> pd.DataFrame:
    _assert_keys(left, on + [lcol], "left")
    _assert_keys(right, on + [rcol], "right")

    cmp = pd.merge(left, right, on=on, how="outer", copy=False)

    cmp[lcol] = _coerce_numeric(cmp[lcol].fillna(0.0))
    cmp[rcol] = _coerce_numeric(cmp[rcol].fillna(0.0))

    for key in on:
        cmp[key] = cmp[key].fillna("<nan>").astype("category")

    cmp["delta_mb"] = cmp[lcol] - cmp[rcol]
    cmp["status"] = _status_series(cmp["delta_mb"], th)
    _categorise(cmp, ["status"])

    return cmp[on + [lcol, rcol, "delta_mb", "status"]]


# ─────────────────────────────── Excel builder


def create_excel(tabs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        wb = xl.book
        fmt = {
            k: wb.add_format({"bg_color": v, "font_color": c, "bold": True})
            for k, v, c in (
                ("OK", "#C6EFCE", "#006100"),
                ("WARN", "#FFEB9C", "#9C6500"),
                ("FAIL", "#FFC7CE", "#9C0006"),
            )
        }
        num_fmt = wb.add_format({"num_format": "0.00"})

        for name, df in tabs.items():
            if df.empty:
                continue
            sheet = name[:31]
            df.to_excel(xl, sheet_name=sheet, index=False)
            ws = xl.sheets[sheet]

            if "status" in df.columns:
                col = df.columns.get_loc("status")
                n = len(df)
                for status, f in fmt.items():
                    ws.conditional_format(
                        1,
                        col,
                        n,
                        col,
                        {
                            "type": "cell",
                            "criteria": "==",
                            "value": f'"{status}"',
                            "format": f,
                        },
                    )

            for i, col_name in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[col_name]):
                    ws.set_column(i, i, None, num_fmt)
                if CFG.AUTO_WIDTH:
                    try:
                        width = (
                            max(df[col_name].astype(str).str.len().max(), len(col_name))
                            + 2
                        )
                    except Exception:
                        width = 15
                    ws.set_column(i, i, min(width, 60))

    buf.seek(0)
    return buf.read()


# ─────────────────────────────── Streamlit UI

st.set_page_config(page_title="Redline Reconciliation", layout="centered")

st.markdown(
    """
    <style>
        /* centre the uploader columns on wide screens */
        section.main > div { max-width: 1100px; margin: auto; }
        /* dark button */
        button[kind="primary"] { background:#111; color:#fff; border:1px solid #555; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three source files and click **Run** to download the reconciliation workbook.")

c1, c2 = st.columns(2)
with c1:
    f_sup = st.file_uploader("Supplier file", type=["csv", "xls", "xlsx"], key="sup")
with c2:
    f_raw = st.file_uploader("Raw usage file", type=["csv", "xls", "xlsx"], key="raw")

f_bill = st.file_uploader("Billing file", type=["csv", "xls", "xlsx"], key="bill")

run = st.button("Run", type="primary", disabled=not all((f_sup, f_raw, f_bill)))

if run:
    with st.spinner("Running reconciliation …"):
        try:
            sup = load_supplier(f_sup)
            raw = load_raw(f_raw)
            bill = load_billing(f_bill)

            # -- aggregate
            sup_realm = _agg(sup, ["carrier", "realm"], "data_mb", "supplier_mb")
            sup_realm_tot = _agg(sup, ["realm"], "data_mb", "supplier_mb")

            raw_realm = _agg(raw, ["carrier", "realm"], "data_mb", "raw_mb")
            raw_cust = _agg(raw, ["customer", "realm"], "data_mb", "raw_mb")

            bill_realm = _agg(bill, ["realm"], "billed_mb", "customer_billed_mb")
            bill_cust = _agg(bill, ["customer", "realm"], "billed_mb", "customer_billed_mb")

            # -- compare
            sup_vs_raw = compare(
                sup_realm,
                raw_realm,
                ["carrier", "realm"],
                "supplier_mb",
                "raw_mb",
                CFG.REALM,
            )
            sup_vs_cust = compare(
                sup_realm_tot,
                bill_realm,
                ["realm"],
                "supplier_mb",
                "customer_billed_mb",
                CFG.REALM,
            )
            raw_vs_cust = compare(
                raw_cust,
                bill_cust,
                ["customer", "realm"],
                "raw_mb",
                "customer_billed_mb",
                CFG.CUSTOMER,
            )

            report = create_excel(
                {
                    "Supplier_vs_Raw": sup_vs_raw,
                    "Supplier_vs_Customer": sup_vs_cust,
                    "Raw_vs_Customer": raw_vs_cust,
                }
            )

        except Exception as e:
            st.error(f"Reconciliation failed: {e}")
            st.stop()

    st.success("✓ reconciliation workbook ready")
    st.download_button(
        "⬇️  Download Excel",
        data=report,
        file_name=f"Redline_{dt.date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )