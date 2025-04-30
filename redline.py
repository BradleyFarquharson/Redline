#!/usr/bin/env python3
"""
Redline — SIM/Bundle Reconciliation  ·  2025-04-30 (evening patch)

 • deterministic choice of *Customer Code* column
 • no lingering spinner on failure
 • still only: 3 uploads → Run → download workbook
"""
from __future__ import annotations
import datetime as dt, io, re, unicodedata
from dataclasses import dataclass, field
from typing import Final, List

import numpy as np, pandas as pd, streamlit as st, xlsxwriter

# ─────────────────────────── configuration
@dataclass(frozen=True)
class Threshold:  warn:int; fail:int
@dataclass(frozen=True)
class CFG:
    REALM:    Threshold = Threshold(5, 20)
    CUSTOMER: Threshold = Threshold(10, 50)

    BILLING_HEADER_ROW: Final[int] = 4
    REGEX_REALM: Final[re.Pattern] = re.compile(r"(?<=\s-\s)([A-Za-z]{2}\s?\w+)", re.I)

    SCHEMA: Final[dict[str, dict[str, list[str]]]] = field(default_factory=lambda:{
        "supplier": {"carrier":["carrier"],"realm":["realm"],
                     "data_mb":["total_mb","usage_mb","data_mb"]},
        "raw": {"customer":["customer_code","customer"],
                "realm":["realm"],"carrier":["carrier"],
                "data_mb":["total_usage_(mb)","usage_mb","data_mb"]},
        "billing": {"customer_code":["customer_code","customer co","customer code"],
                    "customer_desc":["customer"],     # <- long descriptive name
                    "product":["product/service","product"], "qty":["qty"]}
    })
    AUTO_WIDTH: Final[bool] = True
CFG = CFG()  # ────────────────────────────────────────────────


# ─────────────────────────── generic helpers
def _seek(b):                                   # rewind buffer
    try: b.seek(0)
    except Exception: pass

def _norm(series:pd.Series, lower=True):        # canonicalise string keys
    out = (series.fillna("").astype(str)
           .apply(lambda x: unicodedata.normalize("NFKC",x))
           .str.replace(r"\s+"," ",regex=True).str.strip())
    return out.str.lower() if lower else out

def _num(s:pd.Series)->pd.Series:               # robust numeric coerce
    if s.empty: return s
    cleaned=(s.fillna("").astype(str)
               .str.replace(",","",regex=False)
               .replace({"-":"0","": "0"}))
    return pd.to_numeric(cleaned,errors="coerce").fillna(0.0)

def _need(df,cols,name="frame"):                # assert columns exist
    miss=[c for c in cols if c not in df.columns]
    if miss: raise ValueError(f"{name} missing {miss}")

def _std(df, schema):                           # standardise headers
    df=df.copy()
    df.columns=_norm(pd.Series(df.columns),lower=True)
    drops=[]
    for canon,aliases in schema.items():
        hits=[c for c in df.columns if c.replace(" ","_") in
              {canon,*[a.replace(" ","_") for a in aliases]}]
        if not hits: raise ValueError(f"column '{canon}' not found")
        keep=hits[0]
        if keep!=canon: df.rename(columns={keep:canon},inplace=True)
        drops.extend(hits[1:])
    if drops: df.drop(columns=drops,inplace=True)
    return df

def _agg(df,by,src,tgt):
    _need(df,by+[src],"agg")
    c=df.copy()
    for k in by:
        if c[k].isnull().any(): c[k]=c[k].astype(object).fillna("<nan>")
    return (c.groupby(by,observed=True,as_index=False)[src]
              .sum().rename(columns={src:tgt}))

def _status(delta:pd.Series,t:Threshold):
    bins=[0,t.warn,t.fail,np.inf]; lbl=["OK","WARN","FAIL"]
    return pd.cut(delta.abs(),bins,labels=lbl,right=False,
                  include_lowest=True).astype("category")


# ─────────────────────────── loaders
def _read(buf,**k):
    _seek(buf); n=getattr(buf,"name","").lower()
    return (pd.read_excel(buf,engine="openpyxl",**k)
            if n.endswith(("xls","xlsx")) else
            pd.read_csv(buf,encoding_errors="replace",**k))

def load_supplier(b):
    df=_std(_read(b),CFG.SCHEMA["supplier"])
    df["carrier"]=_norm(df["carrier"],lower=False).str.upper()
    df["realm"]  =_norm(df["realm"])
    df["data_mb"]=_num(df["data_mb"])
    return df[["carrier","realm","data_mb"]]

def load_raw(b):
    df=_std(_read(b),CFG.SCHEMA["raw"])
    df["carrier"]=_norm(df["carrier"],lower=False).str.upper().replace("", "<nan>")
    df["realm"]  =_norm(df["realm"]).replace("", "<nan>")
    df["customer"]=_norm(df["customer"]).replace("", "<nan>")
    df["data_mb"]=_num(df["data_mb"])
    return df[["carrier","realm","customer","data_mb"]]

def load_billing(b):
    df=_std(_read(b,header=CFG.BILLING_HEADER_ROW),CFG.SCHEMA["billing"])

    # pick the code column – ALWAYS wins over long description
    df["customer"]=_norm(df["customer_code"]).replace("", "<nan>")

    df["qty"]=_num(df["qty"])
    prod=df["product"].astype(str)
    df["realm"]=_norm(prod.str.extract(CFG.REGEX_REALM)[0]).replace("", "<nan>")

    is_bundle=prod.str.contains("bundle",case=False,na=False)
    is_excess=prod.str.contains("excess",case=False,na=False)
    df["billed_mb"]=df["qty"].where(is_bundle|is_excess,0.0)

    return df[["customer","realm","billed_mb"]]


# ─────────────────────────── compare + xlsx
def compare(l,r,on,lcol,rcol,th):
    for d,n in ((l,"left"),(r,"right")): _need(d,on+[lcol if n=="left" else rcol],n)
    c=pd.merge(l,r,on=on,how="outer",copy=False)
    c[lcol]=_num(c[lcol].fillna(0)); c[rcol]=_num(c[rcol].fillna(0))
    for k in on: c[k]=c[k].astype(object).fillna("<nan>")
    c["delta_mb"]=c[lcol]-c[rcol]
    denom=c[[lcol,rcol]].max(axis=1).replace(0,np.nan)
    c["pct_delta"]=(c["delta_mb"]/denom*100).round(6).fillna(0)
    c["status"]=_status(c["delta_mb"],th)
    return c[on+[lcol,rcol,"delta_mb","pct_delta","status"]]

def to_xlsx(tabs):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine="xlsxwriter") as xl:
        wb=xl.book
        fmt={k:wb.add_format({"bg_color":c,"font_color":t,"bold":True})
             for k,c,t in [("OK","#C6EFCE","#006100"),
                           ("WARN","#FFEB9C","#9C6500"),
                           ("FAIL","#FFC7CE","#9C0006")]}
        numfmt=wb.add_format({"num_format":"0.00"})
        for name,df in tabs.items():
            if df.empty: continue
            df.to_excel(xl,name[:31],index=False)
            ws=xl.sheets[name[:31]]
            if "status" in df.columns:
                col=df.columns.get_loc("status"); n=len(df)
                for s,f in fmt.items():
                    ws.conditional_format(1,col,n,col,
                        {"type":"cell","criteria":"==","value":f'"{s}"',"format":f})
            for i,c in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[c]): ws.set_column(i,i,None,numfmt)
                if CFG.AUTO_WIDTH:
                    w=min(60,max(len(c),df[c].astype(str).str.len().max())+2)
                    ws.set_column(i,i,w)
    buf.seek(0); return buf.read()


# ─────────────────────────── Streamlit UI
st.set_page_config(page_title="Redline",layout="centered")
st.title("Redline — Multi-Source Usage Reconciliation")
st.caption("Upload the three files, click **Run**, then download the workbook.")

c1,c2=st.columns(2)
with c1: sup=st.file_uploader("Supplier file",type=["csv","xls","xlsx"])
with c2: raw=st.file_uploader("Raw usage file",type=["csv","xls","xlsx"])
bill=st.file_uploader("Billing file",type=["csv","xls","xlsx"])
run=st.button("Run",disabled=not all((sup,raw,bill)))

if run:
    try:
        with st.spinner("Reconciling…"):
            sup_df = load_supplier(sup)
            raw_df = load_raw(raw)
            bil_df = load_billing(bill)

            sup_car_realm=_agg(sup_df,["carrier","realm"],"data_mb","supplier_mb")
            sup_realm     =_agg(sup_df,["realm"],"data_mb","supplier_mb")
            raw_car_realm =_agg(raw_df,["carrier","realm"],"data_mb","raw_mb")
            raw_cust      =_agg(raw_df,["customer","realm"],"data_mb","raw_mb")
            bil_realm     =_agg(bil_df,["realm"],"billed_mb","customer_billed_mb")
            bil_cust      =_agg(bil_df,["customer","realm"],"billed_mb","customer_billed_mb")

            xlsx=to_xlsx({
                "Supplier_vs_Raw":      compare(sup_car_realm,raw_car_realm,
                                                ["carrier","realm"],
                                                "supplier_mb","raw_mb",CFG.REALM),
                "Supplier_vs_Customer": compare(sup_realm,bil_realm,["realm"],
                                                "supplier_mb","customer_billed_mb",CFG.REALM),
                "Raw_vs_Customer":      compare(raw_cust,bil_cust,
                                                ["customer","realm"],
                                                "raw_mb","customer_billed_mb",CFG.CUSTOMER)
            })

        st.download_button("⬇️ Download reconciliation workbook",
            xlsx,f"Redline_{dt.date.today():%Y%m%d}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:          # always clear spinner
        st.error(f"Reconciliation failed: {e}")
        st.stop()

st.markdown(f"<small>Generated {dt.datetime.now():%c}</small>",
            unsafe_allow_html=True)
