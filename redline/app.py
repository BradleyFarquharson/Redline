"""
Streamlit front‑end.
Run with:  streamlit run redline/app.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

from redline.core import io, reconcile, report


def main():
    CFG = yaml.safe_load((Path(__file__).parent / "config" / "settings.yaml").read_text())
    OUTPUT_DIR = Path("./outputs")

    st.set_page_config(page_title="Redline – SIM‑Bundle Reconciliation", layout="wide")
    st.title("📊 Redline – SIM‑Bundle Reconciliation")

    uploaded_supplier = st.file_uploader("Supplier / MNO Usage Report", type=("csv", "xlsx"))
    uploaded_raw = st.file_uploader("iONLINE Raw Usage Report", type=("csv", "xlsx"))
    uploaded_billing = st.file_uploader("Customer Billing Report", type=("csv", "xlsx"))

    run_btn = st.button("Run reconciliation", disabled=not all([uploaded_supplier, uploaded_raw, uploaded_billing]))

    if run_btn:
        with st.spinner("Reading files…"):
            sup_df, sup_meta = io.read_file(uploaded_supplier, "supplier")
            raw_df, raw_meta = io.read_file(uploaded_raw, "raw")
            bill_df, bill_meta = io.read_file(uploaded_billing, "billing")

        st.success("Files loaded")

        with st.spinner("Reconciling…"):
            results = reconcile.run(sup_df, raw_df, bill_df, CFG)

        st.success("Done")

        # Display summary tables
        for key, df in results.items():
            st.subheader(key.replace("_", " ").title())
            st.dataframe(df)

        with st.spinner("Building Excel report…"):
            audit = {
                **sup_meta,
                **raw_meta,
                **bill_meta,
                "run_cfg": json.dumps(CFG, separators=(",", ":")),
            }
            file_path = report.write(results, audit, OUTPUT_DIR)

        st.success("Report ready")
        with open(file_path, "rb") as fh:
            st.download_button(
                label="📥 Download Excel",
                data=fh,
                file_name=file_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


if __name__ == "__main__":
    main() 