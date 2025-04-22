"""
Excel writer – creates one workbook with four tabs + audit.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict

import pandas as pd
import xlsxwriter

from .flags import Status


def _fmt(workbook):
    ok = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
    warn = workbook.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500"})
    fail = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
    return {"OK": ok, "WARN": warn, "FAIL": fail}


def write(report: Dict[str, pd.DataFrame], audit: Dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = out_dir / f"Redline_{dt.date.today():%Y%m%d}.xlsx"

    with pd.ExcelWriter(fname, engine="xlsxwriter") as xl:
        wb = xl.book
        styles = _fmt(wb)

        # 1 sheet per comparison
        for name, df in report.items():
            df.to_excel(xl, sheet_name=name[:31], index=False)
            ws = xl.sheets[name[:31]]
            # Conditional format on status
            status_col = df.columns.get_loc("status")
            ws.conditional_format(
                1,
                status_col,
                len(df),
                status_col,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "OK",
                    "format": styles["OK"],
                },
            )
            ws.conditional_format(
                1,
                status_col,
                len(df),
                status_col,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "WARN",
                    "format": styles["WARN"],
                },
            )
            ws.conditional_format(
                1,
                status_col,
                len(df),
                status_col,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "FAIL",
                    "format": styles["FAIL"],
                },
            )

        # Audit tab
        pd.DataFrame([audit]).to_excel(xl, sheet_name="Audit_Log", index=False)

    return fname 