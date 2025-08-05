# app/services/invoice_vr_pdf.py
import pdfkit
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from pathlib import Path
import platform
from django.conf import settings

# EXACT order wanted in the PDF (VR name + Platform last)
STRICT_ORDER = [
    "Date", "Description",
    "Markup (10%)",
    "Discount",
    "Revenue GST", "Revenue PST",
    "Platform fees (Commission + Tax)",
    "Selling price",
    "Payout from platforms", "Unit Price", "Sales Tax", "Total",
    "VR name", "Platform",
]

# Normalize incoming names from Lark to our canonical headers above
ALIAS_MAP = {
    "Markup":      ["Markup", "markup", "Markup (10%)", "markup (10%)"],
    "Revenue GST": ["Revenue GST", "revenue gst", "GST (revenue)", "gst revenue", "gst"],
    "Revenue PST": ["Revenue PST", "revenue pst", "PST (revenue)", "pst revenue", "pst"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the dataframe has canonical headers:
    - Map known aliases to 'Markup', 'Revenue GST', 'Revenue PST'
    - If 'Markup (10%)' is missing but 'Markup' exists, create it for display
    """
    lower = {c.lower(): c for c in df.columns}
    renames = {}
    for canonical, aliases in ALIAS_MAP.items():
        for a in aliases:
            src = lower.get(a.lower())
            if src and canonical not in df.columns:
                renames[src] = canonical
                break
    if renames:
        df = df.rename(columns=renames)

    # Create display column "Markup (10%)" from "Markup" if needed
    if "Markup" in df.columns and "Markup (10%)" not in df.columns:
        df["Markup (10%)"] = df["Markup"]

    return df


def _strict(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the STRICT_ORDER columns, in that order."""
    cols = [c for c in STRICT_ORDER if c in df.columns]
    return df[cols]


def build_vr_pdf(df: pd.DataFrame, meta: dict) -> bytes:
    """
    Build the VR invoice PDF:
    - Normalize headers and enforce strict column order
    - Format dates and numbers
    - Render an HTML template styled to match the Excel-like sample
    - Include an in-grid footer row: 'Amount to be Paid' with value under 'Total'
    """
    # 1) normalize headers, 2) keep only strict columns in strict order
    df = _normalize_columns(df.copy())
    df = _strict(df)

    # Format records for the template (dates and numbers to 2dp)
    def fmt_num(v):
        try:
            return f"{float(v):.2f}"
        except Exception:
            return v

    records = []
    for _, row in df.iterrows():
        rec = {}
        for c in df.columns:
            val = row[c]
            if c.lower() == "date":
                try:
                    val = pd.to_datetime(val).strftime("%Y-%m-%d")
                except Exception:
                    pass
            elif c not in ("Description", "VR name", "Platform"):
                val = fmt_num(val)
            rec[c] = val
        records.append(rec)

    # Grand total for footer row
    grand_total = "0.00"
    if "Total" in df.columns:
        s = 0.0
        for rec in records:
            try:
                s += float(rec["Total"] or 0)
            except Exception:
                pass
        grand_total = f"{s:.2f}"

    # Template path (put file at: <project>/templates/email/vr_template.html)
    templates_root = Path(settings.BASE_DIR) / "templates" / "email"
    env = Environment(loader=FileSystemLoader(str(templates_root)))
    tpl = env.get_template("vr_template.html")

    # Find where "Total" sits so we can place the footer row correctly
    columns = list(df.columns)  # already strict-ordered
    try:
        total_idx = columns.index("Total")          # 0-based index of "Total"
    except ValueError:
        total_idx = len(columns) - 1                # fallback if missing

    label_colspan  = total_idx                      # cells from first col up to (not incl.) "Total"
    trailing_cells = len(columns) - total_idx - 1   # cells to the right of "Total"

    html_str = tpl.render(
        reference_no=f"VRINV#{datetime.now().strftime('%Y%m%d%H%M%S')}",
        date=datetime.now().strftime("%Y-%m-%d"),
        orders_list=records,          # already formatted
        columns=columns,              # in strict order
        filters=meta,
        grand_total=grand_total,
        label_colspan=label_colspan,
        trailing_cells=trailing_cells,
    )

    # Resolve wkhtmltopdf path robustly
    candidates = []
    if getattr(settings, "WKHTMLTOPDF_PATH", None):
        candidates.append(settings.WKHTMLTOPDF_PATH)
    if platform.system() == "Windows":
        candidates.append(r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    else:
        candidates.append("/usr/bin/wkhtmltopdf")

    wkhtml = next((p for p in candidates if Path(p).exists()), None)
    if not wkhtml:
        raise RuntimeError(
            "wkhtmltopdf binary not found. Set settings.WKHTMLTOPDF_PATH or install wkhtmltopdf."
        )

    config = pdfkit.configuration(wkhtmltopdf=wkhtml)
    options = {
        "orientation": "Landscape",
        "page-size": "A4",
        "zoom": "0.85",
        "margin-top": "8mm",
        "margin-bottom": "8mm",
        "margin-left": "8mm",
        "margin-right": "8mm",
    }

    # Generate PDF directly as bytes
    pdf_bytes = pdfkit.from_string(html_str, output_path=False, configuration=config, options=options)
    if not pdf_bytes:
        raise RuntimeError("PDF generation returned empty content")

    return pdf_bytes
