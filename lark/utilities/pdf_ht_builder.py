# billing/utilities/lark/pdf_ht_builder.py

from __future__ import annotations
import datetime as dt
from typing import List, Dict, Any

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.template.loader import render_to_string
from django.utils.text import slugify
from jinja2 import Environment, FileSystemLoader
import pdfkit

# Lark "Date" semantics: UTC+8 midnight window
UTC8 = dt.timedelta(hours=8)

# in billing/utilities/lark/pdf_ht_builder.py (top of file)
import os, platform

def _wkhtmltopdf_path() -> str:
    # If you set a custom path in Django settings, that wins.
    from django.conf import settings
    if getattr(settings, "WKHTMLTOPDF_CMD", None):
        return settings.WKHTMLTOPDF_CMD

    if platform.system() == "Windows":
        candidates = [
            r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe",
            r"C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe",
        ]
    else:
        candidates = ["/usr/bin/wkhtmltopdf", "/usr/local/bin/wkhtmltopdf"]

    for p in candidates:
        if os.path.exists(p):
            return p

    raise RuntimeError(f"wkhtmltopdf not found. Tried: {candidates}. "
                       f"Set settings.WKHTMLTOPDF_CMD to your path if installed elsewhere.")

def _lark_ms_to_nominal_date(ms: int | str) -> dt.date:
    ms = int(str(ms))
    dtu = dt.datetime.utcfromtimestamp(ms / 1000.0)
    return (dtu + UTC8).date()


# add this just above _to_date_str
def _norm_text(v: Any) -> str | None:
    if v is None:
        return None
    # handle {"text": "..."} or [{"text": "..."}]
    if isinstance(v, dict) and "text" in v:
        return str(v["text"]).strip()
    if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]:
        return str(v[0]["text"]).strip()
    # pass through numbers/strings
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()

def _to_str(v: Any) -> str:
    s = _norm_text(v)
    return s if s is not None else ""

def _to_num(v: Any) -> float:
    s = _norm_text(v)
    if s is None or s == "":
        return 0.0
    try:
        return float(str(s).replace(",", ""))
    except Exception:
        return 0.0


def _to_date_str(v: Any) -> str:
    try:
        return _lark_ms_to_nominal_date(v).isoformat()
    except Exception:
        return str(v)

def _to_num(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0

def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v).strip()

# Map only the fields used by the HTML rows; others are static in the template
_FIELD_ALIASES = {
    "order_date": ["Order Date", "order_date", "Date"],
    "order_id": ["Order ID", "order_id", "Id", "ID"],
    "actual_item_price": ["Actual Item Price", "Original Item Price", "Original Item  Price"],
    "item_price": ["Item Price", "Item price"],
    "discount": ["Discount"],
    "restaurant_discount": ["Restaurant Discount", "Discount bear by Restaurant"],
    "hungrytiger_discount": ["Hungrytiger Discount", "Discount bear by Hungrytiger", "HT Discount"],
    "selling_price_inclusive_of_tax": [
        "Selling price (inclusive of tax)",
        "Selling Price (inclusive of tax)",
        "Selling Price (Inclusive of Tax)"
    ],
    "original_delivery_fee": ["Original Delivery Fees", "Original Delivery Fee"],
    "delivery_fee_expense": ["Delivery fees expense", "Delivery Fee Expense"],
    "commission_amount": ["Commission Amount"],
    "amount_to_restaurant": ["Amount to Restaurant", "Amount To Restaurant"],
    # optional (used to split cash vs bkash summary if present)
    "payment_type": ["Payment Type", "payment_type"],
    "on_time_guarantee_fee": ["On-Time Guarantee Fee", "On Time Guarantee Fee", "OTG Fee", "on_time_guarantee_fee", "otg_fee", "OTG"],
    "delivery_fee":         ["Delivery Fee", "delivery_fee"],
    "refund_amount":        ["Refund Amount", "refund_amount"],
   
}

def _first(fields: Dict[str, Any], names: list[str], default=None):
    for n in names:
        if n in fields and fields[n] not in (None, ""):
            return fields[n]
    return default

def _map_row(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "order_date": _to_date_str(_first(fields, _FIELD_ALIASES["order_date"])),
        "order_id": _to_str(_first(fields, _FIELD_ALIASES["order_id"])),
        "actual_item_price": _to_num(_first(fields, _FIELD_ALIASES["actual_item_price"])),
        "item_price": _to_num(_first(fields, _FIELD_ALIASES["item_price"])),
        "discount": _to_num(_first(fields, _FIELD_ALIASES["discount"])),
        "restaurant_discount": _to_num(_first(fields, _FIELD_ALIASES["restaurant_discount"])),
        "hungrytiger_discount": _to_num(_first(fields, _FIELD_ALIASES["hungrytiger_discount"])),
        "selling_price_inclusive_of_tax": _to_num(_first(fields, _FIELD_ALIASES["selling_price_inclusive_of_tax"])),
        "original_delivery_fee": _to_num(_first(fields, _FIELD_ALIASES["original_delivery_fee"])),
        "delivery_fee_expense": _to_num(_first(fields, _FIELD_ALIASES["delivery_fee_expense"])),
        "commission_amount": _to_num(_first(fields, _FIELD_ALIASES["commission_amount"])),
        "amount_to_restaurant": _to_num(_first(fields, _FIELD_ALIASES["amount_to_restaurant"])),
        # optional for summary split
        "payment_type": _to_str(_first(fields, _FIELD_ALIASES.get("payment_type", []))),
        "otg_fee": _to_num(_first(fields, _FIELD_ALIASES["on_time_guarantee_fee"])),
        "delivery_fee": _to_num(_first(fields, _FIELD_ALIASES["delivery_fee"])),
        "refund_amount": _to_num(_first(fields, _FIELD_ALIASES["refund_amount"])),
    }

def _chunk(lst: list[dict], size: int = 25) -> list[list[dict]]:
    if size <= 0: size = 25
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def _sum_by_payment(orders: list[dict]) -> tuple[float, float]:
    # If no payment_type present we’ll treat all as cash (matches your static cells)
    cash = 0.0
    bkash = 0.0
    for o in orders:
        amt = float(o.get("selling_price_inclusive_of_tax") or 0.0)
        pt = (o.get("payment_type") or "").lower()
        if not pt or any(k in pt for k in ["cash", "cod", "person"]):
            cash += amt
        else:
            bkash += amt
    return (round(cash, 2), round(bkash, 2))

def render_ht_pdf_from_lark(
    *,
    rows: List[Dict[str, Any]],
    restaurant_name: str,
    start_ms: int,
    end_ms: int,
    template_name: str = "invoices/ht_orders_summary.html",
) -> dict:
    """
    Build PDF for HT using your HTML file and raw Lark rows.
    Returns: {"pdf_url": "...", "storage_path": "..."}
    """
    orders = [_map_row(r) for r in rows]
    total_amount_to_restaurant = round(sum(float(o.get("amount_to_restaurant") or 0.0) for o in orders), 2)
    cash, sales_bkash = _sum_by_payment(orders)

    start_nom = _lark_ms_to_nominal_date(start_ms).isoformat()
    end_nom   = _lark_ms_to_nominal_date(end_ms).isoformat()

    context = {
        "date": f"{start_nom} to {end_nom}",
        "restaurant": {"name": restaurant_name},
        # keep empty location to satisfy template keys
        "location": {"address": {"street_number": "", "street_name": "", "city": "", "zip": "", "country": ""}},
        "orders_list": _chunk(orders, size=25),
        "cash": cash,
        "sales_bkash": sales_bkash,
        "total_amount_to_restaurant": total_amount_to_restaurant,
    }

    html = render_to_string(template_name, context)

    config = pdfkit.configuration(wkhtmltopdf=_wkhtmltopdf_path())

    options = {
        "page-size": "A4",
        "orientation": "Landscape",
        "encoding": "UTF-8",
        "margin-top": "6mm",
        "margin-bottom": "6mm",
        "margin-left": "6mm",
        "margin-right": "6mm",
    }
    pdf_bytes = pdfkit.from_string(html, False, configuration=config, options=options)

    slug = slugify(restaurant_name or "restaurant")
    filename = f"HT_{slug}_{start_nom}_to_{end_nom}.pdf"
    storage_path = f"invoices/HT/{slug}/{filename}"
    saved_path = default_storage.save(storage_path, ContentFile(pdf_bytes))
    pdf_url = default_storage.url(saved_path)
    return {"pdf_url": pdf_url, "storage_path": saved_path}
