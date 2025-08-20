# billing/utilities/lark_ht_sync.py
# Drop-in: no new packages needed.

import os
import json
import time
import datetime
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import SSLError, ConnectionError, ReadTimeout, ChunkedEncodingError
from django.db.models import Q
from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry


# --- Config -------------------------------------------------------------------
LARK_APP_ID     = os.getenv("LARK_APP_ID", "cli_a8030393dd799010")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET", "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY")
LARK_BASE_ID    = os.getenv("LARK_BASE_ID", "OGP4b0T04a2QmesErNsuSkRTs4P")
LARK_TABLE_ID   = os.getenv("LARK_TABLE_ID", "tblzjjzgvDMYfb7c")

API_BASE = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}"
API_REC  = f"{API_BASE}/records"

# Safe batch sizes
BATCH_CREATE = 200
BATCH_UPDATE = 200
BATCH_DELETE = 200


# --- Resilient HTTP (fixes SSLEOF) -------------------------------------------
HTTP = requests.Session()
HTTP.headers.update({
    "Content-Type": "application/json; charset=utf-8",
    "Connection": "close",   # avoid stale keep-alive sockets
})

_retry = Retry(
    total=8, connect=8, read=8,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET","POST","PUT","PATCH","DELETE"]),
    respect_retry_after_header=True,
)
HTTP.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=50))

def _request(method: str, url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", (10, 90))  # (connect, read)
    for i in range(3):
        try:
            return HTTP.request(method, url, **kw)
        except (SSLError, ConnectionError, ReadTimeout, ChunkedEncodingError):
            try:
                HTTP.close()
            except:
                pass
            time.sleep(1.5 * (i + 1))
    return HTTP.request(method, url, **kw)  # last try; let caller raise


# --- Small helpers ------------------------------------------------------------
def _norm_id(v) -> str:
    return "" if v is None else str(v).strip()

def _norm_text(v):
    if isinstance(v, dict) and "text" in v: return v["text"]
    if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]: return v[0]["text"]
    if isinstance(v, str): return v.strip()
    return v

def _chunked(xs: List, n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]


# --- Token & metadata ---------------------------------------------------------
def get_lark_token() -> str:
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = _request("POST", url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    token = res.json().get("tenant_access_token")
    if not token:
        raise RuntimeError(f"Failed to get tenant_access_token: {res.text}")
    return token

def _auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}

def get_field_definitions(token: str) -> Dict[str, Dict]:
    url = f"{API_BASE}/fields"
    res = _request("GET", url, headers=_auth_headers(token))
    res.raise_for_status()
    items = (res.json().get("data") or {}).get("items", []) or []
    info = {}
    for f in items:
        ftype = f.get("type")
        opts = None
        if ftype == 3:
            prop = f.get("property") or {}
            opts = [o.get("name") for o in prop.get("options", []) if o.get("name")]
        info[f.get("field_name")] = {"type": ftype, "options": opts}
    return info

def validate_select_value(field_name: str, value: str, field_info: Dict) -> str:
    meta = field_info.get(field_name) or {}
    options = meta.get("options") or []
    val = _norm_id(value)
    if val in options:
        return val
    if "Unknown Restaurant" in options:
        return "Unknown Restaurant"
    return options[0] if options else val


# --- Read whole table once (no per-row GET) -----------------------------------
def fetch_all_records(token: str) -> List[Dict]:
    records, page_token = [], None
    while True:
        params = {"page_size": 500}
        if page_token: params["page_token"] = page_token
        res = _request("GET", API_REC, headers=_auth_headers(token), params=params)
        res.raise_for_status()
        data = res.json().get("data") or {}
        items = data.get("items") or []
        records.extend(items)
        if not data.get("has_more"): break
        page_token = data.get("page_token")
    return records

def build_existing_index(existing: List[Dict]) -> Tuple[Dict[str, str], Dict[str, Dict]]:
    by_order_id, fields_by_rec = {}, {}
    for r in existing:
        rid = r.get("record_id")
        flds = r.get("fields") or {}
        oid  = _norm_id(flds.get("Order ID"))
        if rid:
            fields_by_rec[rid] = flds
        if oid and rid and oid not in by_order_id:
            by_order_id[oid] = rid
    return by_order_id, fields_by_rec


# --- Batch endpoints ----------------------------------------------------------
def batch_create(token: str, rows: List[Dict]):
    if not rows: return
    url = f"{API_REC}/batch_create"
    headers = _auth_headers(token)
    for chunk in _chunked(rows, BATCH_CREATE):
        body = {"records": [{"fields": r} for r in chunk]}
        res = _request("POST", url, headers=headers, data=json.dumps(body))
        if not (200 <= res.status_code < 300):
            raise RuntimeError(f"batch_create failed: {res.status_code} {res.text}")

def batch_update(token: str, rows: List[Tuple[str, Dict]]):
    if not rows: return
    url = f"{API_REC}/batch_update"
    headers = _auth_headers(token)
    for chunk in _chunked(rows, BATCH_UPDATE):
        body = {"records": [{"record_id": rid, "fields": fields} for rid, fields in chunk]}
        res = _request("POST", url, headers=headers, data=json.dumps(body))
        if not (200 <= res.status_code < 300):
            raise RuntimeError(f"batch_update failed: {res.status_code} {res.text}")

def batch_delete(token: str, record_ids: List[str]):
    if not record_ids: return
    url = f"{API_REC}/batch_delete"
    headers = _auth_headers(token)
    for chunk in _chunked([r for r in record_ids if r], BATCH_DELETE):
        body = {"records": chunk}
        res = _request("POST", url, headers=headers, data=json.dumps(body))
        if not (200 <= res.status_code < 300):
            raise RuntimeError(f"batch_delete failed: {res.status_code} {res.text}")


# --- Dedupe helper (optional) -------------------------------------------------
def dedupe_lark_by_order_id(token: str) -> Dict[str, str]:
    existing = fetch_all_records(token)
    first_by_oid, dup_ids, empty_ids = {}, [], []
    for r in existing:
        rid = r.get("record_id")
        f   = r.get("fields") or {}
        oid = _norm_id(f.get("Order ID"))
        if not oid:
            empty_ids.append(rid); continue
        if oid in first_by_oid:
            dup_ids.append(rid)
        else:
            first_by_oid[oid] = rid
    if dup_ids or empty_ids:
        batch_delete(token, dup_ids + empty_ids)
    return first_by_oid


# --- Row formatter (backend -> Lark fields) -----------------------------------
def _format_row(data: Dict, field_info: Dict, order_obj, restaurant_obj) -> Dict:
    date_str = _norm_id(data.get("order_date"))
    if not date_str:
        return {}
    ts_ms = int(datetime.datetime.strptime(date_str[:10], "%Y-%m-%d").timestamp() * 1000)

    if order_obj and getattr(order_obj, "restaurant", None) and getattr(order_obj.restaurant, "name", None):
        rname = order_obj.restaurant.name
    elif restaurant_obj and getattr(restaurant_obj, "name", None):
        rname = restaurant_obj.name
    else:
        rname = "Unknown Restaurant"

    customer_name = (data.get("Customer") or
                 (getattr(order_obj, "customer", "") if order_obj else "")).strip() or "Unknown Customer"


    row = {
        "Order ID": _norm_id(data.get("order_id")),
        "Order Date": ts_ms,
        "Restaurant": validate_select_value("Restaurant", rname, field_info),
        "Payment Type": data.get("payment_type"),
        "Order Mode": validate_select_value("Order Mode", _norm_id(data.get("order_mode")), field_info),

        "Original Item Price": float(data.get("actual_item_price", 0) or 0),
        "Item Price": float(data.get("item_price", 0) or 0),
        "BOGO Item Inflation Percentage": float(data.get("BOGO_item_inflation_percentage", 0) or 0) / 100.0,
        "Discount": float(data.get("discount", 0) or 0),
        "Restaurant Discount": float(data.get("restaurant_discount", 0) or 0),
        "Hungrytiger Discount": float(data.get("hungrytiger_discount", 0) or 0),
        "Tax": float(data.get("tax", 0) or 0),
        "Selling price (inclusive of tax)": float(data.get("selling_price_inclusive_of_tax", 0) or 0),
        "Original Delivery Fees": float(data.get("original_delivery_fee", 0) or 0),
        "Customer absorb on delivery fees": float(data.get("customer_absorb_on_delivery_fees", 0) or 0),
        "Delivery fees expense": float(data.get("delivery_fee_expense", 0) or 0),
        "Commission Percentage": f'{float(data.get("commission_percentage", 0) or 0):.1f}%',
        "Commission Amount": float(data.get("commission_amount", 0) or 0),
        "Service fees to Restaurant": float(data.get("service_fees_to_restaurant", 0) or 0),
        "Service fee to Hungrytiger": float(data.get("service_fee_to_hungrytiger", 0) or 0),
        "Tips for restaurant": float(data.get("tips_for_restaurant", 0) or 0),
        "Bag fees": float(data.get("bag_fees", 0) or 0),
        "Container fees": float(data.get("container_fees", 0) or 0),
        "Amount to Restaurant": round(float(data.get("amount_to_restaurant", 0) or 0), 2),
         "Customer": customer_name,
        "On-Time Guarantee Fee": float(data.get("On-Time Guarantee Fee", data.get("otg_fee") or 0) or 0),
        "Delivery Fee": float(data.get("Delivery Fee", data.get("delivery_fee", data.get("original_delivery_fee") or 0)) or 0),
        "Refund Amount": float(data.get("Refund Amount", data.get("refund_amount") or 0) or 0),
    }
    # Only keep fields that exist in Bitable (avoid invalid-field errors)
    return {k: v for k, v in row.items() if k in field_info}


def _diff_fields(existing_fields: Dict, new_fields: Dict) -> Dict:
    changed = {}
    for k, v in new_fields.items():
        if _norm_text(existing_fields.get(k)) != _norm_text(v):
            changed[k] = v
    return changed


# --- Main sync (no extra installs) -------------------------------------------
def push_all_hungry_orders_direct(allow_delete_orphans: bool = True):
    from billing.models import Order, Location

    token = get_lark_token()
    field_info = get_field_definitions(token)

    # (Optional) one-time cleanup of dupes / empty IDs
    dedupe_lark_by_order_id(token)

    # Backend queryset
    primary_q   = Q(is_paid=True) | Q(payment_method=Order.PaymentMethod.CASH)
    completed_q = Q(status__iexact=getattr(getattr(Order, "Status", None), "COMPLETED", "completed"))
    exclude_q   = Q(customer__icontains="test") | Q(customer__iexact="Nazmulllll")

    base_qs = (
        Order.objects
        .filter(primary_q)
        .filter(restaurant__is_remote_Kitchen=True)
        .filter(completed_q)
        .exclude(exclude_q)
        .select_related("restaurant", "location")
        .order_by("created_date")
    )

    # Build all backend rows once
    backend_rows: Dict[str, Dict] = {}
    loc_ids = base_qs.values_list("location_id", flat=True).distinct()

    for loc in Location.objects.filter(id__in=loc_ids).select_related("restaurant"):
        rest = loc.restaurant
        orders = list(base_qs.filter(restaurant_id=rest.id, location_id=loc.id))
        order_map = {_norm_id(o.order_id): o for o in orders}

        _, obj = generate_excel_invoice_for_hungry(
            orders=orders, restaurant=rest, location=loc, only_data=True
        )

        for data in obj[0]:
            oid = _norm_id(data.get("order_id"))
            if not oid:
                continue
            row = _format_row(data, field_info, order_map.get(oid), rest)
            if row:
                backend_rows[oid] = row

    backend_ids = set(backend_rows.keys())
    print(f"Prepared backend rows: {len(backend_ids)}")

    # Snapshot Lark once and diff locally
    existing = fetch_all_records(token)
    by_order_id, fields_by_rec = build_existing_index(existing)
    existing_ids = set(by_order_id.keys())

    # Plan
    to_create_ids   = sorted(list(backend_ids - existing_ids))
    to_update_ids   = sorted(list(backend_ids & existing_ids))
    to_delete_recids = []
    if allow_delete_orphans:
        orphan_ids = sorted(list(existing_ids - backend_ids))
        to_delete_recids = [by_order_id[oid] for oid in orphan_ids if by_order_id.get(oid)]

    # Build payloads
    create_rows: List[Dict] = [backend_rows[oid] for oid in to_create_ids]
    update_rows: List[Tuple[str, Dict]] = []
    for oid in to_update_ids:
        rid = by_order_id.get(oid)
        if not rid:
            continue
        existing_fields = fields_by_rec.get(rid, {}) or {}
        changed = _diff_fields(existing_fields, backend_rows[oid])
        if changed:
            update_rows.append((rid, changed))

    print(f"Plan: create={len(create_rows)} update={len(update_rows)} delete={len(to_delete_recids)}")

    # Execute
    if create_rows:
        batch_create(token, create_rows)
        print(f"✅ Created: {len(create_rows)}")
    if update_rows:
        batch_update(token, update_rows)
        print(f"✅ Updated: {len(update_rows)}")
    if to_delete_recids:
        batch_delete(token, to_delete_recids)
        print(f"🗑️ Deleted: {len(to_delete_recids)}")

    print("🎉 Sync complete.")
