<<<<<<< HEAD
import requests
import uuid
from dateutil import parser
import datetime
import traceback
from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry

LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblzjjzgvDMYfb7c"

LARK_API_URL = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"

def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    return res.json().get("tenant_access_token")

def get_all_lark_records(token):
    headers = {"Authorization": f"Bearer {token}"}
    all_records = []
    page_token = None

    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token

        res = requests.get(LARK_API_URL, headers=headers, params=params)
        res.raise_for_status()
        data = res.json().get("data", {})
        all_records.extend(data.get("items", []))

        if not data.get("has_more"):
            break
        page_token = data.get("page_token")

    return all_records

def normalize_lark_value(val):
    if isinstance(val, dict) and "text" in val:
        return val["text"]
    if isinstance(val, list) and val and isinstance(val[0], dict) and "text" in val[0]:
        return val[0]["text"]
    return val

def get_field_definitions():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    fields = res.json().get("data", {}).get("items", [])
    field_info = {}

    for f in fields:
        options = [opt["name"] for opt in f["property"].get("options", [])] if f["type"] == 3 else None
        field_info[f["field_name"]] = {"type": f["type"], "options": options}
    return field_info

def validate_select_value(field_name, value, field_info):
    options = field_info.get(field_name, {}).get("options", [])
    if value in options:
        return {"text": value}
    elif "Unknown Restaurant" in options:
        # use existing "Unknown Restaurant" option if present
        return {"text": "Unknown Restaurant"}
    elif options:
        # if no "Unknown Restaurant" option exists, create it in Lark or just fallback
        return {"text": "Unknown Restaurant"}
    else:
        return None


def sync_ht_payout_record_to_lark(record, field_info, *, token=None, existing_map=None):
    # 1) Reuse token/map if the caller passed them
    if token is None:
        token = get_lark_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 2) Build payload using ONLY fields that exist in Lark (avoid invalid-field errors)
    fields_payload = {}
    for key, val in record.items():
        meta = field_info.get(key)
        if not meta:
            continue  # skip unknown columns
        if meta["type"] == 3:  # SingleSelect
            opts = meta.get("options") or []
            if isinstance(val, dict) and val.get("text") in opts:
                fields_payload[key] = val["text"]
            elif isinstance(val, str) and val in opts:
                fields_payload[key] = val
            elif opts:
                fields_payload[key] = opts[0]  # safe fallback
            # else: skip if no options configured
        else:
            fields_payload[key] = val

    order_id = fields_payload.get("Order ID")
    if not order_id:
        return

    # 3) Look up existing record id from the map (no full-table fetch per row)
    rec_id = None
    if existing_map is not None:
        rec_id = existing_map.get(order_id)
    else:
        # Build a minimal map once if none provided (slower path)
        existing_map = {}
        for r in get_all_lark_records(token):
            oid = (r.get("fields") or {}).get("Order ID")
            if oid:
                existing_map[oid] = r.get("record_id")
        rec_id = existing_map.get(order_id)

    # 4) CREATE if not found
    if not rec_id:
        res = requests.post(LARK_API_URL, json={"fields": fields_payload}, headers=headers)
        res.raise_for_status()
        rid = (res.json().get("data") or {}).get("record", {}).get("record_id")
        if rid:
            if existing_map is not None:
                existing_map[order_id] = rid
            print(f"CREATED {order_id}")
        else:
            print(f"CREATE FAIL {order_id}: {res.text}")
        return

    # 5) UPDATE only changed fields
    res_get = requests.get(f"{LARK_API_URL}/{rec_id}", headers=headers)
    res_get.raise_for_status()
    existing_fields = (res_get.json().get("data") or {}).get("record", {}).get("fields", {}) or {}

    def _norm(v):
        if isinstance(v, dict) and "text" in v: return v["text"]
        if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]: return v[0]["text"]
        return v

    changed = {k: v for k, v in fields_payload.items() if _norm(existing_fields.get(k)) != _norm(v)}
    if not changed:
        print(f"SKIP {order_id}")
        return

    res_upd = requests.put(f"{LARK_API_URL}/{rec_id}", json={"fields": changed}, headers=headers)
    res_upd.raise_for_status()
    print(f"UPDATED {order_id}: {', '.join(changed.keys())}")


def push_all_hungry_invoices_to_lark():
    from billing.models import PayoutHistoryForHungry
    from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry
    field_info = get_field_definitions()
    histories = PayoutHistoryForHungry.objects.select_related("restaurant", "location").prefetch_related("orders")

    for history in histories:
        orders = history.orders.all().order_by("created_date")
        if not orders.exists():
            continue

        restaurant = history.restaurant
        location = history.location
        _, obj = generate_excel_invoice_for_hungry(orders, restaurant, location, adjustments=history.adjustments, adjustments_note=history.adjustments_note, only_data=True)

        for data in obj[0]:
            if not data.get("order_date"):
                continue
            formatted = {
                    "Order ID": data["order_id"],
                    "Order Date": int(datetime.datetime.strptime(data["order_date"][:10], "%Y-%m-%d").timestamp() * 1000),
                    "Restaurant": validate_select_value("Restaurant", restaurant.name, field_info),
                    "Payment Type": data["payment_type"],
                    "Order Mode": validate_select_value("Order Mode", data["order_mode"], field_info),
                    "Original Item Price": float(data["actual_item_price"]),
                    "Item Price": float(data["item_price"]),
"BOGO Item Inflation Percentage": float(data["BOGO_item_inflation_percentage"]) / 100,
                    "Discount": float(data["discount"]),
                    "Hungrytiger Discount": float(data["hungrytiger_discount"]),
                     
                    "Restaurant Discount": float(data["restaurant_discount"]),
                    "Tax": float(data["tax"]),
                    "Selling price (inclusive of tax)": float(data["selling_price_inclusive_of_tax"]),
                    "Original Delivery Fees": float(data["original_delivery_fee"]),
                    "Customer absorb on delivery fees": float(data["customer_absorb_on_delivery_fees"]),
                    "Delivery fees expense": float(data["delivery_fees_expense"]),
                    "Commission Percentage": f'{float(data["commission_percentage"]):.1f}%',
                    "Commission Amount": float(data["commission_amount"]),
                    "Service fees to Restaurant": float(data["service_fees_to_restaurant"]),
                    "Service fee to Hungrytiger": float(data["service_fee_to_hungrytiger"]),
                    "Tips for restaurant": float(data["tips_for_restaurant"]),
                    "Bag fees": float(data["bag_fees"]),
                    "Container fees": float(data["container_fees"]),
                    "Amount to Restaurant": round(float(data["amount_to_restaurant"]), 2),
                }

            sync_ht_payout_record_to_lark(formatted, field_info)

from django.db.models import Q


def push_all_hungry_orders_direct():
    from billing.models import Order, Location
    from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry

    field_info = get_field_definitions()
    token = get_lark_token()

    # Build a quick map of Order ID → Lark record_id (to speed up matching)
    existing_map = {}
    for r in get_all_lark_records(token):
        oid = (r.get("fields") or {}).get("Order ID")
        if oid:
            existing_map[oid] = r.get("record_id")

    # ✅ Include only PENDING orders (do NOT exclude pending)
    #    Keep cancelled/rejected out.
    primary_query = (
        (Q(is_paid=True) | Q(payment_method=Order.PaymentMethod.CASH))
       
    )
    exclude_test_order = Q(customer__icontains="test")
    rejected_canceled_order = Q(status__iexact="cancelled") | Q(status__iexact="rejected")|Q(status__iexact="pending")

    base_qs = (
        Order.objects
        .filter(primary_query)
        .filter(restaurant__is_remote_Kitchen=True)
        .exclude(exclude_test_order)
        .exclude(rejected_canceled_order)
=======
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
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
        .select_related("restaurant", "location")
        .order_by("created_date")
    )

<<<<<<< HEAD
    total_eligible = base_qs.count()
    print(f"✅ Eligible orders to consider: {total_eligible}")

    # Loop by location to reuse your generator logic
    loc_ids = base_qs.values_list("location_id", flat=True).distinct()
    for loc in Location.objects.filter(id__in=loc_ids).select_related("restaurant"):
        rest = loc.restaurant
        orders = base_qs.filter(restaurant_id=rest.id, location_id=loc.id)

        # Map order_id -> Order so we can read restaurant from each order
        order_map = {str(o.order_id): o for o in orders}

        _, obj = generate_excel_invoice_for_hungry(
            orders=orders,
            restaurant=rest,
            location=loc,
            only_data=True
        )

        for data in obj[0]:
            date_str = str(data.get("order_date") or "").strip()
            if not date_str:
                continue

            order_id = str(data["order_id"])
            order_obj = order_map.get(order_id)

            # ✅ Restaurant name from the Order; fallback to loop restaurant; final fallback string
            if order_obj and order_obj.restaurant and order_obj.restaurant.name:
                restaurant_name = order_obj.restaurant.name
            elif rest and getattr(rest, "name", None):
                restaurant_name = rest.name
            else:
                restaurant_name = "Unknown Restaurant"

            formatted = {
                "Order ID": order_id,
                "Order Date": int(__import__("datetime").datetime.strptime(date_str[:10], "%Y-%m-%d").timestamp() * 1000),
                "Restaurant": validate_select_value("Restaurant", restaurant_name, field_info),
                "Payment Type": data["payment_type"],
                "Order Mode": validate_select_value("Order Mode", data["order_mode"], field_info),

                "Original Item Price": float(data["actual_item_price"]),
                "Item Price": float(data["item_price"]),
                "BOGO Item Inflation Percentage": float(data["BOGO_item_inflation_percentage"]) / 100.0,
                "Discount": float(data["discount"]),
                "Restaurant Discount": float(data["restaurant_discount"]),
                "Hungrytiger Discount": float(data["hungrytiger_discount"]),

                "Tax": float(data["tax"]),
                "Selling price (inclusive of tax)": float(data["selling_price_inclusive_of_tax"]),
                "Original Delivery Fees": float(data["original_delivery_fee"]),
                "Customer absorb on delivery fees": float(data["customer_absorb_on_delivery_fees"]),
                "Delivery fees expense": float(data["delivery_fees_expense"]),

                "Commission Percentage": f'{float(data["commission_percentage"]):.1f}%',
                "Commission Amount": float(data["commission_amount"]),
                "Service fees to Restaurant": float(data["service_fees_to_restaurant"]),
                "Service fee to Hungrytiger": float(data["service_fee_to_hungrytiger"]),
                "Tips for restaurant": float(data["tips_for_restaurant"]),
                "Bag fees": float(data["bag_fees"]),
                "Container fees": float(data["container_fees"]),
                "Amount to Restaurant": round(float(data["amount_to_restaurant"]), 2),
            }

            # Efficient update/create
            sync_ht_payout_record_to_lark(
                formatted, field_info, token=token, existing_map=existing_map
            )
=======
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
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
