<<<<<<< HEAD
# DO Lark sync — optimized for 20k+ rows (batch create/update/delete, single snapshot, resilient HTTP)

import os
import json
import time
import datetime as dt
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import SSLError, ConnectionError, ReadTimeout, ChunkedEncodingError

from django.db.models import Q
from billing.models import Order, PayoutHistory
from billing.utilities.generate_invoice import generate_excel_invoice  # DO logic
from billing.models import Location
# === CONFIG ===
LARK_APP_ID     = os.getenv("LARK_APP_ID", "cli_a8030393dd799010")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET", "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY")
LARK_BASE_ID    = os.getenv("LARK_DO_BASE_ID", "OGP4b0T04a2QmesErNsuSkRTs4P")
LARK_TABLE_ID   = os.getenv("LARK_DO_TABLE_ID", "tblIXtMH8WhFDQ9Z")

API_BASE = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}"
API_REC  = f"{API_BASE}/records"

# Safe batch sizes
BATCH_CREATE = 200
BATCH_UPDATE = 200
BATCH_DELETE = 200

# === Resilient HTTP (no extra installs needed) ===
HTTP = requests.Session()
HTTP.headers.update({"Content-Type": "application/json; charset=utf-8", "Connection": "close"})

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
    return HTTP.request(method, url, **kw)

# === Helpers ===
def _norm_id(v) -> str:
    return "" if v is None else str(v).strip()

def _norm_text(v):
    if isinstance(v, dict) and "text" in v: return v["text"]
    if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]: return v[0]["text"]
    if isinstance(v, (int, float)): return str(v)
    if isinstance(v, str): return v.strip()
    return v

def _chunked(xs: List, n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

# === Auth & metadata ===
def get_lark_token() -> str:
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = _request("POST", url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    tok = res.json().get("tenant_access_token")
    if not tok:
        raise RuntimeError(f"Failed to get tenant_access_token: {res.text}")
    return tok

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
            opts = [o.get("name") or o.get("text") for o in (prop.get("options") or []) if (o.get("name") or o.get("text"))]
        info[f.get("field_name")] = {"type": ftype, "options": opts}
    return info

# === Table snapshot once (no per-row GET) ===
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
    """
    Returns:
      by_order_id: {order_id -> record_id}
      fields_by_rec: {record_id -> fields_dict}
    """
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

# === Select validation & type coercion ===
def validate_single_select(field_name: str, value: str, field_info: Dict) -> str:
    meta = field_info.get(field_name) or {}
    opts = meta.get("options") or []
    if not opts:
        return _norm_id(value)
    v = _norm_id(value)
    return v if v in opts else (opts[0] if opts else v)
=======

import requests
import uuid
from dateutil import parser
from billing.models import PayoutHistory
from billing.models import Order
from dateutil import parser
from pytz import timezone as pytz_timezone
from billing.utilities.generate_invoice import generate_excel_invoice  # DO logic
from billing.models import PayoutHistory
import datetime

# from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry


# === CONFIG ===
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblYJICxzIo7ZzLz"
LARK_API_URL = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"

# === AUTH ===
# === AUTH ===
def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    # Keep logs minimal: do not print token messages
    return res.json().get("tenant_access_token")

# === HELPERS ===
def get_all_lark_records(token):
    headers = {"Authorization": f"Bearer {token}"}
    all_records, page_token = [], None
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


def get_field_definitions():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    fields = res.json().get("data", {}).get("items", [])
    field_info = {}
    for f in fields:
        # type=2 Number, type=3 SingleSelect, type=5 Date
        options = [opt["name"] for opt in f.get("property", {}).get("options", [])] if f["type"] == 3 else None
        field_info[f["field_name"]] = {"type": f["type"], "options": options}
    return field_info


def validate_single_select(field_name, value, field_info):
    meta = field_info.get(field_name, {})
    opts = meta.get("options") or []
    if not opts:
        return None  # no options configured in Lark -> skip this field
    return value if value in opts else opts[0]

>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab

def _strip_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}

<<<<<<< HEAD
def _coerce_by_field_types(payload: dict, field_info: dict) -> dict:
=======

def _normalize_for_compare(v):
    # Lark returns numbers as strings; compare using string form
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, dict) and "text" in v:
        return v["text"]
    return str(v)


def _coerce_by_field_types(payload: dict, field_info: dict) -> dict:
    """Coerce values based on Lark field types to avoid NumberFieldConvFail etc."""
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
    fixed = {}
    for k, v in payload.items():
        meta = field_info.get(k)
        if not meta:
<<<<<<< HEAD
            continue
        t = meta.get("type")
        try:
            if t == 2:  # Number
                if v in (None, ""): continue
                fixed[k] = float(v)
            elif t == 5:  # Date (ms epoch int)
                if v in (None, ""): continue
                if isinstance(v, int):
                    fixed[k] = v
                elif isinstance(v, str):
                    ts = int(dt.datetime.strptime(v[:10], "%Y-%m-%d").timestamp() * 1000)
                    fixed[k] = ts
            elif t == 3:  # SingleSelect expects string
                if isinstance(v, str): fixed[k] = v
            else:
=======
            continue  # field not present in table
        t = meta.get("type")
        try:
            if t == 2:  # Number
                if v in (None, ""):
                    continue
                fixed[k] = float(v)
            elif t == 5:  # Date (expects ms epoch int)
                if v in (None, ""):
                    continue
                if isinstance(v, int):
                    fixed[k] = v
                elif isinstance(v, str):
                    ts = int(datetime.datetime.strptime(v[:10], "%Y-%m-%d").timestamp() * 1000)
                    fixed[k] = ts
                else:
                    continue
            elif t == 3:  # SingleSelect expects string option
                if isinstance(v, str):
                    fixed[k] = v
            else:
                # Text or others -> pass through
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
                fixed[k] = v
        except Exception as e:
            print(f"! Coercion failed for '{k}' value '{v}': {e}")
    return fixed

<<<<<<< HEAD
# === Batch endpoints ===
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

# === Row builder: backend -> Lark fields (DO schema) ===
def _format_row(row: Dict, field_info: Dict, restaurant_name: str) -> Dict:
    # generator returns 'date' field
    date_str = _norm_id(row.get("date"))
    ts_ms = None
    if date_str:
        try:
            ts_ms = int(dt.datetime.strptime(date_str[:10], "%Y-%m-%d").timestamp() * 1000)
        except Exception:
            ts_ms = None

    payload = {
        "Order ID": _norm_id(row.get("order_id")),
        "Order Date": ts_ms,
        "Restaurant": restaurant_name,
        "Payment Type": _norm_id(row.get("payment_type", "")),
        "Order Mode": validate_single_select("Order Mode", _norm_id(row.get("order_mode", "")), field_info),
        "Item Price": row.get("item_price", 0),
        "Discount": row.get("discount", 0),
        "Tax": row.get("tax", 0),
        "Selling price (inclusive of tax)": row.get("selling_price", 0),
        "Original Delivery Fees": row.get("Original_Delivery_Fees", 0),
        "Customer absorb on delivery fees": row.get("Customer_absorb_on_delivery_fees", 0),
        "Delivery fees expense": row.get("Delivery_fees_expense", 0),
        "Stripe Fees": row.get("stripe_fees", 0),
        "Service fees to restaurant": row.get("service_fees_to_restaurant", 0),
        "Service fee to chatchefs": row.get("service_fees_to_chatchefs", 0),
        "Tips for restaurant": row.get("tips_for_restaurant", 0),
        "Bag fees": row.get("bag_fees", 0),
        "Utensil fees": row.get("utensil_fees", 0),
        "Refund Amount": row.get("refund_amount", 0),
        "Sub-Total Payment": row.get("sub_total", 0),
    }
    payload = _strip_none(payload)
    payload = _coerce_by_field_types(payload, field_info)
    # send only columns that exist
    return {k: v for k, v in payload.items() if k in field_info}

def _diff_fields(existing_fields: Dict, new_fields: Dict) -> Dict:
    changed = {}
    for k, v in new_fields.items():
        if _norm_text(existing_fields.get(k)) != _norm_text(v):
            changed[k] = v
    return changed

# === MAIN ENTRYPOINT (called by your webhook) ===
def push_all_DO_invoices_to_lark(allow_delete_orphans: bool = True):
    token = get_lark_token()
    field_info = get_field_definitions(token)

    # Build backend rows once (group by location/restaurant)
    primary_q   = Q(is_paid=True) | Q(payment_method=Order.PaymentMethod.CASH)
    completed_q = Q(status__iexact=getattr(getattr(Order, "Status", None), "COMPLETED", "completed"))
    exclude_q   = Q(customer__icontains="test")

    base_qs = (
        Order.objects
        .filter(primary_q)
        .filter(restaurant__is_remote_Kitchen=False)     # DO side
        .filter(completed_q)
        .exclude(exclude_q)
        .select_related("restaurant", "location")
        .order_by("created_date")
    )

    # Use your existing generator per (restaurant, location)
    backend_rows: Dict[str, Dict] = {}  # {order_id -> fields}
    loc_ids = base_qs.values_list("location_id", flat=True).distinct()
    for loc in Location.objects.filter(id__in=loc_ids).select_related("restaurant"):
        rest = loc.restaurant
        orders = list(base_qs.filter(restaurant_id=rest.id, location_id=loc.id))

        _, obj = generate_excel_invoice(
            orders=orders,
            restaurant=rest,
            location=loc,
            only_data=True
        )

        for row in obj[0]:
            oid = _norm_id(row.get("order_id"))
            if not oid:
                continue

            # build payload, coerce types, etc. (unchanged)
            built = _format_row(row, field_info, getattr(rest, "name", ""))
            if built:
                backend_rows[oid] = built


    print(f"Prepared backend rows: {len(backend_rows)}")

    # Snapshot Lark once; make index
    existing = fetch_all_records(token)
    by_order_id, fields_by_rec = build_existing_index(existing)

    backend_ids  = set(backend_rows.keys())
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

    # Execute in batches (robust)
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
=======

def sync_Do_record_to_lark(record: dict, field_info: dict):
    token = get_lark_token()
    order_id = record.get("Order ID")
    headers = {"Authorization": f"Bearer {token}"}

    # Build payload respecting field types; skip fields unknown to table
    fields_payload, skipped = {}, []
    for key, val in record.items():
        meta = field_info.get(key)
        if not meta:
            skipped.append((key, "field_not_found"))
            continue
        if meta["type"] == 3:  # SingleSelect
            opts = meta.get("options") or []
            if not opts:
                skipped.append((key, "no_options"))
                continue
            if isinstance(val, str) and val in opts:
                fields_payload[key] = val
            elif isinstance(val, str):
                fields_payload[key] = opts[0]
            else:
                skipped.append((key, f"invalid_value_type:{type(val).__name__}"))
        else:
            fields_payload[key] = val

    fields_payload = _strip_none(fields_payload)
    fields_payload = _coerce_by_field_types(fields_payload, field_info)

    # Find existing record by Order ID
    all_records = get_all_lark_records(token)
    existing = [r for r in all_records if r.get("fields", {}).get("Order ID") == order_id]

    try:
        if not existing:
            # CREATE
            res = requests.post(LARK_API_URL, json={"fields": fields_payload}, headers=headers)
            if not (200 <= res.status_code < 300):
                print(f"CREATE FAIL | order_id={order_id} | status={res.status_code} | resp={res.text}")
                print(f"PAYLOAD: {fields_payload}")
                return
            rid = (res.json().get("data") or {}).get("record", {}).get("record_id")
            if rid:
                print(f"CREATED  | order_id={order_id} | record_id={rid}")
            else:
                print(f"CREATE FAIL | order_id={order_id} | missing record_id | resp={res.text}")
                print(f"PAYLOAD: {fields_payload}")
        elif len(existing) == 1:
            rec = existing[0]
            rec_id = rec["record_id"]
            existing_fields = rec.get("fields", {})
            changed = {k: v for k, v in fields_payload.items() if _normalize_for_compare(existing_fields.get(k)) != _normalize_for_compare(v)}
            if not changed:
                print(f"SKIP     | order_id={order_id} | up-to-date")
                return
            res = requests.put(f"{LARK_API_URL}/{rec_id}", json={"fields": changed}, headers=headers)
            if not (200 <= res.status_code < 300):
                print(f"UPDATE FAIL | order_id={order_id} | status={res.status_code} | resp={res.text}")
                print(f"CHANGED: {changed}")
                return
            print(f"UPDATED  | order_id={order_id} | fields={', '.join(changed.keys())}")
        else:
            print(f"DUPLICATE | order_id={order_id} | count={len(existing)} | skipped")
    except Exception as e:
        print(f"ERROR    | order_id={order_id} | {e}")
        print(f"PAYLOAD: {fields_payload}")


# === BULK SYNC ===
def push_DO_invoices_to_lark():
    field_info = get_field_definitions()
    histories = PayoutHistory.objects.select_related("restaurant", "location").prefetch_related("orders")
    print(f"TOTAL PAYOUTS: {histories.count()}")

    for history in histories:
        orders = history.orders.all().order_by("created_date")
        if not orders.exists():
            continue

        _, obj = generate_excel_invoice(
            orders=orders,
            restaurant=history.restaurant,
            location=history.location,
            adjustments=history.adjustments,
            adjustments_note=history.adjustments_note,
            only_data=True,
        )

        for row in obj[0]:
            order_id = row.get("order_id")
            if not order_id:
                continue
            date_str = row.get("date")  # your generator uses 'date'
            order_ts = None
            if date_str:
                try:
                    order_ts = int(datetime.datetime.strptime(date_str[:10], "%Y-%m-%d").timestamp() * 1000)
                except Exception:
                    pass

            payload = {
                "Order ID": order_id,
                "Order Date": order_ts,
                "Payment Type": row.get("payment_type", ""),
                "Order Mode": validate_single_select("Order Mode", row.get("order_mode", ""), field_info),
                "Item Price": row.get("item_price", 0),
                "Discount": row.get("discount", 0),
                "Tax": row.get("tax", 0),
                "Selling price (inclusive of tax)": row.get("selling_price", 0),
                "Original Delivery Fees": row.get("Original_Delivery_Fees", 0),
                "Customer absorb on delivery fees": row.get("Customer_absorb_on_delivery_fees", 0),
                "Delivery fees expense": row.get("Delivery_fees_expense", 0),
                "Stripe Fees": row.get("stripe_fees", 0),
                "Service fees to restaurant": row.get("service_fees_to_restaurant", 0),
                "Service fee to chatchefs": row.get("service_fees_to_chatchefs", 0),
                "Tips for restaurant": row.get("tips_for_restaurant", 0),
                "Bag fees": row.get("bag_fees", 0),
                "Utensil fees": row.get("utensil_fees", 0),
                "Refund Amount": row.get("refund_amount", 0),
                "Sub-Total Payment": row.get("sub_total", 0),
            }

            # Remove Nones, then coerce by field types (numbers to float, date to int ms, singleselect str)
            payload = _strip_none(payload)
            payload = _coerce_by_field_types(payload, field_info)

            sync_Do_record_to_lark(payload, field_info)


            

# === OPTIONAL DEBUG UTILITY ===
def print_lark_field_options():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()

    data = res.json()
    print("\n🧾 Lark Field Metadata (SingleSelect fields):\n")
    for field in data.get("data", {}).get("items", []):
        name = field["field_name"]
        ftype = field["type"]
        if ftype == 3:  # SingleSelect
            options = [opt["name"] for opt in field.get("property", {}).get("options", [])]
            print(f"- {name} (SingleSelect): {options}")




def fetch_all_lark_records():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}

    all_records = []
    has_more = True
    page_token = None

    while has_more:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token

        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        data = res.json().get("data", {})

        items = data.get("items", [])
        all_records.extend(items)

        has_more = data.get("has_more", False)
        page_token = data.get("page_token")

    print(f"\n📦 Total records fetched from Lark: {len(all_records)}")

    # 🧾 Print field keys for the first few records
    for i, rec in enumerate(all_records[:5], start=1):
        print(f"\n🔎 Record {i} — record_id: {rec.get('record_id')}")
        for k, v in rec.get("fields", {}).items():
            print(f"   📌 {k} : {v}")

    return all_records
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
