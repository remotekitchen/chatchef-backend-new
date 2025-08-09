
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


def _strip_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _normalize_for_compare(v):
    # Lark returns numbers as strings; compare using string form
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, dict) and "text" in v:
        return v["text"]
    return str(v)


def _coerce_by_field_types(payload: dict, field_info: dict) -> dict:
    """Coerce values based on Lark field types to avoid NumberFieldConvFail etc."""
    fixed = {}
    for k, v in payload.items():
        meta = field_info.get(k)
        if not meta:
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
                fixed[k] = v
        except Exception as e:
            print(f"! Coercion failed for '{k}' value '{v}': {e}")
    return fixed


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
