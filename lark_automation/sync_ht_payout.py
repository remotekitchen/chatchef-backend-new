
import requests
import uuid
from dateutil import parser

# === CONFIG ===
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblmm5GcSsbPyI5i"
LARK_TABLE_ID = "tblBmPEwSywTlDBu"

LARK_API_URL = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"

# === AUTH ===
def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    print("🔐 Lark token obtained")
    return res.json().get("tenant_access_token")

# === HELPER: Get all records from Lark ===
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

# === HELPER: Fetch Lark field types and options ===
def get_field_definitions():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    fields = res.json().get("data", {}).get("items", [])
    field_info = {}

    for f in fields:
        if f["type"] == 3:  # SingleSelect
            options = [opt["name"] for opt in f["property"].get("options", [])]
        else:
            options = None
        field_info[f["field_name"]] = {
            "type": f["type"],
            "options": options
        }
    return field_info

# === HELPER: Validate select field values ===
def validate_select_value(field_name, value, field_info):
    options = field_info.get(field_name, {}).get("options", [])
    return value if value in options else (options[0] if options else "")

# === SYNC FUNCTION ===
def sync_ht_payout_record_to_lark(record, field_info):
    token = get_lark_token()
    order_id = record.get("Order ID")

    # 🔍 Replace remote filter with local match
    print(f"🔍 Searching locally for Order ID: {order_id}")
    all_records = get_all_lark_records(token)
    existing_records = [r for r in all_records if r.get("fields", {}).get("Order ID") == order_id]
    print(f"📊 Found {len(existing_records)} matching record(s)")

    headers = {"Authorization": f"Bearer {token}"}

    # ✅ Build payload
    fields_payload = {}
    for key, val in record.items():
        field_meta = field_info.get(key)
        if not field_meta:
            fields_payload[key] = val
            continue

        if field_meta["type"] == 3:
            valid_options = field_meta["options"]
            if isinstance(val, dict) and val.get("text") in valid_options:
                fields_payload[key] = val
            elif isinstance(val, str) and val in valid_options:
                fields_payload[key] = val
            else:
                continue
        else:
            fields_payload[key] = val

    if len(existing_records) == 0:
        # 🔨 Create
        res = requests.post(LARK_API_URL, json={"fields": fields_payload}, headers=headers)
        print(f"🆕 Created record: {order_id}")

    elif len(existing_records) == 1:
        # 🔁 Update if changed
        rec = existing_records[0]
        rec_id = rec["record_id"]
        existing_fields = rec.get("fields", {})
        changed_fields = {k: v for k, v in fields_payload.items() if existing_fields.get(k) != v}

        if changed_fields:
            update_url = f"{LARK_API_URL}/{rec_id}"
            res = requests.put(update_url, json={"fields": changed_fields}, headers=headers)
            changed_list = ', '.join(changed_fields.keys())
            print(f"🔁 Updated record {order_id} — fields changed: {changed_list}")
        else:
            print(f"✅ Record {order_id} is up-to-date.")

    else:
        # ⚠️ Handle duplicates
        print(f"⚠️ Duplicate records found for {order_id}")
        rec_to_keep = existing_records[0]
        rec_id = rec_to_keep["record_id"]
        existing_fields = rec_to_keep.get("fields", {})
        changed_fields = {k: v for k, v in fields_payload.items() if existing_fields.get(k) != v}

        if changed_fields:
            update_url = f"{LARK_API_URL}/{rec_id}"
            res = requests.put(update_url, json={"fields": changed_fields}, headers=headers)
            changed_list = ', '.join(changed_fields.keys())
            print(f"🔁 Updated primary record {order_id} — fields changed: {changed_list}")
        else:
            print(f"✅ Primary record {order_id} is up-to-date.")

        for duplicate in existing_records[1:]:
            dup_id = duplicate.get("record_id")
            delete_url = f"{LARK_API_URL}/{dup_id}"
            res = requests.delete(delete_url, headers=headers)
            print(f"🗑️ Deleted duplicate record: {order_id} (record_id={dup_id})")

    print(f"✅ Synced Order ID: {order_id}")

# === BULK SYNC FUNCTION ===
def push_hungry_invoices_to_lark_from_history(start_date, end_date):
    from billing.models import PayoutHistoryForHungry

    if isinstance(start_date, str):
        start_date = parser.parse(start_date)
    if isinstance(end_date, str):
        end_date = parser.parse(end_date)

    records = PayoutHistoryForHungry.objects.filter(
        statement_start_date=start_date,
        statement_end_date=end_date
    )

    field_info = get_field_definitions()

    for record in records:
        if not record.orders.exists():
            continue

        order = record.orders.first()
        created_date = order.created_date

        formatted = {
            "Order ID": str(order.order_id),
            "Order Date": int(created_date.timestamp() * 1000),
            "Restaurant": validate_select_value("Restaurant", record.restaurant.name, field_info),
            "Payment Type": order.payment_method,
            "Order Mode": validate_select_value("Order Mode", order.order_method, field_info),
            "Original Item Price": order.subtotal,
            "Item Price": order.total,
            "BOGO Item Inflation Percentage": 0,
            "Discount": record.discount,
            "Restaurant Discount": record.restaurant_discount,
            "Tax": record.tax,
            "Selling price (inclusive of tax)": record.selling_price_inclusive_of_tax,
            "Original Delivery Fees": record.original_delivery_fees,
            "Customer absorb on delivery fees": record.customer_absorbed_delivery_fees,
            "Delivery fees expense": record.delivery_fees,
            "Commission Percentage": f"{record.commission_percentage}%",
            "Commission Amount": record.commission_amount,
            "Service fees to Restaurant": record.service_fee_to_restaurant,
            "Service fee to Hungrytiger": record.service_fee_to_hungrytiger,
            "Tips for restaurant": record.tips_for_restaurant,
            "Bag fees": record.bag_fees,
            "Container fees": record.container_fees,
            "Amount to Restaurant": record.amount_to_restaurant,
        }

        for k, v in formatted.items():
            if isinstance(v, uuid.UUID):
                formatted[k] = str(v)

        print("📦 Syncing record:", formatted["Order ID"])
        sync_ht_payout_record_to_lark(formatted, field_info)

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




# def fetch_all_lark_records():
#     token = get_lark_token()
#     url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
#     headers = {"Authorization": f"Bearer {token}"}

#     all_records = []
#     has_more = True
#     page_token = None

#     while has_more:
#         params = {}
#         if page_token:
#             params["page_token"] = page_token

#         res = requests.get(url, headers=headers, params=params)
#         res.raise_for_status()
#         data = res.json().get("data", {})

#         items = data.get("items", [])
#         all_records.extend(items)

#         has_more = data.get("has_more", False)
#         page_token = data.get("page_token")

#     print(f"\n📦 Total records fetched from Lark: {len(all_records)}\n")

#     for i, rec in enumerate(all_records, start=1):
#         fields = rec.get("fields", {})
#         order_id = fields.get("Order ID", "<no id>")
#         order_date = fields.get("Order Date", "<no date>")
#         print(f"{i}. Order ID: {order_id} | Order Date: {order_date}")