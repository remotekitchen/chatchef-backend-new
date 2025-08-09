import requests
import uuid
from dateutil import parser
import datetime
import traceback
from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry

LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblbJMfZtZxPL78Q"

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
    return {"text": value} if value in options else ({"text": options[0]} if options else None)

def sync_ht_payout_record_to_lark(record, field_info):
    token = get_lark_token()
    order_id = record.get("Order ID")
    all_records = get_all_lark_records(token)
    existing_records = [r for r in all_records if r.get("fields", {}).get("Order ID") == order_id]
    headers = {"Authorization": f"Bearer {token}"}

    fields_payload = {}
    for key, val in record.items():
        field_meta = field_info.get(key)
        if not field_meta:
            fields_payload[key] = val
            continue

        if field_meta["type"] == 3:
            valid_options = field_meta["options"]
            if isinstance(val, dict) and val.get("text") in valid_options:
                fields_payload[key] = val["text"]
            elif isinstance(val, str) and val in valid_options:
                fields_payload[key] = val
            else:
                continue
        else:
            fields_payload[key] = val

    if not existing_records:
        try:
            print(f"\n🧪 Validating data types for Order ID {order_id}:")
            for k, v in fields_payload.items():
                print(f"  {k}: {v} ({type(v)})")

            res = requests.post(LARK_API_URL, json={"fields": fields_payload}, headers=headers)
            res.raise_for_status()
            response_data = res.json()
            record_info = response_data.get("data", {}).get("record")
            record_id = record_info.get("record_id") if record_info else None
            if not record_id:
                print("❗ Lark did not return a valid record_id. Full response:")
                print(response_data)


            print(f"✅ Created record: {order_id} (record_id={record_id})" if record_id else f"❌ Failed to retrieve record_id for {order_id}")

        except Exception as e:
            print(f"❌ Lark error while creating Order ID {order_id}: {e}")
            print("📤 Payload:")
            for k, v in fields_payload.items():
                print(f"  - {k}: {v}")
            print("📥 Lark response:")
            if 'res' in locals():
                print(res.status_code, res.text)
            import traceback
            traceback.print_exc()
    elif len(existing_records) == 1:
        rec_id = existing_records[0]["record_id"]
        existing_fields = existing_records[0].get("fields", {})
        changed_fields = {k: v for k, v in fields_payload.items() if normalize_lark_value(existing_fields.get(k)) != normalize_lark_value(v)}
        if changed_fields:
            res = requests.put(f"{LARK_API_URL}/{rec_id}", json={"fields": changed_fields}, headers=headers)
            print(f"🔁 Updated record {order_id}: {', '.join(changed_fields.keys())}")
        else:
            print(f"✅ Record {order_id} is up-to-date.")
    else:
        print(f"⚠️ Duplicate records for {order_id}")

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