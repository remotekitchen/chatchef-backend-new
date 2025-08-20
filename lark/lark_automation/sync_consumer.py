import requests
import time
import dateutil.parser
import json
# === CONFIG ===
TEST_MODE = False
DJANGO_API_URL = "https://api.hungrytiger.chatchefs.com/api/billing/v1/customer-orders/"
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "Ms7dbtQTfaew87s3OHfuRGRZsze"

LARK_TABLE_ID = "tblrssqND91wnmdC"



def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    print("\U0001f510 Lark token obtained")
    return res.json().get("tenant_access_token")


def clean_fields(customer):
    def safe_int(val):
        try: return int(val)
        except: return 0

    def clean_phone(val):
        try:
            return int(str(val).replace("+", "").replace(".", "").replace(" ", "").strip() or "0")
        except:
            return 0

    def format_date(val):
        if not val: return None
        try:
            if isinstance(val, (int, float)): return int(val)
            dt = dateutil.parser.parse(val)
            return int(dt.timestamp() * 1000)
        except Exception as e:
            print("[Error] Date parse error:", val, e)
            return None

    return {
        "Full Name": customer.get("full_name", "").strip(),
        "Email": customer.get("email", "").strip(),
        "Phone": clean_phone(customer.get("phone")),
        "Total Orders": safe_int(customer.get("total_orders")),
        "Date Joined": format_date(customer.get("date_joined")),
        "First Order Date": format_date(customer.get("first_order_date")),
        "Last Order Date": format_date(customer.get("last_order_date")),
    }


def fetch_api_customers():
    res = requests.get(DJANGO_API_URL)
    res.raise_for_status()
    return res.json()


def fetch_lark_records(token):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": 100}
    all_records = []

    while True:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        data = res.json()
        all_records.extend(data.get("data", {}).get("items", []))
        page_token = data.get("data", {}).get("page_token")
        if not page_token:
            break
        params["page_token"] = page_token

    phone_to_record = {}
    duplicates = []

    for rec in all_records:
        phone_val = rec["fields"].get("Phone")
        if not phone_val or phone_val in ["None", None, ""]:
            continue
        try:
            phone_key = int(str(phone_val).replace("+", "").replace(" ", "").strip())
            if phone_key in phone_to_record:
                duplicates.append(rec["record_id"])
            else:
                phone_to_record[phone_key] = rec
        except:
            continue

    return phone_to_record, duplicates


def sync_customers():
    token = get_lark_token()
    phone_to_record, duplicates = fetch_lark_records(token)

    headers = {"Authorization": f"Bearer {token}"}

    for dup_id in duplicates:
        del_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records/{dup_id}"
        requests.delete(del_url, headers=headers)

    phone_to_record, _ = fetch_lark_records(token)
    customers = fetch_api_customers()
    if TEST_MODE:
        customers = customers[:10]

    for customer in customers:
        fields = clean_fields(customer)
        phone = fields.get("Phone")
        print(f"\n Syncing: {fields['Full Name']} | {phone}")

        record = phone_to_record.get(phone)
        if record:
            record_id = record.get("record_id")
            existing_fields = record.get("fields", {})
            changed_fields = {}

            for k, v in fields.items():
                if str(existing_fields.get(k, "")).strip() != str(v).strip():
                    changed_fields[k] = v

            if not changed_fields:
                print(f"[SUCCESS] No changes — record up-to-date.")
                continue

            # Show detailed diff
            def format_field_diff(existing_fields, changed_fields):
                diffs = []
                for k, new_val in changed_fields.items():
                    old_val = existing_fields.get(k, "<missing>")
                    diffs.append(f"- {k}:\n    before → {json.dumps(old_val)}\n    after  → {json.dumps(new_val)}")
                return "\n".join(diffs)

            print(f"[SUCCESS] Updating fields:\n{format_field_diff(existing_fields, changed_fields)}")

            put_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records/{record_id}"
            res = requests.put(put_url, headers=headers, json={"fields": changed_fields})

            if res.status_code in [200, 201]:
                print(f"[SUCCESS] Successfully updated.")
            else:
                print(f"[ERROR] Update failed: {res.status_code} → {res.text}")

        else:
            post_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
            res = requests.post(post_url, headers=headers, json={"fields": fields})
            if res.status_code in [200, 201]:
                print(f"[SUCCESS] Created record: {phone}")
            else:
                print(f"[ERROR] Create failed: {res.status_code} → {res.text}")



if __name__ == "__main__":
    sync_customers()
