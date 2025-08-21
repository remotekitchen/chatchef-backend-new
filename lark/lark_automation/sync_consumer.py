import os
import json
import dateutil.parser
import requests
from requests.adapters import HTTPAdapter, Retry

# === CONFIG (logic unchanged) ===
TEST_MODE = False
DJANGO_API_URL = "http://127.0.0.1:8000/api/lark/v1/customer-orders/"
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"

LARK_BASE_ID = "Ms7dbtQTfaew87s3OHfuRGRZsze"
LARK_TABLE_ID = "tblrssqND91wnmdC"

LARK_API_ROOT = "https://open.larksuite.com/open-apis"
TIMEOUT = 30  # seconds

# --- HTTP session with retries/backoff (no logic change to API calls) ---
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()


def get_lark_token() -> str:
    url = f"{LARK_API_ROOT}/auth/v3/tenant_access_token/internal/"
    res = SESSION.post(
        url,
        json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
        timeout=TIMEOUT,
    )
    res.raise_for_status()
    print("🔐 Lark token obtained")
    data = res.json()
    return data.get("tenant_access_token")


def clean_fields(customer: dict) -> dict:
    # --- keep original behavior exactly ---
    def safe_int(val):
        try:
            return int(val)
        except:
            return 0

    def clean_phone(val):
        try:
            return int(str(val).replace("+", "").replace(".", "").replace(" ", "").strip() or "0")
        except:
            return 0

    def format_date(val):
        if not val:
            return None
        try:
            if isinstance(val, (int, float)):
                return int(val)
            dt = dateutil.parser.parse(val)
            return int(dt.timestamp() * 1000)
        except Exception as e:
            print("[Error] Date parse error:", val, e)
            return None

    return {
        "Full Name": (customer.get("full_name", "")).strip(),
        "Email": (customer.get("email", "")).strip(),
        "Phone": clean_phone(customer.get("phone")),
        "Total Orders": safe_int(customer.get("total_orders")),
        "Date Joined": format_date(customer.get("date_joined")),
        "First Order Date": format_date(customer.get("first_order_date")),
        "Last Order Date": format_date(customer.get("last_order_date")),
    }


def fetch_api_customers() -> list:
    res = SESSION.get(DJANGO_API_URL, timeout=TIMEOUT)
    res.raise_for_status()
    return res.json()


def bitable_records_url(base_id: str, table_id: str) -> str:
    return f"{LARK_API_ROOT}/bitable/v1/apps/{base_id}/tables/{table_id}/records"


def fetch_lark_records(token: str):
    """Fetch all records and return (phone_to_record, duplicates) as in original code."""
    url = bitable_records_url(LARK_BASE_ID, LARK_TABLE_ID)
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": 100}
    all_records = []

    while True:
        res = SESSION.get(url, headers=headers, params=params, timeout=TIMEOUT)
        res.raise_for_status()
        data = res.json()
        items = data.get("data", {}).get("items", []) or []
        all_records.extend(items)
        page_token = data.get("data", {}).get("page_token")
        if not page_token:
            break
        params["page_token"] = page_token

    phone_to_record = {}
    duplicates = []

    for rec in all_records:
        fields = rec.get("fields", {})
        phone_val = fields.get("Phone")
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


def format_field_diff(existing_fields: dict, changed_fields: dict) -> str:
    diffs = []
    for k, new_val in changed_fields.items():
        old_val = existing_fields.get(k, "<missing>")
        diffs.append(f"- {k}:\n    before → {json.dumps(old_val)}\n    after  → {json.dumps(new_val)}")
    return "\n".join(diffs)


def fields_diff(existing_fields: dict, new_fields: dict) -> dict:
    """Keep exact compare semantics: stringified + strip."""
    changed = {}
    for k, v in new_fields.items():
        if str(existing_fields.get(k, "")).strip() != str(v).strip():
            changed[k] = v
    return changed


def sync_customers():
    token = get_lark_token()
    phone_to_record, duplicates = fetch_lark_records(token)
    headers = {"Authorization": f"Bearer {token}"}

    # Delete duplicates (same behavior, but with error checks)
    for dup_id in duplicates:
        del_url = f"{bitable_records_url(LARK_BASE_ID, LARK_TABLE_ID)}/{dup_id}"
        try:
            res = SESSION.delete(del_url, headers=headers, timeout=TIMEOUT)
            if res.status_code not in (200, 204):
                print(f"[WARN] Duplicate delete failed {dup_id}: {res.status_code} → {res.text}")
        except requests.RequestException as e:
            print(f"[WARN] Duplicate delete error {dup_id}: {e}")

    # Rebuild map after deletions (same behavior)
    phone_to_record, _ = fetch_lark_records(token)

    # Fetch customers
    customers = fetch_api_customers()
    if TEST_MODE:
        customers = customers[:10]

    base_url = bitable_records_url(LARK_BASE_ID, LARK_TABLE_ID)

    for customer in customers:
        fields = clean_fields(customer)
        phone = fields.get("Phone")
        full_name = fields.get("Full Name", "")
        print(f"\n Syncing: {full_name} | {phone}")

        record = phone_to_record.get(phone)
        if record:
            record_id = record.get("record_id")
            existing_fields = record.get("fields", {}) or {}
            changed_fields = fields_diff(existing_fields, fields)

            if not changed_fields:
                print("[SUCCESS] No changes — record up-to-date.")
                continue

            print(f"[SUCCESS] Updating fields:\n{format_field_diff(existing_fields, changed_fields)}")

            put_url = f"{base_url}/{record_id}"
            res = SESSION.put(put_url, headers=headers, json={"fields": changed_fields}, timeout=TIMEOUT)
            if res.status_code in (200, 201):
                print("[SUCCESS] Successfully updated.")
            else:
                print(f"[ERROR] Update failed: {res.status_code} → {res.text}")

        else:
            # Create
            res = SESSION.post(base_url, headers=headers, json={"fields": fields}, timeout=TIMEOUT)
            if res.status_code in (200, 201):
                print(f"[SUCCESS] Created record: {phone}")
            else:
                print(f"[ERROR] Create failed: {res.status_code} → {res.text}")


if __name__ == "__main__":
    sync_customers()
