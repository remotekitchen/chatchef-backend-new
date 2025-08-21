import json
import time
import dateutil.parser
import requests
from requests.adapters import HTTPAdapter, Retry

# === CONFIG (logic unchanged) ===
TEST_MODE = False
DJANGO_API_URL = "https://api.hungrytiger.chatchefs.com/api/lark/v1/customer-orders/"
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"

LARK_BASE_ID = "Ms7dbtQTfaew87s3OHfuRGRZsze"
LARK_TABLE_ID = "tblrssqND91wnmdC"

LARK_API_ROOT = "https://open.larksuite.com/open-apis"
TIMEOUT = (10, 60)  # (connect, read)

# --- HTTP session with retries/backoff & pooling ---
SESSION = requests.Session()
retries = Retry(
    total=6, connect=6, read=6,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"]),
    respect_retry_after_header=True,
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)


def _request(method, url, **kw):
    kw.setdefault("timeout", TIMEOUT)
    # a tiny extra retry against transient TLS/connection hiccups
    for i in range(2):
        try:
            return SESSION.request(method, url, **kw)
        except requests.RequestException:
            time.sleep(0.6 * (i + 1))
    return SESSION.request(method, url, **kw)


def get_lark_token():
    url = f"{LARK_API_ROOT}/auth/v3/tenant_access_token/internal/"
    res = _request("POST", url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    print("🔐 Lark token obtained")
    return res.json().get("tenant_access_token")


def clean_fields(customer):
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


def fetch_api_customers():
    res = _request("GET", DJANGO_API_URL)
    res.raise_for_status()
    return res.json()


def _bitable_records_url():
    return f"{LARK_API_ROOT}/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"


def fetch_lark_records(token):
    url = _bitable_records_url()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": 500}
    all_records = []
    page_token = None

    while True:
        if page_token:
            params["page_token"] = page_token
        res = _request("GET", url, headers=headers, params=params)
        res.raise_for_status()
        data = res.json().get("data", {})
        items = data.get("items") or []
        all_records.extend(items)
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")

    phone_to_record = {}
    duplicates = []

    for rec in all_records:
        fields = rec.get("fields", {}) or {}
        phone_val = fields.get("Phone")
        if not phone_val or phone_val in ["None", None, ""]:
            continue
        try:
            phone_key = int(str(phone_val).replace("+", "").replace(" ", "").strip())
            if phone_key in phone_to_record:
                duplicates.append(rec.get("record_id"))
            else:
                phone_to_record[phone_key] = rec
        except Exception:
            continue

    return phone_to_record, [d for d in duplicates if d]


def _diff(existing_fields, new_fields):
    # keep your exact compare semantics: stringified + strip
    changed = {}
    for k, v in new_fields.items():
        if str(existing_fields.get(k, "")).strip() != str(v).strip():
            changed[k] = v
    return changed


def _format_field_diff(existing_fields, changed_fields):
    diffs = []
    for k, new_val in changed_fields.items():
        old_val = existing_fields.get(k, "<missing>")
        diffs.append(f"- {k}:\n    before → {json.dumps(old_val)}\n    after  → {json.dumps(new_val)}")
    return "\n".join(diffs)


def sync_customers():
    token = get_lark_token()
    phone_to_record, duplicates = fetch_lark_records(token)
    headers = {"Authorization": f"Bearer {token}"}

    # delete duplicates (same logic, but safer logs)
    for dup_id in duplicates:
        del_url = f"{_bitable_records_url()}/{dup_id}"
        res = _request("DELETE", del_url, headers=headers)
        if res.status_code not in (200, 204):
            print(f"[WARN] Duplicate delete failed {dup_id}: {res.status_code} → {res.text}")

    # rebuild after deletes
    phone_to_record, _ = fetch_lark_records(token)

    customers = fetch_api_customers()
    if TEST_MODE:
        customers = customers[:10]

    base_url = _bitable_records_url()

    for customer in customers:
        fields = clean_fields(customer)
        phone = fields.get("Phone")
        print(f"\n Syncing: {fields['Full Name']} | {phone}")

        record = phone_to_record.get(phone)
        if record:
            record_id = record.get("record_id")
            existing_fields = record.get("fields", {}) or {}
            changed_fields = _diff(existing_fields, fields)

            if not changed_fields:
                print("[SUCCESS] No changes — record up-to-date.")
                continue

            print(f"[SUCCESS] Updating fields:\n{_format_field_diff(existing_fields, changed_fields)}")
            put_url = f"{base_url}/{record_id}"
            res = _request("PUT", put_url, headers=headers, json={"fields": changed_fields})
            if res.status_code in (200, 201):
                print("[SUCCESS] Successfully updated.")
            else:
                print(f"[ERROR] Update failed: {res.status_code} → {res.text}")

        else:
            res = _request("POST", base_url, headers=headers, json={"fields": fields})
            if res.status_code in (200, 201):
                print(f"[SUCCESS] Created record: {phone}")
            else:
                print(f"[ERROR] Create failed: {res.status_code} → {res.text}")


if __name__ == "__main__":
    sync_customers()
