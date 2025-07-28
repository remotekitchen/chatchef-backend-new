import requests
from datetime import datetime
import time

# === Test Mode Toggle ===
TEST_MODE = True  # Set to False to sync all customers

# === Lark Base Configuration ===
DJANGO_API_URL = "http://127.0.0.1:8000/api/billing/v1/customer-orders/"
LARK_APP_ID = "cli_a8d53aeb9038d010"
LARK_APP_SECRET = "9b0pQoLYJbnNJOdrHSJNAfD6iBeOUYSs"
LARK_BASE_ID = "Ms7dbtQTfaew87s3OHfuRGRZsze"
LARK_TABLE_ID = "tblrssqND91wnmdC"

# === Get Lark Access Token ===
def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={
        "app_id": LARK_APP_ID,
        "app_secret": LARK_APP_SECRET
    })
    res.raise_for_status()
    return res.json()["tenant_access_token"]

# === Format Dates to Lark Format ===
def format_date(date_value):
    if not date_value:
        return ""
    try:
        if isinstance(date_value, int):  # UNIX timestamp
            dt = datetime.fromtimestamp(date_value / 1000)
        elif isinstance(date_value, str):
            dt = datetime.fromisoformat(date_value.replace("Z", "+00:00")) if "T" in date_value else datetime.strptime(date_value, "%Y-%m-%d")
        else:
            return ""
        return dt.strftime("%Y/%m/%d %H:%M")
    except Exception as e:
        print(f"Date format error: {e} → Raw: {date_value}")
        return ""

# === Clean Field Mapping ===
def clean_fields(customer):
    return {
        "Full Name": customer.get("full_name", "").strip(),
        "Email": customer.get("email", "").strip(),
        "Phone": customer.get("phone", "").strip(),
        "Date Joined": format_date(customer.get("date_joined")),
        "First Order Date": format_date(customer.get("first_order_date")),
        "Last Order Date": format_date(customer.get("last_order_date")),
        "Total Orders": int(customer.get("total_orders", 0)),
    }

# === Fetch Customers from Django API ===
def fetch_api_customers():
    res = requests.get(DJANGO_API_URL)
    res.raise_for_status()
    return res.json()

# === Fetch Records from Lark with Retry Limit ===
def fetch_lark_records(token, max_retries=5):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    all_records = []
    page_token = None
    retry_count = 0

    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token

        res = requests.get(url, headers=headers, params=params)

        if res.status_code == 429:
            if retry_count >= max_retries:
                print(" Max retry limit reached. Exiting fetch.")
                break
            retry_count += 1
            print(f"⚠️ Rate limited. Retrying in 5s... [{retry_count}/{max_retries}]")
            time.sleep(20)
            continue

        res.raise_for_status()
        data = res.json()
        all_records.extend(data["data"]["items"])

        if not data["data"].get("has_more"):
            break
        page_token = data["data"]["page_token"]

    return all_records

# === Sync Logic ===
def sync_customers():
    token = get_lark_token()
    print("Lark token obtained.")

    lark_records = fetch_lark_records(token)
    print(f"Fetched {len(lark_records)} records from Lark Base.")

    api_customers = fetch_api_customers()
    print(f"Fetched {len(api_customers)} customers from Django API.")

    if TEST_MODE:
        api_customers = api_customers[:10]
        print("TEST MODE ENABLED: Only syncing first 10 customers")

    phone_to_record = {rec["fields"].get("Phone", "").strip(): rec for rec in lark_records}

    for customer in api_customers:
        fields = clean_fields(customer)
        print(f" Syncing: {fields['Full Name']} | {fields['Phone']}")
        record = phone_to_record.get(fields["Phone"])

        try:
            if record:
                # Update
                record_id = record["record_id"]
                url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records/{record_id}"
                res = requests.patch(url, headers={"Authorization": f"Bearer {token}"}, json={"fields": fields})
                res.raise_for_status()
                print(f"Updated: {fields['Phone']}")
            else:
                # Create with retry
                url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
                for retry in range(5):
                    res = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"fields": fields})
                    if res.status_code == 429:
                        print(f"⚠️ Rate limited during create. Retry {retry + 1}/5. Sleeping 5s...")
                        time.sleep(5)
                        continue
                    res.raise_for_status()
                    print(f" Created: {fields['Phone']}")
                    break
        except requests.exceptions.RequestException as e:
            print(f" Error syncing {fields['Phone']}: {e}")

# === Main Runner ===
if __name__ == "__main__":
    sync_customers()
