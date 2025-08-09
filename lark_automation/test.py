import requests

LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblBmPEwSywTlDBu"

def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    return res.json()["tenant_access_token"]

def get_field_definitions():
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    fields = res.json()["data"]["items"]

    print("📋 Field Definitions:\n")
    for field in fields:
        print(f"- Name: {field['field_name']}")
        print(f"  → Type: {field['type']}")
        if field["type"] == 3 and "options" in field["property"]:  # SingleSelect
            print("  → Options:", [opt["name"] for opt in field["property"]["options"]])
        print()
    return fields





def get_sample_records(limit=3):
    token = get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_BASE_ID}/tables/{LARK_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": limit}
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    items = res.json()["data"]["items"]

    print("📦 Sample Record Values:\n")
    for idx, item in enumerate(items, 1):
        print(f"--- Record {idx} ---")
        for key, value in item["fields"].items():
            print(f"{key}: {value}")
        print()

# Run this:
if __name__ == "__main__":
    get_field_definitions()
    get_sample_records()

