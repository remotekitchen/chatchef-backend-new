# billing/lark_helpers.py
import requests

def lark_update_fields(base_id: str, table_id: str, record_id: str, token: str, fields: dict):
    assert record_id, "record_id is required"
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {"fields": fields}

    res = requests.put(url, headers=headers, json=payload)
    # Debug log (keep while testing)
    print("Lark PUT:", url, payload, res.status_code, res.text[:300])

    res.raise_for_status()
    return res.json()
