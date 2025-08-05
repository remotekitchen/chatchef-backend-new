import json
import requests
from django.conf import settings

BASE = "https://open.larksuite.com/open-apis"

class LarkClient:
    def __init__(self):
        self.token = self._tenant_token()

    def _tenant_token(self):
        r = requests.post(
            f"{BASE}/auth/v3/tenant_access_token/internal",
            json={"app_id": settings.LARK_APP_ID, "app_secret": settings.LARK_APP_SECRET},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["tenant_access_token"]

    def get_record(self, base_id: str, table_id: str, record_id: str) -> dict:
        url = f"{BASE}/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
        r = requests.get(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=30)
        payload = r.json()
        if r.status_code != 200 or "data" not in payload:
            raise RuntimeError(f"Lark get_record failed: status={r.status_code}, body={payload}")
        return payload["data"]["record"]

    def update_record(self, base_id: str, table_id: str, record_id: str, fields: dict):
        url = f"{BASE}/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
        r = requests.put(
            url,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=utf-8"},
            data=json.dumps({"fields": fields}),
            timeout=30,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Lark update_record failed: status={r.status_code}, body={r.text}")
        return r.json()

    def list_records(self, base_id: str, table_id: str) -> list:
        url = f"{BASE}/bitable/v1/apps/{base_id}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        items, page_token = [], ""
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            r = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            data = r.json().get("data", {})
            items += data.get("items", [])
            if not data.get("has_more"):
                break
            page_token = data.get("page_token", "")
        return items

    # Upload specifically for Bitable attachment fields
    def upload_attachment(self, base_id: str, table_id: str, filename: str, content: bytes, mime: str) -> str:
        """
        Try Bitable attachments/upload first; if 404 (not available in your tenant),
        fall back to Drive upload_all and return that token, which still works for
        bitable attachment fields.
        """
        url = f"{BASE}/bitable/v1/apps/{base_id}/tables/{table_id}/attachments/upload"
        headers = {"Authorization": f"Bearer {self.token}"}
        files = {"file": (filename, content, mime)}
        data = {"file_name": filename}
        r = requests.post(url, headers=headers, files=files, data=data, timeout=120)

        if r.status_code == 200:
            return r.json()["data"]["file_token"]

        # If this route isn't available under your tenant/app, you'll see 404.
        # Fall back to Drive upload_all.
        # (Ensure your app has Drive file write scope.)
        if r.status_code == 404:
            return self.upload_file_drive(filename, content, mime)

        # Anything else: surface the error payload to logs
        raise RuntimeError(f"Bitable attachments upload failed: status={r.status_code}, body={r.text}")


    def upload_file_drive(self, filename: str, content: bytes, mime: str) -> str:
        """
        Drive upload fallback. Returns file_token usable in Bitable attachment fields.
        Requires Drive scope enabled for your app.
        """
        url = f"{BASE}/drive/v1/files/upload_all"
        headers = {"Authorization": f"Bearer {self.token}"}
        files = {"file": (filename, content, mime)}
        data = {"file_name": filename}
        r = requests.post(url, headers=headers, files=files, data=data, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Drive upload_all failed: status={r.status_code}, body={r.text}")
        return r.json()["data"]["file_token"]

    # Download works for attachment tokens
    def download_file(self, file_token: str) -> bytes:
        url = f"{BASE}/drive/v1/files/{file_token}/download"
        r = requests.get(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Drive download failed: status={r.status_code}, body={r.text}")
        return r.content
