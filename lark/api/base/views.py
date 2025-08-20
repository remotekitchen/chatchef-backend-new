"""
PUT-only invoice generator that writes ONLY the Excel URL into a TEXT field
(e.g. 'invoice excel'). No PDF link is written.

POST /api/billing/v1/invoice/generate_DO_or_HT
Headers:  X-Lark-Token: <webhook token>
Body:     {"control_record_id":"<record_id>"}
"""

from __future__ import annotations

import json
import typing as t
import datetime as dt
import requests
from requests.adapters import HTTPAdapter, Retry

from django.utils import timezone

from lark.utilities.jobrunner import ht_sync_runner
from lark.utilities.invoice_exporter_do import generate_invoice_files_do
from lark.utilities.invoice_exporter_ht import generate_invoice_files_ht



# billing/views_ht_update.py

import json
import traceback
import requests  # needed for get_lark_token
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

from lark.utilities.jobrunner import ht_sync_runner
from lark.lark_automation.sync_ht_payout import push_all_hungry_orders_direct
from lark.lark_automation.sync_DO_calculation import push_all_DO_invoices_to_lark
from lark.utilities.lark_helpers import lark_update_fields

# shared secret (matches Lark header)
LARK_WEBHOOK_TOKEN = "UKqiyV4W0iHlDDNW9-352CqWmig-ZmJDy64jNIB5wxU"

#  fixed IDs (don’t send these from Lark)
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
CONTROL_TABLE_ID = "tblzjjzgvDMYfb7c"  # HT control table (has the status field)

# DO control table (for DO module runs)
LARK_DO_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_DO_TABLE_ID = "tblIXtMH8WhFDQ9Z"

# Lark auth for updating Status fields
LARK_APP_ID        = "cli_a8030393dd799010"
LARK_APP_SECRET    = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"


def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    res = requests.post(url, json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET})
    res.raise_for_status()
    return res.json().get("tenant_access_token")


# ---------- Reusable helpers ----------

def get_field_definitions_for(base_id: str, table_id: str, *, token: str = None):
    """Return field_name -> {type, options} (options only for SingleSelect)."""
    token = token or get_lark_token()
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    data = res.json()

    field_map = {}
    for f in (data.get("data", {}) or {}).get("items", []) or []:
        opts = []
        if f.get("type") == 3:  # SingleSelect
            raw = (f.get("property") or {}).get("options", []) or []
            # Options can be under "name" or "text"
            opts = [(o.get("name") or o.get("text")) for o in raw if (o.get("name") or o.get("text"))]
        field_map[f["field_name"]] = {"type": f.get("type"), "options": opts}
    return field_map


def resolve_field_name_ci(field_info: dict, wanted: str) -> str:
    """Resolve actual field name in Bitable, case-insensitive match."""
    wanted_l = str(wanted).lower()
    for fname in field_info.keys():
        if str(fname).lower() == wanted_l:
            return fname
    return wanted  # fallback


def choose_select_option(field_info: dict, field_name: str, desired: str) -> str:
    """Pick a valid SingleSelect option; fallback to first available or desired."""
    real = resolve_field_name_ci(field_info, field_name)
    options = (field_info.get(real) or {}).get("options", []) or []
    return desired if desired in options else (options[0] if options else desired)


# ---------- HT webhook ----------


@csrf_exempt
def lark_ht_update(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST required")
    if request.headers.get("X-Lark-Token") != LARK_WEBHOOK_TOKEN:
        return HttpResponseForbidden("Invalid token")

    try:
        body = json.loads(request.body or "{}")
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    control_record_id = body.get("control_record_id")
    if not control_record_id:
        return HttpResponseBadRequest("Missing control_record_id")

    token = get_lark_token()

    # Pull valid options for the SingleSelect "status" FROM THE HT CONTROL TABLE
    field_info = get_field_definitions_for(LARK_BASE_ID, CONTROL_TABLE_ID, token=token)
    status_field = resolve_field_name_ci(field_info, "status")
    running = choose_select_option(field_info, status_field, "Running")

    # Set status: Running
    try:
        lark_update_fields(LARK_BASE_ID, CONTROL_TABLE_ID, control_record_id, token, {status_field: running})
    except Exception as e:
        return JsonResponse({"status": "error", "message": f"Failed to set Running: {e}"}, status=500)

    # Background job flips to Done/Failed using the same table/field
    def job():
        try:
            push_all_hungry_orders_direct()
            done = choose_select_option(field_info, status_field, "Done")
            lark_update_fields(LARK_BASE_ID, CONTROL_TABLE_ID, control_record_id, token, {status_field: done})
        except Exception:
            traceback.print_exc()
            failed = choose_select_option(field_info, status_field, "Failed")
            try:
                lark_update_fields(LARK_BASE_ID, CONTROL_TABLE_ID, control_record_id, token, {status_field: failed})
            except Exception:
                pass

    started = ht_sync_runner.start(target=job)
    if not started:
        return JsonResponse({"status": "busy", "message": "HT update already running"}, status=202)

    return JsonResponse({"status": "queued", "module": "HT", "started_at": timezone.now().isoformat()}, status=202)


# ---------- DO webhook (correct & reused) ----------

@csrf_exempt
def lark_DO_update(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST required")
    if request.headers.get("X-Lark-Token") != LARK_WEBHOOK_TOKEN:
        return HttpResponseForbidden("Invalid token")

    try:
        body = json.loads(request.body or "{}")
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    control_record_id = body.get("control_record_id")
    if not control_record_id:
        return HttpResponseBadRequest("Missing control_record_id")

    token = get_lark_token()

    # Pull valid options for the SingleSelect "status" FROM THE DO CONTROL TABLE
    field_info = get_field_definitions_for(LARK_DO_BASE_ID, LARK_DO_TABLE_ID, token=token)
    status_field = resolve_field_name_ci(field_info, "status")
    running = choose_select_option(field_info, status_field, "Running")

    # Set status: Running (on DO control table)
    try:
        lark_update_fields(LARK_DO_BASE_ID, LARK_DO_TABLE_ID, control_record_id, token, {status_field: running})
    except Exception as e:
        return JsonResponse({"status": "error", "message": f"Failed to set Running: {e}"}, status=500)

    # Background job flips to Done/Failed using the DO table/field
    def job():
        try:
            push_all_DO_invoices_to_lark()
            done = choose_select_option(field_info, status_field, "Done")
            lark_update_fields(LARK_DO_BASE_ID, LARK_DO_TABLE_ID, control_record_id, token, {status_field: done})
        except Exception:
            traceback.print_exc()
            failed = choose_select_option(field_info, status_field, "Failed")
            try:
                lark_update_fields(LARK_DO_BASE_ID, LARK_DO_TABLE_ID, control_record_id, token, {status_field: failed})
            except Exception:
                pass

    started = ht_sync_runner.start(target=job)
    if not started:
        return JsonResponse({"status": "busy", "message": "DO update already running"}, status=202)

    return JsonResponse({"status": "queued", "module": "DO", "started_at": timezone.now().isoformat()}, status=202)



# ===== CONFIG =====
CONTROL_BASE_ID  = "ALsmbv5kRatPPusWj9vu7HtusV1"
CONTROL_TABLE_ID = "tblU9Qvqb9Rl5TGV"

DATA_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
DO_TABLE_ID  = "tblIXtMH8WhFDQ9Z"
DO_VIEW_ID   = "vewBuNqMBG"

HT_TABLE_ID  = "tblzjjzgvDMYfb7c"
HT_VIEW_ID   = "vewScBhc1z"

LARK_WEBHOOK_TOKEN = "UKqiyV4W0iHlDDNW9-352CqWmig-ZmJDy64jNIB5wxU"

# field name fallbacks
CF_DATE_START       = ["Date- start", "Date start", "date- start", "date start"]
CF_DATE_END         = ["Date End", "Date end", "date end"]
CF_PLATFORM         = ["BM / VR / DR / HT", "BM/VR/DR/HT", "Platform"]
CF_RESTAURANT_NAME  = ["Restaurant name", "Restaurant Name", "Restaurant", "restaurant"]
CF_GEN_INVOICE      = ["generate invoice", "Generate invoice", "Generate Invoice"]
from jinja2 import Environment, FileSystemLoader
# where to write the Excel URL (TEXT field)
CF_INVOICE_EXCEL_TEXT = [
    "invoice excel", "Invoice excel", "Invoice Excel",
    "Invoice URL", "Invoice link", "Excel URL", "Excel link"
]

DF_ORDER_DATE = ["Order Date", "order_date", "Date"]

# --- Lark date-only semantics: UTC+8 midnight ---
UTC8 = dt.timedelta(hours=8)

# === BEGIN: robust date helpers ===
import re

BD_OFFSET = dt.timedelta(hours=6)  # Asia/Dhaka

_MS_RE = re.compile(r"^-?\d{10,13}$")

def _control_date(val: t.Any) -> dt.date:
    """
    Control table Date fields (date-only). Accept ms or 'YYYY-MM-DD'.
    Lark date-only uses a UTC+8 nominal day.
    """
    s = _norm_text(val)
    if not s:
        raise ValueError(f"Bad control date: {val!r}")
    if _MS_RE.match(s):
        n = int(s)
        if len(s) == 10:  # seconds -> ms
            n *= 1000
        dtu = dt.datetime.utcfromtimestamp(n / 1000.0)
        return (dtu + UTC8).date()
    # tolerate plain date strings
    return dt.date.fromisoformat(s.replace("/", "-")[:10])

def _order_row_date(val: t.Any) -> dt.date | None:
    """
    Row 'Order Date' may be:
      - date-only ms (00:00Z or 16:00Z)  -> treat as UTC+8 nominal day
      - real UTC datetime ms             -> convert to Asia/Dhaka local day
      - 'YYYY-MM-DD' / 'YYYY/MM/DD'      -> parse as calendar day
    """
    s = _norm_text(val)
    if not s:
        return None

    if _MS_RE.match(s):
        n = int(s)
        if len(s) == 10:
            n *= 1000
        dtu = dt.datetime.utcfromtimestamp(n / 1000.0)
        # Heuristic: date-only epochs show at 00:00Z or 16:00Z
        if dtu.minute == 0 and dtu.second == 0 and dtu.hour in (0, 16):
            return (dtu + UTC8).date()
        # true UTC datetime -> shift to BD local
        return (dtu + BD_OFFSET).date()

    # tolerate plain strings
    try:
        return dt.date.fromisoformat(s.replace("/", "-")[:10])
    except Exception:
        return None
# === END: robust date helpers ===


def _lark_ms_to_nominal_date(ms: int | str) -> dt.date:
    """Lark 'Date' ms -> add +8h, then take the calendar date."""
    if ms is None:
        raise ValueError("ms is None")
    ms = int(str(ms))
    dtu = dt.datetime.utcfromtimestamp(ms / 1000.0)
    return (dtu + UTC8).date()

# ===== HTTP session =====
HTTP = requests.Session()
HTTP.headers["Content-Type"] = "application/json; charset=utf-8"
_retry = Retry(
    total=5, connect=3, read=3, backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST", "PUT"]),
)
HTTP.mount("https://", HTTPAdapter(max_retries=_retry))

# ===== helpers =====
def _H(tok: str) -> dict:
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"}

def _ok_bitable(resp) -> bool:
    try:
        j = resp.json()
        return resp.status_code == 200 and j.get("code") == 0
    except Exception:
        return False

def _bitable_put(token: str, base_id: str, table_id: str, record_id: str, fields_obj: dict) -> dict:
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    r = requests.put(url, headers=_H(token), json={"fields": fields_obj}, timeout=(10, 60))
    if _ok_bitable(r):
        return r.json()
    try:
        body = r.json()
    except Exception:
        body = r.text
    raise RuntimeError(f"Bitable PUT failed: {r.status_code} {body}")

def _norm_text(v: t.Any) -> str | None:
    if v is None: return None
    if isinstance(v, dict) and "text" in v: return str(v["text"]).strip()
    if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]: return str(v[0]["text"]).strip()
    if isinstance(v, (int, float)): return str(v)
    if isinstance(v, str): return v.strip()
    return str(v).strip()

def _first_field(fields: dict, names: list[str]) -> t.Any:
    for n in names:
        if n in fields and fields[n] not in (None, ""):
            return fields[n]
    return None

def _dataset_from_control(val: t.Any) -> str | None:
    up = (_norm_text(val) or "").upper()
    if up in {"VR", "DR", "BM", "DO", "DOORDASH"}: return "DO"
    if up == "HT": return "HT"
    return None

def _truthy(val: t.Any) -> bool:
    s = (_norm_text(val) or "").lower()
    return s not in {"", "0", "false", "no", "off"}

def get_lark_token() -> str:
    # Obtain tenant_access_token using APP ID/SECRET.
    r = HTTP.post(
        "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/",
        json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
        timeout=(10, 60),
    )
    r.raise_for_status()
    return r.json()["tenant_access_token"]

def lark_get_record(base_id: str, table_id: str, record_id: str, token: str) -> dict:
    r = HTTP.get(
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}",
        headers=_H(token), timeout=(10, 60)
    )
    r.raise_for_status()
    return (r.json().get("data") or {}).get("record") or {}

def lark_list_records(base_id: str, table_id: str, token: str, *, view_id: str | None = None, page_size: int = 500) -> list[dict]:
    items, page = [], None
    while True:
        params = {"page_size": page_size}
        if page:    params["page_token"] = page
        if view_id: params["view_id"]    = view_id
        r = HTTP.get(
            f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records",
            headers=_H(token), params=params, timeout=(10, 90)
        )
        r.raise_for_status()
        data = r.json().get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"): break
        page = data.get("page_token")
    return items

def lark_get_fields(base_id: str, table_id: str, token: str) -> list[dict]:
    r = HTTP.get(
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/fields",
        headers=_H(token), timeout=(10, 60)
    )
    r.raise_for_status()
    return (r.json().get("data") or {}).get("items") or []

def _is_text_type(ftype: int) -> bool:
    return int(ftype) in (1, 11, 19, 20, 1001)  # text-ish

def find_text_field_name(base_id: str, table_id: str, token: str, candidates: list[str]) -> str | None:
    fields = lark_get_fields(base_id, table_id, token)
    cand_lc = [c.lower() for c in candidates]
    for f in fields:
        name = f.get("field_name") or ""
        if name.lower() in cand_lc and _is_text_type(f.get("type") or 0):
            return name
    for f in fields:
        name = (f.get("field_name") or "")
        if _is_text_type(f.get("type") or 0) and any(k in name.lower() for k in ("invoice", "url", "link", "excel")):
            return name
    return None


def _list_records_union_all(base_id: str, table_id: str, token: str, view_id: str | None) -> list[dict]:
    """
    Fetch from the given view (if any) + the whole table (no view),
    then de-dupe by record_id. Prevents view filters (e.g., 'only August')
    from hiding days like 07/31.
    """
    seen: set[str] = set()
    out: list[dict] = []

    def _add(batch: list[dict] | None):
        if not batch:
            return
        for rec in batch:
            rid = rec.get("record_id") or rec.get("id")
            if rid and rid not in seen:
                seen.add(rid)
                out.append(rec)

    if view_id:
        _add(lark_list_records(base_id, table_id, token, view_id=view_id))
    _add(lark_list_records(base_id, table_id, token, view_id=None))
    return out

# ===== dataset rows =====
def _collect_rows(token: str, start_ms: int, end_ms: int, restaurant_name: str, dataset: str) -> list[dict]:
    """
    Exact calendar-day match against the Control row:
    - Control dates are 'date-only' -> UTC+8 nominal day
    - Row 'Order Date' can be ms (date-only or datetime) or 'YYYY-MM-DD'
      -> resolve to the correct calendar day and compare inclusively.
    Also bypasses view filters by fetching union(view + no view).
    """
    if dataset == "DO":
        table_id, view_id = DO_TABLE_ID, DO_VIEW_ID
    elif dataset == "HT":
        table_id, view_id = HT_TABLE_ID, HT_VIEW_ID
    else:
        raise RuntimeError(f"Unknown dataset: {dataset}")

    start_d = _control_date(start_ms)
    end_d   = _control_date(end_ms)

    rows: list[dict] = []
    rname_l = (_norm_text(restaurant_name) or "").lower()

    # IMPORTANT: fetch beyond the view to avoid losing 07/31, etc.
    all_recs = _list_records_union_all(DATA_BASE_ID, table_id, token, view_id)

    # (optional) debug to prove which days exist
    # seen_days: dict[dt.date, int] = {}
    for rec in all_recs:
        f = rec.get("fields") or {}

        # restaurant exact match (case-insensitive)
        rest = _norm_text(next((f.get(n) for n in ["Restaurant name","Restaurant Name","Restaurant","restaurant"] if n in f), None))
        if not rest or rest.lower() != rname_l:
            continue

        od_raw = next((f.get(n) for n in DF_ORDER_DATE if n in f), None)
        if od_raw is None:
            continue

        od = _order_row_date(od_raw)
        if not od:
            continue

        # seen_days[od] = 1 + seen_days.get(od, 0)

        if start_d <= od <= end_d:
            rows.append(f)

    # print(f"[DEBUG] range {start_d}..{end_d} matched_days={sorted(seen_days) if seen_days else []} kept={len(rows)}")
    return rows

# ===== core job =====
# ===== core job =====
def _run_invoice_job(control_record_id: str) -> dict:
    tok = get_lark_token()

    ctrl = lark_get_record(CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, tok)
    f = ctrl.get("fields") or {}

    start_raw = _first_field(f, CF_DATE_START)
    end_raw   = _first_field(f, CF_DATE_END)
    rname     = _norm_text(_first_field(f, CF_RESTAURANT_NAME))
    platform  = _norm_text(_first_field(f, CF_PLATFORM))
    gen_flag  = _truthy(_first_field(f, CF_GEN_INVOICE))
    dataset   = _dataset_from_control(platform)

    if not gen_flag:
        raise RuntimeError("Generate invoice is not checked.")
    if not (start_raw and end_raw and rname):
        raise RuntimeError("Missing one of: Date- start, Date End, Restaurant name.")
    if dataset not in {"DO", "HT"}:
        raise RuntimeError("Unsupported dataset (expect VR/DR/BM/DO → DO, or HT).")

    start_ms = int(start_raw)
    end_ms   = int(end_raw)

    start_nom = _control_date(start_ms).isoformat()
    end_nom   = _control_date(end_ms).isoformat()

    print(f"[CONTROL] restaurant='{rname}', start_date={start_nom}, end_date={end_nom}, dataset={dataset}")

    rows = _collect_rows(tok, start_ms, end_ms, rname, dataset)
    print(f"Prepared {dataset} rows: {len(rows)}")
    if not rows:
        raise RuntimeError("No rows found for given range/restaurant.")

    # Build Excel
    if dataset == "DO":
        excel_path, _ = generate_invoice_files_do(rows, restaurant_name=rname, start_ms=start_ms, end_ms=end_ms)
    else:
        excel_path, _ = generate_invoice_files_ht(rows, restaurant_name=rname, start_ms=start_ms, end_ms=end_ms)

    print(f"Excel: {excel_path}")

    # ---- Write Excel URL into a TEXT field
    excel_field_name = find_text_field_name(CONTROL_BASE_ID, CONTROL_TABLE_ID, tok, CF_INVOICE_EXCEL_TEXT)
    if not excel_field_name:
        raise RuntimeError("No TEXT field found for Excel URL (e.g. 'invoice excel'). Please create one in Control table.")
    _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {excel_field_name: str(excel_path)})

    # ---- If HT, also build PDF from same Lark rows and save to "invoice excel pdf"
    if dataset == "HT":
        from lark.utilities.pdf_ht_builder import render_ht_pdf_from_lark
        pdf_res = render_ht_pdf_from_lark(
            rows=rows,
            restaurant_name=rname,
            start_ms=start_ms,
            end_ms=end_ms,
           
            template_name="invoices/ht_orders_summary.html",
        )
        pdf_url = pdf_res["pdf_url"]
        # Look for a TEXT field named like "invoice excel pdf" first (your request)
        CF_INVOICE_PDF_TEXT = [
            "invoice excel pdf", "Invoice excel pdf", "Invoice Excel PDF",
            "invoice pdf", "Invoice PDF", "PDF URL", "PDF Link"
        ]
        pdf_field_name = find_text_field_name(CONTROL_BASE_ID, CONTROL_TABLE_ID, tok, CF_INVOICE_PDF_TEXT)
        if not pdf_field_name:
            raise RuntimeError("No TEXT field found for PDF URL (e.g. 'invoice excel pdf'). Please create one in Control table.")
        _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {pdf_field_name: str(pdf_url)})
        print(f"PDF: {pdf_url}")

    # Status → Success
    try:
        _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"status": "Success"})
    except Exception:
        _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"Status": "Success"})

    out = {"excel_url": str(excel_path), "count": len(rows), "dataset": dataset}
    if dataset == "HT":
        out["pdf_url"] = pdf_url
    return out


# ===== webhook =====
@csrf_exempt
def lark_generate_invoice(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST required")
    if request.headers.get("X-Lark-Token") != LARK_WEBHOOK_TOKEN:
        return HttpResponseForbidden("Invalid token")

    try:
        body = json.loads(request.body or "{}")
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    control_record_id = body.get("control_record_id")
    if not control_record_id:
        return HttpResponseBadRequest("Missing control_record_id")

    # status → Running (PUT only)
    try:
        tok = get_lark_token()
        try:
            _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"status": "Running"})
        except Exception:
            _bitable_put(tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"Status": "Running"})
    except Exception:
        pass

    def job():
        try:
            res = _run_invoice_job(control_record_id)
            print(f"✅ Invoice built: rows={res['count']} | {res['dataset']} | {res['excel_url']}")
        except Exception as e:
            import traceback; traceback.print_exc()
            try:
                tok2 = get_lark_token()
                try:
                    _bitable_put(tok2, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"status": "Failed"})
                except Exception:
                    _bitable_put(tok2, CONTROL_BASE_ID, CONTROL_TABLE_ID, control_record_id, {"Status": "Failed"})
            except Exception:
                pass
            print(f"❌ Invoice build failed: {e}")

    if not ht_sync_runner.start(target=job):
        return JsonResponse({"status": "busy", "message": "Invoice job already running"}, status=202)

    return JsonResponse({"status": "queued", "module": "INVOICE", "started_at": timezone.now().isoformat()}, status=202)

# ---------- Send API: takes emails, sends the saved PDF ----------

# ---------- Send API: takes emails, sends the saved PDF ----------
# === SEND INVOICE BY EMAIL (SendGrid via marketing.email_sender.send_email) ===
import json, re, requests
from django.conf import settings
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from marketing.email_sender import send_email  # <-- your SendGrid helper
# add near the top
import re
# add near the top with other imports



# add these constants (same spellings you use in Lark)
CF_SEND_EMAIL       = ["send email", "Send email", "send Email", "Send Email"]
CF_EMAIL_TO         = ["Invoice Email Address", "invoice email address"]
CF_INVOICE_PDF_TEXT = ["invoice excel pdf", "Invoice excel pdf", "Invoice Excel PDF",
                       "invoice pdf", "Invoice PDF", "PDF URL", "PDF Link"]


# (reuse existing CF_DATE_START, CF_DATE_END, CF_PLATFORM, CF_RESTAURANT_NAME, CF_INVOICE_EXCEL_TEXT)

# ---------- email helpers ----------
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def _split_emails(s: str) -> list[str]:
    if not s: return []
    found = _EMAIL_RE.findall(s)
    if found: return found
    parts = re.split(r"[,; \n\r\t]+", s)
    return [p for p in (p.strip() for p in parts) if _EMAIL_RE.fullmatch(p)]

def _get_emails_from_fields(fields: dict, names: list[str]) -> list[str]:
    out: list[str] = []
    for n in names:
        v = fields.get(n)
        if isinstance(v, str):
            out += _split_emails(v)
        elif isinstance(v, list):
            for it in v:
                if isinstance(it, str):
                    out += _split_emails(it)
                elif isinstance(it, dict):
                    t = it.get("text") or it.get("value") or it.get("email") or it.get("name") or ""
                    out += _split_emails(str(t))
        elif isinstance(v, dict):
            t = v.get("text") or v.get("value") or v.get("email") or v.get("name") or ""
            out += _split_emails(str(t))
    # de-dupe
    seen=set(); uniq=[]
    for e in out:
        el=e.lower()
        if el not in seen:
            seen.add(el); uniq.append(e)
    return uniq

def _get_checkbox(fields: dict, names: list[str]) -> bool:
    for n in names:
        v = fields.get(n)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, str)) and str(v).strip().lower() in {"1","true","yes","on"}:
            return True
    return False

# ---------- PDF URL helpers ----------
# Lark Cloud Space links sometimes look like /drive/v1/medias/<token>/download or ?file_token=...
_MEDIA_TOKEN_RE = re.compile(r"/drive/v1/medias?/([A-Za-z0-9_-]+)/download")
_QS_TOKEN_RE    = re.compile(r"[?&]file_token=([A-Za-z0-9_-]+)")

def _get_textish(fields: dict, names: list[str]) -> str | None:
    """Return first non-empty text value across text/mixed fields."""
    for n in names:
        v = fields.get(n)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict) and (v.get("text") or v.get("value")):
            return (v.get("text") or v.get("value")).strip()
        if isinstance(v, list) and v:
            # pick first text-looking piece
            it = v[0]
            if isinstance(it, str) and it.strip():
                return it.strip()
            if isinstance(it, dict) and (it.get("text") or it.get("value")):
                return (it.get("text") or it.get("value")).strip()
    return None

def _extract_token_from_url(u: str) -> str | None:
    m = _MEDIA_TOKEN_RE.search(u)
    if m: return m.group(1)
    m = _QS_TOKEN_RE.search(u)
    return m.group(1) if m else None

def _drive_tmp_url(token: str, file_token: str) -> str:
    r = requests.get(
        "https://open.larksuite.com/open-apis/drive/v1/medias/batch_get_tmp_download_url",
        headers={"Authorization": f"Bearer {token}"},
        params={"file_tokens": file_token},
        timeout=(30, 240),
    )
    r.raise_for_status()
    items = (r.json().get("data") or {}).get("tmp_download_urls") or []
    if not items or "tmp_download_url" not in items[0]:
        raise RuntimeError("No tmp_download_url returned")
    return items[0]["tmp_download_url"]

def _drive_file_download_bytes(token: str, file_token: str) -> bytes:
    # Explorer v2 first
    r = requests.get(
        "https://open.larksuite.com/open-apis/drive/explorer/v2/file/download",
        headers={"Authorization": f"Bearer {token}"},
        params={"file_token": file_token},
        timeout=(30, 240),
    )
    if r.status_code == 200:
        return r.content
    # Legacy fallback
    r = requests.get(
        f"https://open.larksuite.com/open-apis/drive/v1/files/{file_token}/download",
        headers={"Authorization": f"Bearer {token}"},
        timeout=(30, 240),
    )
    r.raise_for_status()
    return r.content

def _download_pdf_by_url(tok: str, url_or_token: str) -> tuple[bytes, str]:
    """Accepts either a raw Cloud token or a full URL; returns (bytes, filename)."""
    if not url_or_token:
        raise RuntimeError("Empty PDF URL")
    url_or_token = url_or_token.strip()

    # If a token was passed directly
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", url_or_token):
        try:
            tmp = _drive_tmp_url(tok, url_or_token)
            r = requests.get(tmp, timeout=(30, 240)); r.raise_for_status()
            return r.content, "invoice.pdf"
        except Exception:
            return _drive_file_download_bytes(tok, url_or_token), "invoice.pdf"

    # If a URL
    ft = _extract_token_from_url(url_or_token)
    if ft:
        try:
            tmp = _drive_tmp_url(tok, ft)
            r = requests.get(tmp, timeout=(30, 240)); r.raise_for_status()
            return r.content, "invoice.pdf"
        except Exception:
            return _drive_file_download_bytes(tok, ft), "invoice.pdf"

    # Public/S3/Spaces link etc.
    r = requests.get(url_or_token, timeout=(30, 240))
    r.raise_for_status()
    # Try to infer a name
    name = "invoice.pdf"
    cd = r.headers.get("Content-Disposition") or ""
    m = re.search(r'filename="?([^"]+)"?', cd)
    if m: name = m.group(1)
    elif "." in url_or_token.rsplit("/", 1)[-1]:
        name = url_or_token.rsplit("/", 1)[-1]
    return r.content, name

# ---------- Main endpoint ----------
@csrf_exempt
def send_invoice_pdf(request):
    """
    POST /api/billing/v1/invoice/send
    Headers: X-Lark-Token: <token>
    Body:    {"control_record_id":"recXXXX"}
    """
    if request.method != "POST":
        return HttpResponseBadRequest("POST required")
    if request.headers.get("X-Lark-Token") != LARK_WEBHOOK_TOKEN:
        return HttpResponseBadRequest("Invalid token")

    # Parse body
    try:
        body = json.loads(request.body or "{}")
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")
    rec_id = body.get("control_record_id")
    if not rec_id:
        return HttpResponseBadRequest("Missing control_record_id")

    # Fetch row
    tok = get_lark_token()
    rec = lark_get_record(CONTROL_BASE_ID, CONTROL_TABLE_ID, rec_id, tok)
    f = rec.get("fields") or {}

    # Must be checked
    if not _get_checkbox(f, CF_SEND_EMAIL):
        return HttpResponseBadRequest("'Send email' is not checked on this row.")

    # Recipients
    to_emails = _get_emails_from_fields(f, CF_EMAIL_TO)
    if not to_emails:
        return HttpResponseBadRequest("No 'Invoice Email Address' on this row.")

    # PDF source: strictly from 'invoice excel pdf' (text URL)
    pdf_url_text = _get_textish(f, CF_INVOICE_PDF_TEXT)
    if not pdf_url_text:
        return HttpResponseBadRequest("No 'invoice excel pdf' URL found on this row.")
    try:
        pdf_bytes, pdf_name = _download_pdf_by_url(tok, pdf_url_text)
    except Exception as e:
        return JsonResponse({"ok": False, "error": f"PDF download failed: {e}"}, status=500)

    # Subject/context
    rname = _norm_text(_first_field(f, CF_RESTAURANT_NAME)) or ""
    platform = _norm_text(_first_field(f, CF_PLATFORM)) or ""
    up = platform.upper()
    dataset = "HT" if up == "HT" else ("DO" if up in {"VR","DR","BM","DO","DOORDASH"} else up or "")

    start_raw = _first_field(f, CF_DATE_START); end_raw = _first_field(f, CF_DATE_END)
    date_start = _lark_ms_to_nominal_date(int(start_raw)).isoformat() if start_raw else ""
    date_end   = _lark_ms_to_nominal_date(int(end_raw)).isoformat() if end_raw else ""

    # (Optional) Excel URL if you want it in the email body
    excel_url = _get_textish(f, CF_INVOICE_EXCEL_TEXT)

    subject = "Invoice"
    if dataset: subject += f" ({dataset})"
    if rname:   subject += f" — {rname}"
    if date_start and date_end: subject += f" — {date_start} → {date_end}"

    context = {
        "brand_name": "Thunder Digital Kitchen Ltd",
        "restaurant_name": rname,
        "dataset": dataset,
        "date_start": date_start,
        "date_end": date_end,
        "record_id": rec_id,
        "excel_url": excel_url,
        "pdf_filename": pdf_name,
    }

    # From address
    from_addr = getattr(settings, "DEFAULT_HUNGRY_TIGER_EMAIL", getattr(settings, "DEFAULT_FROM_EMAIL", None))
    if not from_addr:
        return HttpResponseBadRequest("DEFAULT_HUNGRY_TIGER_EMAIL/DEFAULT_FROM_EMAIL not configured.")

    # Fire email
    status_code = send_email(
        subject=subject,
        html_path="email/DO_HT_invoice.html",  # <- your template
        context=context,
        to_emails=to_emails,
        from_email=from_addr,
        attachment={"filename": pdf_name, "content": pdf_bytes, "mimetype": "application/pdf"},
    )
    emailed = (status_code == 202) or bool(status_code)

    # Update row (best effort): uncheck send email + mark status
    try:
        _bitable_put(
            tok, CONTROL_BASE_ID, CONTROL_TABLE_ID, rec_id,
            {"status": "Email Sent" if emailed else "Email Failed", "send email": False}
        )
    except Exception:
        pass

    if not emailed:
        return JsonResponse({"ok": False, "sent": False, "to": to_emails, "status_code": status_code}, status=500)
    return JsonResponse({"ok": True, "sent": True, "to": to_emails, "status_code": status_code})
