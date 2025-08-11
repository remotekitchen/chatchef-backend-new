import requests
import uuid
from dateutil import parser
import datetime
import traceback
from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry

LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"
LARK_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID = "tblzjjzgvDMYfb7c"

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
    if value in options:
        return {"text": value}
    elif "Unknown Restaurant" in options:
        # use existing "Unknown Restaurant" option if present
        return {"text": "Unknown Restaurant"}
    elif options:
        # if no "Unknown Restaurant" option exists, create it in Lark or just fallback
        return {"text": "Unknown Restaurant"}
    else:
        return None


def sync_ht_payout_record_to_lark(record, field_info, *, token=None, existing_map=None):
    # 1) Reuse token/map if the caller passed them
    if token is None:
        token = get_lark_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 2) Build payload using ONLY fields that exist in Lark (avoid invalid-field errors)
    fields_payload = {}
    for key, val in record.items():
        meta = field_info.get(key)
        if not meta:
            continue  # skip unknown columns
        if meta["type"] == 3:  # SingleSelect
            opts = meta.get("options") or []
            if isinstance(val, dict) and val.get("text") in opts:
                fields_payload[key] = val["text"]
            elif isinstance(val, str) and val in opts:
                fields_payload[key] = val
            elif opts:
                fields_payload[key] = opts[0]  # safe fallback
            # else: skip if no options configured
        else:
            fields_payload[key] = val

    order_id = fields_payload.get("Order ID")
    if not order_id:
        return

    # 3) Look up existing record id from the map (no full-table fetch per row)
    rec_id = None
    if existing_map is not None:
        rec_id = existing_map.get(order_id)
    else:
        # Build a minimal map once if none provided (slower path)
        existing_map = {}
        for r in get_all_lark_records(token):
            oid = (r.get("fields") or {}).get("Order ID")
            if oid:
                existing_map[oid] = r.get("record_id")
        rec_id = existing_map.get(order_id)

    # 4) CREATE if not found
    if not rec_id:
        res = requests.post(LARK_API_URL, json={"fields": fields_payload}, headers=headers)
        res.raise_for_status()
        rid = (res.json().get("data") or {}).get("record", {}).get("record_id")
        if rid:
            if existing_map is not None:
                existing_map[order_id] = rid
            print(f"CREATED {order_id}")
        else:
            print(f"CREATE FAIL {order_id}: {res.text}")
        return

    # 5) UPDATE only changed fields
    res_get = requests.get(f"{LARK_API_URL}/{rec_id}", headers=headers)
    res_get.raise_for_status()
    existing_fields = (res_get.json().get("data") or {}).get("record", {}).get("fields", {}) or {}

    def _norm(v):
        if isinstance(v, dict) and "text" in v: return v["text"]
        if isinstance(v, list) and v and isinstance(v[0], dict) and "text" in v[0]: return v[0]["text"]
        return v

    changed = {k: v for k, v in fields_payload.items() if _norm(existing_fields.get(k)) != _norm(v)}
    if not changed:
        print(f"SKIP {order_id}")
        return

    res_upd = requests.put(f"{LARK_API_URL}/{rec_id}", json={"fields": changed}, headers=headers)
    res_upd.raise_for_status()
    print(f"UPDATED {order_id}: {', '.join(changed.keys())}")


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
                    "Hungrytiger Discount": float(data["hungrytiger_discount"]),
                     
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

from django.db.models import Q


def push_all_hungry_orders_direct():
    from billing.models import Order, Location
    from billing.utilities.generate_invoices_for_hungry import generate_excel_invoice_for_hungry

    field_info = get_field_definitions()
    token = get_lark_token()

    # Build a quick map of Order ID → Lark record_id (to speed up matching)
    existing_map = {}
    for r in get_all_lark_records(token):
        oid = (r.get("fields") or {}).get("Order ID")
        if oid:
            existing_map[oid] = r.get("record_id")

    # ✅ Include only PENDING orders (do NOT exclude pending)
    #    Keep cancelled/rejected out.
    primary_query = (
        (Q(is_paid=True) | Q(payment_method=Order.PaymentMethod.CASH))
       
    )
    exclude_test_order = Q(customer__icontains="test")
    rejected_canceled_order = Q(status__iexact="cancelled") | Q(status__iexact="rejected")|Q(status__iexact="pending")

    base_qs = (
        Order.objects
        .filter(primary_query)
        .filter(restaurant__is_remote_Kitchen=True)
        .exclude(exclude_test_order)
        .exclude(rejected_canceled_order)
        .select_related("restaurant", "location")
        .order_by("created_date")
    )

    total_eligible = base_qs.count()
    print(f"✅ Eligible orders to consider: {total_eligible}")

    # Loop by location to reuse your generator logic
    loc_ids = base_qs.values_list("location_id", flat=True).distinct()
    for loc in Location.objects.filter(id__in=loc_ids).select_related("restaurant"):
        rest = loc.restaurant
        orders = base_qs.filter(restaurant_id=rest.id, location_id=loc.id)

        # Map order_id -> Order so we can read restaurant from each order
        order_map = {str(o.order_id): o for o in orders}

        _, obj = generate_excel_invoice_for_hungry(
            orders=orders,
            restaurant=rest,
            location=loc,
            only_data=True
        )

        for data in obj[0]:
            date_str = str(data.get("order_date") or "").strip()
            if not date_str:
                continue

            order_id = str(data["order_id"])
            order_obj = order_map.get(order_id)

            # ✅ Restaurant name from the Order; fallback to loop restaurant; final fallback string
            if order_obj and order_obj.restaurant and order_obj.restaurant.name:
                restaurant_name = order_obj.restaurant.name
            elif rest and getattr(rest, "name", None):
                restaurant_name = rest.name
            else:
                restaurant_name = "Unknown Restaurant"

            formatted = {
                "Order ID": order_id,
                "Order Date": int(__import__("datetime").datetime.strptime(date_str[:10], "%Y-%m-%d").timestamp() * 1000),
                "Restaurant": validate_select_value("Restaurant", restaurant_name, field_info),
                "Payment Type": data["payment_type"],
                "Order Mode": validate_select_value("Order Mode", data["order_mode"], field_info),

                "Original Item Price": float(data["actual_item_price"]),
                "Item Price": float(data["item_price"]),
                "BOGO Item Inflation Percentage": float(data["BOGO_item_inflation_percentage"]) / 100.0,
                "Discount": float(data["discount"]),
                "Restaurant Discount": float(data["restaurant_discount"]),
                "Hungrytiger Discount": float(data["hungrytiger_discount"]),

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

            # Efficient update/create
            sync_ht_payout_record_to_lark(
                formatted, field_info, token=token, existing_map=existing_map
            )
