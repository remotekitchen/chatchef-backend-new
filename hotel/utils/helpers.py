from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
import math
import csv
from io import StringIO
from datetime import datetime
from decimal import Decimal, InvalidOperation
from django.db import transaction
from hotel.models import RoomType, RoomAvailability
from django.core.files.storage import default_storage
from hotel.models import RoomHourlyAvailability
import math
from django.db import transaction
from io import StringIO
import csv




from django.core.cache import cache

CACHE_VERSION_KEY = "hotel_search_version"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # distance = haversine(22.8456, 89.5403, 22.7300, 89.6100)
    # print(f"Distance: {distance} km")
    return R * c


def get_date_range(start_date, end_date):
    delta = end_date - start_date
    return [start_date + timedelta(days=i) for i in range(delta.days)]




def bounding_box(lat, lon, radius_km):
    earth_radius = 6371

    lat_delta = (radius_km / earth_radius) * (180 / math.pi)
    min_lat = lat - lat_delta
    max_lat = lat + lat_delta

    lon_delta = (radius_km / earth_radius) * (180 / math.pi) / math.cos(lat * math.pi / 180)
    min_lon = lon - lon_delta
    max_lon = lon + lon_delta

    return min_lat, max_lat, min_lon, max_lon




def validate_bulk_inventory_csv(file_obj, room_type):
    errors = []
    processed_dates = set()

    try:
        file_content = file_obj.read().decode("utf-8")
    except UnicodeDecodeError:
        return ["CSV file must be UTF-8 encoded."]
    finally:
        file_obj.seek(0)

    csvfile = StringIO(file_content)
    reader = csv.DictReader(csvfile)

    required_fields = {"date", "available_rooms"}
    if not required_fields.issubset(reader.fieldnames or []):
        return ["Missing required headers: 'date' and 'available_rooms'"]

    row_num = 1
    for row in reader:
        row_num += 1

        date_str = row.get("date", "").strip()
        available_rooms_str = row.get("available_rooms", "").strip()

        if not date_str or not available_rooms_str:
            errors.append(f"Row {row_num}: 'date' and 'available_rooms' are required.")
            continue

        try:
            availability_date = datetime.strptime(date_str, "%m/%d/%Y").date()
        except ValueError:
            errors.append(f"Row {row_num}: Invalid date format '{date_str}'. Expected MM/DD/YYYY.")
            continue

        if availability_date in processed_dates:
            errors.append(f"Row {row_num}: Duplicate date '{date_str}' in file.")
            continue
        processed_dates.add(availability_date)

        try:
            available_rooms = int(available_rooms_str)
            if available_rooms < 0:
                raise ValueError
            if available_rooms > room_type.total_rooms:
                errors.append(
                    f"Row {row_num}: 'available_rooms' exceeds total rooms ({room_type.total_rooms})."
                )
                continue
        except ValueError:
            errors.append(f"Row {row_num}: 'available_rooms' must be a non-negative integer.")
            continue

        price_override_str = row.get("price_override", "").strip()
        try:
            if price_override_str:
                price_override = Decimal(price_override_str)
                if price_override < 0:
                    raise InvalidOperation
        except (InvalidOperation, ValueError):
            errors.append(f"Row {row_num}: Invalid 'price_override' value.")
            continue

    return errors




def process_bulk_inventory_csv(room_type_id: int, csv_path: str):
    from hotel.models import RoomType, RoomAvailability  # Adjust import
    room_type = RoomType.objects.get(id=room_type_id)
    inserted_count = 0
    updated_count = 0
    errors = []

    with default_storage.open(csv_path, mode='rb') as f:
        try:
            decoded = f.read().decode('utf-8')
        except UnicodeDecodeError:
            return {"inserted": 0, "updated": 0, "errors": ["CSV file must be UTF-8 encoded."]}

    reader = csv.DictReader(StringIO(decoded))
    rows = list(reader)

    with transaction.atomic():
        for row_num, row in enumerate(rows, start=1):
            try:
                date_obj = datetime.strptime(row['date'].strip(), '%m/%d/%Y').date()
                available_rooms = int(row['available_rooms'])

                price_override_str = row.get('price_override', '').strip()
                price_override = Decimal(price_override_str) if price_override_str else None

                is_special_offer_str = row.get('is_special_offer', '').strip().lower()
                is_special_offer = is_special_offer_str in ('true', '1', 'yes')

                special_offer_name = row.get('special_offer_name', '').strip() or None

                min_nights_stay_str = row.get('min_nights_stay', '').strip()
                try:
                    min_nights_stay = int(min_nights_stay_str) if min_nights_stay_str else 1
                    if min_nights_stay < 1:
                        min_nights_stay = 1
                except ValueError:
                    min_nights_stay = 1

                obj, created = RoomAvailability.objects.update_or_create(
                    room_type=room_type,
                    date=date_obj,
                    defaults={
                        'available_rooms': available_rooms,
                        'price_override': price_override,
                        'is_special_offer': is_special_offer,
                        'special_offer_name': special_offer_name,
                        'min_nights_stay': min_nights_stay,
                    }
                )
                if created:
                    inserted_count += 1
                else:
                    updated_count += 1

            except Exception as e:
                errors.append(f"Row {row_num}: {str(e)}")

    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "errors": errors,
    }




def check_hourly_availability(room_type, date, checkin_time, checkout_time, rooms_requested):
    overlapping_slots = RoomHourlyAvailability.objects.select_for_update().filter(
        room_type=room_type,
        date=date,
        start_time__lt=checkout_time,
        end_time__gt=checkin_time,
    )
    if not overlapping_slots.exists():
        return False, "No availability for the requested hourly time range."

    min_available = min(slot.available_rooms for slot in overlapping_slots)
    if min_available < rooms_requested:
        return False, f"Only {min_available} rooms available in the requested time slot."
    return True, None

def block_hourly_availability(room_type, date, checkin_time, checkout_time, rooms):
    overlapping_slots = RoomHourlyAvailability.objects.select_for_update().filter(
        room_type=room_type,
        date=date,
        start_time__lt=checkout_time,
        end_time__gt=checkin_time,
    )
    for slot in overlapping_slots:
        slot.available_rooms = max(slot.available_rooms - rooms, 0)
        slot.save()




from django.core.cache import cache

CACHE_VERSION_KEY = "hotel_search_version"

def get_cache_version():
    version = cache.get(CACHE_VERSION_KEY)
    if version is None:
        version = "v1"
        cache.set(CACHE_VERSION_KEY, version)
    return version

def increment_cache_version():
    version = cache.get(CACHE_VERSION_KEY)
    if version is None:
        version = "v1"
    else:
        try:
            num = int(version[1:])
            version = f"v{num + 1}"
        except Exception:
            version = "v1"
    cache.set(CACHE_VERSION_KEY, version)