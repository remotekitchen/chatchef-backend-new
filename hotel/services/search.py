from hotel.models import Hotel, RoomType, RoomAvailability
from hotel.utils.location import get_lat_lon_from_city, get_date_range, haversine
from django.core.cache import cache
from datetime import datetime
from django.db.models import Prefetch, Q
from hotel.utils.helpers import get_date_range,haversine,bounding_box
from hotel.models import RoomHourlyAvailability


def search_available_hotels(
    request,
    lat,
    lon,
    radius_km=5,
    checkin=None,
    checkout=None,
    booking_type="daily",
    checkin_time=None,
    checkout_time=None,
    mapbox_key=None,
    number_of_adults=1,
    number_of_children=0,
    number_of_rooms=1,
    min_price=None,
    max_price=None,
    room_amenity_filters=None,
):
    import logging
    logger = logging.getLogger(__name__)

    # Validate and parse dates
    try:
        checkin_date = datetime.strptime(checkin, "%Y-%m-%d").date()

        if booking_type == "hourly":
            if not checkout:
                checkout_date = checkin_date
            else:
                checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()
                if checkout_date != checkin_date:
                    return None, {"error": "For hourly bookings, check-in and check-out dates must be the same."}
        else:
            if not checkout:
                return None, {"error": "Checkout date is required for daily bookings."}
            checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()
            if checkout_date <= checkin_date:
                return None, {"error": "Checkout must be after checkin."}
    except ValueError:
        return None, {"error": "Invalid checkin/checkout format. Use YYYY-MM-DD."}

    is_hourly = booking_type == "hourly"
    date_range = [checkin_date] if is_hourly else get_date_range(checkin_date, checkout_date)

    total_guests = number_of_adults + number_of_children

    if room_amenity_filters is None:
        room_amenity_filters = {}

    # Filter RoomTypes by occupancy and amenities
    room_types_qs = RoomType.objects.filter(
        max_adults__gte=number_of_adults,
        max_children__gte=number_of_children,
        max_occupancy__gte=total_guests,
        **room_amenity_filters
    )

    if is_hourly:
        room_types_qs = room_types_qs.filter(is_hourly=True)
        if min_price is not None:
            try:
                min_price_val = float(min_price)
                room_types_qs = room_types_qs.filter(hourly_price__gte=min_price_val)
            except ValueError:
                pass
        if max_price is not None:
            try:
                max_price_val = float(max_price)
                room_types_qs = room_types_qs.filter(hourly_price__lte=max_price_val)
            except ValueError:
                pass
    else:
        if min_price is not None:
            try:
                min_price_val = float(min_price)
                room_types_qs = room_types_qs.filter(price_per_night__gte=min_price_val)
            except ValueError:
                pass
        if max_price is not None:
            try:
                max_price_val = float(max_price)
                room_types_qs = room_types_qs.filter(price_per_night__lte=max_price_val)
            except ValueError:
                pass

    hotel_ids_with_room_types = room_types_qs.values_list("hotel_id", flat=True).distinct()

    # Add buffer to bounding box to avoid missing hotels close to edges
    buffer_km = 2
    min_lat, max_lat, min_lon, max_lon = bounding_box(lat, lon, radius_km + buffer_km)

    logger.info(f"Searching hotels within radius: {radius_km} km around lat={lat}, lon={lon}")
    logger.info(f"Bounding box: min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}")
    logger.info(f"Hotel IDs with matching room types: {list(hotel_ids_with_room_types)}")

    hotels = Hotel.objects.filter(
        is_active=True,
        latitude__gte=min_lat,
        latitude__lte=max_lat,
        longitude__gte=min_lon,
        longitude__lte=max_lon,
        id__in=hotel_ids_with_room_types,
    ).only("id", "latitude", "longitude")

    hotel_objs = hotels.prefetch_related(
        Prefetch("room_types", queryset=room_types_qs.prefetch_related("availability"))
    )

    available_hotels = []

    for hotel in hotel_objs:
        hotel_lat = float(hotel.latitude)
        hotel_lon = float(hotel.longitude)

        distance = haversine(lat, lon, hotel_lat, hotel_lon)
        logger.debug(f"Hotel {hotel.id} at ({hotel_lat}, {hotel_lon}), Distance: {distance:.2f} km")

        # Strictly filter by exact distance radius
        if distance > radius_km:
            logger.debug(f"Skipping hotel {hotel.id} - outside radius ({distance:.2f} km)")
            continue

        # Check room availability per hotel
        for room in hotel.room_types.all():
            if is_hourly:
                checkin_time_obj = datetime.strptime(checkin_time, "%H:%M").time()
                checkout_time_obj = datetime.strptime(checkout_time, "%H:%M").time()

                overlapping_slots = RoomHourlyAvailability.objects.filter(
                    room_type=room,
                    date=checkin_date,
                    start_time__lt=checkout_time_obj,
                    end_time__gt=checkin_time_obj,
                )

                if not overlapping_slots.exists():
                    continue

                min_available = min(slot.available_rooms for slot in overlapping_slots)
                if min_available < number_of_rooms:
                    continue

            else:
                avail_entries = [
                    a for a in room.availability.all()
                    if a.date in date_range and a.available_rooms >= number_of_rooms
                ]
                available_dates = set(a.date for a in avail_entries)
                if not all(day in available_dates for day in date_range):
                    continue

                stay_length = len(date_range)
                min_stays = [a.min_nights_stay or 1 for a in avail_entries]
                if any(stay_length < min_stay for min_stay in min_stays):
                    continue

            hotel.distance_km = round(distance, 2)
            available_hotels.append(hotel)
            break  # No need to check more rooms once one fits

    return available_hotels, None
