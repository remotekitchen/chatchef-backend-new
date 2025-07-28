
import requests
import math
from datetime import timedelta
from django.db.models import Prefetch, Subquery, OuterRef, Count, Q, Exists

from hotel.models import Hotel, RoomType, RoomAvailability,Guest
from rest_framework.response import Response
from hotel.api.base.serializers import GuestSerializer
import requests
from decimal import Decimal
import math



def get_or_create_guest(data, user=None):
    guest = Guest.objects.filter(email=data.get("email")).first()
    serializer = GuestSerializer(guest, data=data, partial=bool(guest)) if guest else GuestSerializer(data=data)
    serializer.is_valid(raise_exception=True)
    return serializer.save(user=user if user else None)


def calculate_booking_cost(room_type, nights=0, rooms=1, coupon=None, extra_bed_requested=False,
                           extra_bed_rooms_count=0, extra_bed_count_per_room=0,
                           booking_type="daily", hours=0):
    if booking_type == "hourly":
        base_price = room_type.hourly_price or Decimal("0.00")
        total_room_price = base_price * rooms * hours
    else:
        base_price = room_type.get_current_price()
        total_room_price = base_price * nights * rooms

    tax_rate = getattr(room_type, "tax_rate", getattr(room_type.hotel, "tax_rate", 0)) or 0
    service_fee = getattr(room_type, "service_fee", getattr(room_type.hotel, "service_fee", 0)) or 0

    taxes = (total_room_price * Decimal(tax_rate)) / 100
    service_fee_total = Decimal(service_fee)

    extra_bed_fee_total = Decimal("0.00")
    if extra_bed_requested and room_type.extra_bed_fee and extra_bed_rooms_count > 0 and extra_bed_count_per_room > 0:
        extra_bed_fee = Decimal(room_type.extra_bed_fee)
        if booking_type == "hourly":
            extra_bed_fee_hourly = extra_bed_fee / Decimal("24")
            extra_bed_fee_total = extra_bed_fee_hourly * hours * extra_bed_rooms_count * extra_bed_count_per_room
        else:
            extra_bed_fee_total = extra_bed_fee * nights * extra_bed_rooms_count * extra_bed_count_per_room

    discount_amount = Decimal(coupon.discount_amount) if coupon else Decimal("0.00")

    total_price = total_room_price + taxes + service_fee_total + extra_bed_fee_total - discount_amount

    return (
        Decimal(base_price),
        Decimal(total_room_price),
        Decimal(taxes),
        service_fee_total,
        discount_amount,
        total_price,
        extra_bed_fee_total
    )

