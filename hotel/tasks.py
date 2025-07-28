from celery import shared_task
from django.core.mail import send_mail
from twilio.rest import Client
from hotel.models import Booking
from django.conf import settings
import logging
import csv
from io import StringIO
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from django.core.cache import cache
from django.core.files.storage import default_storage
from django.db import transaction
from marketing.utils.send_sms import send_sms_bd
from hotel.models import RoomType, RoomAvailability
from hotel.utils.helpers import process_bulk_inventory_csv

logger = logging.getLogger(__name__)

@shared_task
def send_payment_confirmation_message(booking_id):
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"📨 Starting confirmation task for booking ID {booking_id}")
    
    try:
        booking = Booking.objects.select_related("guest", "hotel").get(id=booking_id)
        guest = booking.guest
        hotel = booking.hotel

        logger.info(f"Found guest: {guest.email} / {guest.phone}")

        # ✅ Email message body
        message_body = (
            f"Hi {guest.first_name},\n\n"
            f"✅ Your payment for booking #{booking.booking_number} is confirmed!\n\n"
            f"📍 Hotel: {hotel.name}\n"
            f"📅 Check-in: {booking.check_in_date.strftime('%Y-%m-%d')}\n"
            f"📅 Check-out: {booking.check_out_date.strftime('%Y-%m-%d')}\n"
            f"💳 Total Paid: {booking.total_price} {hotel.currency or 'BDT'}\n\n"
            f"Thank you for choosing {hotel.name}. We look forward to hosting you!\n\n"
            f"Warm regards,\n"
            f"The {hotel.name} Team"
        )

        # ✅ SMS message
        sms_message = (
        f"Hello {guest.first_name},\n"
        f"Your payment for booking #{booking.booking_number} at {hotel.name} has been confirmed ✅.\n"
        f"Check-in Date: {booking.check_in_date.strftime('%Y-%m-%d')}\n"
        f"Check-out Date: {booking.check_out_date.strftime('%Y-%m-%d')}\n\n"
        f"Thank you for choosing {hotel.name}! We look forward to your stay."
    )


        # ✅ Send Email
        if guest.email:
            send_mail(
                subject="🎉 Booking Confirmed – See You Soon!",
                message=message_body,
                from_email=settings.EMAIL_HOST_USER,
                recipient_list=[guest.email],
                fail_silently=False,
            )
            logger.info(f"✅ Email sent to {guest.email}")

        
    # Send SMS using send_sms_bd instead of Twilio
        if guest.phone:
            try:
                response = send_sms_bd(guest.phone, sms_message)
                logger.info(f"📱 SMS sent to {guest.phone}, response: {response.status_code} {response.text}")
            except Exception as sms_error:
                logger.warning(f"⚠️ SMS send failed: {sms_error}")


    except Booking.DoesNotExist:
        logger.error(f"❌ Booking with ID {booking_id} does not exist.")
    except Exception as e:
        logger.exception(f"❌ Unexpected error in payment confirmation task: {e}")




@shared_task
def send_booking_confirmation_message(booking_id):
    import logging
    from django.core.mail import send_mail
    from django.conf import settings

    logger = logging.getLogger(__name__)
    try:
        booking = Booking.objects.select_related("guest", "hotel").get(id=booking_id)
        guest = booking.guest
        hotel = booking.hotel

        logger.info(f"Sending booking confirmation message to {guest.email} / {guest.phone}")

        # Email message body
        email_body = (
            f"Hi {guest.first_name or 'Guest'},\n\n"
            f"🎉 Your booking #{booking.booking_number} at {hotel.name} has been confirmed!\n"
            f"📍 Hotel: {hotel.name}\n"
            f"📅 Check-in Date: {booking.check_in_date.strftime('%Y-%m-%d')}\n"
            f"📅 Check-out Date: {booking.check_out_date.strftime('%Y-%m-%d')}\n"
            f"🛏️ Room Type: {booking.room_type.name}\n"
            f"🛎️ Number of Rooms: {booking.number_of_rooms}\n\n"
            f"Thank you for choosing {hotel.name}. We look forward to hosting you!\n\n"
            f"Warm regards,\n"
            f"The {hotel.name} Team"
        )

        # SMS message
        sms_body = (
            f"Hello {guest.first_name or 'Guest'}, your booking #{booking.booking_number} at {hotel.name} "
            f"has been confirmed. Check-in: {booking.check_in_date.strftime('%Y-%m-%d')}, "
            f"Check-out: {booking.check_out_date.strftime('%Y-%m-%d')}. Thank you!"
        )

        # Send email
        if guest.email:
            send_mail(
                subject=f"Booking Confirmed - #{booking.booking_number}",
                message=email_body,
                from_email=settings.EMAIL_HOST_USER,
                recipient_list=[guest.email],
                fail_silently=False,
            )
            logger.info(f"Booking confirmation email sent to {guest.email}")

        # Send SMS (replace send_sms_bd with your SMS sending function)
        if guest.phone:
            try:
                response = send_sms_bd(guest.phone, sms_body)
                logger.info(f"Booking confirmation SMS sent to {guest.phone}: {response.status_code} {response.text}")
            except Exception as e:
                logger.warning(f"Failed to send SMS: {e}")

    except Booking.DoesNotExist:
        logger.error(f"Booking with ID {booking_id} not found.")
    except Exception as e:
        logger.exception(f"Error in booking confirmation task: {e}")





@shared_task(bind=True)
def process_bulk_inventory_csv_task(self, room_type_id, file_path):
    task_id = self.request.id
    success_count = 0
    batch_size = 500
    batch = []
    errors = []

    try:
        room_type = RoomType.objects.get(id=room_type_id)
    except RoomType.DoesNotExist:
        err = f"RoomType {room_type_id} not found"
        cache.set(f"bulk_inv_status_{task_id}", {"error": err}, timeout=3600)
        logger.error(err)
        return

    try:
        with default_storage.open(file_path, mode='rb') as file_obj:
            file_content = file_obj.read().decode("utf-8")

        reader = list(csv.DictReader(StringIO(file_content)))
        total_rows = len(reader)

        for row_num, row in enumerate(reader, start=1):
            try:
                availability_date = datetime.strptime(row["date"].strip(), "%m/%d/%Y").date()
                available_rooms = int(row["available_rooms"].strip())

                if available_rooms > room_type.total_rooms:
                    raise ValueError(f"Available rooms exceed total rooms ({room_type.total_rooms})")

                price_override_str = row.get("price_override", "").strip()
                price_override = Decimal(price_override_str) if price_override_str else None
                if price_override is not None and price_override < 0:
                    raise InvalidOperation("Negative price override")

                is_special_offer_str = row.get("is_special_offer", "").strip().lower()
                is_special_offer = is_special_offer_str in ("true", "1", "yes")

                special_offer_name = row.get("special_offer_name", "").strip() or None

                min_nights_stay_str = row.get("min_nights_stay", "").strip()
                try:
                    min_nights_stay = int(min_nights_stay_str) if min_nights_stay_str else 1
                    if min_nights_stay < 1:
                        min_nights_stay = 1
                except ValueError:
                    min_nights_stay = 1

                batch.append(
                    RoomAvailability(
                        room_type=room_type,
                        date=availability_date,
                        available_rooms=available_rooms,
                        price_override=price_override,
                        is_special_offer=is_special_offer,
                        special_offer_name=special_offer_name,
                        min_nights_stay=min_nights_stay,
                    )
                )

                if len(batch) >= batch_size:
                    _bulk_upsert_availability(batch)
                    success_count += len(batch)
                    batch.clear()

                    progress = (row_num / total_rows) * 100
                    cache.set(
                        f"bulk_inv_status_{task_id}",
                        {
                            "progress": round(progress, 2),
                            "success": success_count,
                            "errors": errors,
                        },
                        timeout=3600,
                    )

            except Exception as e:
                error_msg = f"Row {row_num}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        if batch:
            _bulk_upsert_availability(batch)
            success_count += len(batch)

        cache.set(
            f"bulk_inv_status_{task_id}",
            {
                "progress": 100,
                "success": success_count,
                "errors": errors,
            },
            timeout=3600,
        )

        logger.info(f"Task {task_id} completed: {success_count} records saved.")

    except Exception as e:
        logger.error(f"Critical error in task {task_id}: {str(e)}", exc_info=True)
        cache.set(f"bulk_inv_status_{task_id}", {"error": str(e)}, timeout=3600)
        raise e


def _bulk_upsert_availability(batch):
    if not batch:
        return

    room_type = batch[0].room_type
    dates = [obj.date for obj in batch]

    existing_qs = RoomAvailability.objects.filter(room_type=room_type, date__in=dates)
    existing_map = {obj.date: obj for obj in existing_qs}

    to_update = []
    to_create = []

    for obj in batch:
        if obj.date in existing_map:
            existing = existing_map[obj.date]
            existing.available_rooms = obj.available_rooms
            existing.price_override = obj.price_override
            existing.is_special_offer = obj.is_special_offer
            existing.special_offer_name = obj.special_offer_name
            existing.min_nights_stay = obj.min_nights_stay
            to_update.append(existing)
        else:
            to_create.append(obj)

    with transaction.atomic():
        if to_update:
            RoomAvailability.objects.bulk_update(
                to_update,
                fields=[
                    "available_rooms",
                    "price_override",
                    "is_special_offer",
                    "special_offer_name",
                    "min_nights_stay",
                ],
                batch_size=1000,
            )
        if to_create:
            RoomAvailability.objects.bulk_create(to_create, batch_size=1000)



from datetime import timedelta, date, time
from hotel.models import RoomType, RoomHourlyAvailability

@shared_task
def create_hourly_availability_for_room_type(room_type_id):
    try:
        room_type = RoomType.objects.get(id=room_type_id)
    except RoomType.DoesNotExist:
        logger.error(f"❌ RoomType with ID {room_type_id} does not exist.")
        return
    
    today = date.today()
    # Generate all possible hourly slots for next 365 days
    availability_data = []
    for i in range(365):  # Next year
        availability_date = today + timedelta(days=i)
        for hour in range(24):
            start_time = time(hour, 0)
            if hour == 23:
                end_time = time(23, 59, 59)
            else:
                end_time = time(hour + 1, 0)
            availability_data.append(RoomHourlyAvailability(
                room_type=room_type,
                date=availability_date,
                start_time=start_time,
                end_time=end_time,
                available_rooms=room_type.total_rooms,
            ))
            
     # Fetch existing entries to avoid duplicates
    existing_slots = RoomHourlyAvailability.objects.filter(
        room_type=room_type,
        date__gte=today,
        date__lte=today + timedelta(days=365)
    ).values_list("date", "start_time", "end_time")

    existing_set = set(existing_slots)

    safe_data = [
        slot for slot in availability_data
        if (slot.date, slot.start_time, slot.end_time) not in existing_set
    ]

    if safe_data:
        RoomHourlyAvailability.objects.bulk_create(safe_data, batch_size=1000)
        logger.info(f"✅ Created {len(safe_data)} new hourly availability records for RoomType {room_type.id}")
    else:
        logger.info(f"⚠️ No new availability entries were added for RoomType {room_type.id} (all already exist)")
