from django.db.models.signals import post_save
from django.dispatch import receiver
from datetime import timedelta, date
from hotel.models import RoomType, RoomAvailability
from datetime import timedelta, date, time
from hotel.models import RoomType, RoomHourlyAvailability
from hotel.tasks import create_hourly_availability_for_room_type
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

@receiver(post_save, sender=RoomType)
def create_room_availability(sender, instance, created, **kwargs):
    if created:
        today = date.today()
        # Daily availability
        for i in range(365):
            availability_date = today + timedelta(days=i)
            RoomAvailability.objects.get_or_create(
                room_type=instance,
                date=availability_date,
                defaults={"available_rooms": instance.total_rooms}
            )
        # Hourly availability via background task
        if instance.is_hourly:
            create_hourly_availability_for_room_type.delay(instance.id)

from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from .models import Hotel, RoomAvailability, Booking
from hotel.utils.helpers import increment_cache_version

@receiver(post_save, sender=Hotel)
@receiver(post_delete, sender=Hotel)
@receiver(post_save, sender=RoomAvailability)
@receiver(post_delete, sender=RoomAvailability)
@receiver(post_save, sender=Booking)
@receiver(post_delete, sender=Booking)
def hotel_related_change(sender, instance, **kwargs):
    increment_cache_version()
