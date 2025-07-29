import time
from celery import shared_task
from django.db import transaction
from django.core.cache import cache

import requests
from django.core.files import File
from django.core.files.temp import NamedTemporaryFile
import datetime

from hungrytiger.celery import app
from core.utils import get_logger
from food.models import Image, Location, MenuItem, ModifierGroup
import pytz
from django.utils import timezone
from django_celery_beat.models import ClockedSchedule, PeriodicTask, IntervalSchedule, CrontabSchedule
import json
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver

logger = get_logger()


@app.task(name="chatchef.save_remote_image")
def task_save_remote_image(image_id):
    instance = Image.objects.get(id=image_id)
    if not instance.local_url or instance.local_url == "":
        try:
            response = requests.get(instance.remote_url)

            # Generate a temporary file name
            img_temp = NamedTemporaryFile(delete=True)

            # Write the image content to the temporary file
            img_temp.write(response.content)
            img_temp.flush()

            # Create a Django file object from the temporary file
            django_file = File(img_temp)

            # Save the file to the ImageField
            instance.local_url.save(
                f"{int(time.time())}.jpg", django_file, save=True)
        except Exception as e:
            logger.error(f"Could not save image: {e}")


      # test comment

@app.task(name="chatchef.menu_item_unavailable_for_today")
def menu_item_unavailable_for_today(pk):
    try:
        with transaction.atomic():
            query_set = MenuItem.objects.filter(id=pk)

            if not query_set.exists():
                return f"Item with ID {pk} not found"

            for item in query_set:
                item.is_available = True
                item.is_available_today = True
                item.save(update_fields=["is_available", "is_available_today"])

            cache_key = f"menu_item_{pk}"  # Adjust this based on your caching setup
            cache.delete(cache_key)  # Clear cache

        return "Updated Successfully"
    except Exception as error:
        return f"Failed: {error}"

# Demo testing
# @shared_task(name="chatchef.menu_item_unavailable_for_today")
# def menu_item_unavailable_for_today(pks):
#     try:
#         current_time = datetime.timezone.now()  # Get current time
#         print(f"[{current_time}] Marking items unavailable for pks: {pks}")
#         # Fetch menu items based on the list of primary keys (pks)
#         query_set = MenuItem.objects.filter(id__in=pks)
 
#         # Update fields for unavailable items
#         for item in query_set:
#             if not item.is_available_today:
#                 item.is_available = False
#                 item.is_available_today = False
        
#         # Bulk update the fields in the database
#         MenuItem.objects.bulk_update(query_set, ["is_available", "is_available_today"])

#         # Schedule the re-enable task for each pk after 5 minutes
#         for pk in pks:
#             enable_item_in_5_minutes.apply_async((pk,), countdown=300)  # 300 seconds = 5 minutes
#             print(f"[{datetime.timezone.now()}] Scheduled re-enable task for item with pk: {pk}")
        
#         return "updated"
#     except Exception as error:
#         print(f"[{datetime.timezone.now()}] Error occurred: {error}")
#         return f"failed {error}"
      
# @shared_task(name="chatchef.enable_item_in_5_minutes")
# def enable_item_in_5_minutes(pk):
#     try:
#         current_time = datetime.timezone.now()  # Get current time
#         print(f"[{current_time}] Re-enabling item with pk: {pk}")
#         # Fetch the menu item by primary key
#         query_set = MenuItem.objects.filter(id=pk)

#         # Re-enable the item
#         for item in query_set:
#             item.is_available = True
#             item.is_available_today = True

#         # Bulk update the item in the database
#         MenuItem.objects.bulk_update(query_set, ["is_available", "is_available_today"])
#         print(f"[{datetime.timezone.now()}] Item with pk: {pk} has been re-enabled")
#         return "item re-enabled"
#     except Exception as error:
#         print(f"[{datetime.timezone.now()}] Error occurred while re-enabling item with pk {pk}: {error}")
#         return f"failed to re-enable item {error}"


@app.task(name="chatchef.location_scheduled_pause_unpause")
def location_scheduled_pause_unpause(**kwargs):
    ids = kwargs.get('ids')
    state = kwargs.get('state')

    try:
        query_set = Location.objects.filter(id__in=ids)

        for location in query_set:
            location.is_location_closed = not state
        Location.objects.bulk_update(query_set, ["is_location_closed"])
        print("updated")
    except Exception as error:
        pass


@app.task(name="chatchef.location_scheduled_unpause")
def location_scheduled_unpause(**kwargs):
    pk = kwargs.get('pk')

    try:
        location = Location.objects.get(id=pk)

        if location.is_location_closed:
            location.is_location_closed = False
            location.save()
            return "location unpaused"
        else:
            return "location already unpaused"
    except Exception as error:
        pass


@app.task(name="chatchef.modifier_available")
def modifier_available(**kwargs):
    pk = kwargs.get('pk')

    try:
        modifier = ModifierGroup.objects.get(id=pk)

        if not modifier.is_available_today:
            modifier.is_available_today = True
            modifier.is_available = True
            modifier.save()
            return "modifier available"
        else:
            return "location already available"
    except Exception as error:
        pass

