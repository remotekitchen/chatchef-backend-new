from django.db.models.signals import post_save
from django.dispatch import receiver

from billing.clients.doordash_client import DoordashClient
from food.models import (Image, Location, ModifierGroupOrder,
                         ModifiersItemsOrder, Restaurant)
from food.tasks import task_save_remote_image
from decimal import Decimal

# @receiver(post_save, sender=Image)
# def save_remote_image(sender, instance: Image, created, **kwargs):
#     if not created:
#         return
#     try:
#         task_save_remote_image.delay(instance.id)
#     except:
#         pass


@receiver(post_save, sender=Restaurant)
def create_doordash_store_restaurant(sender, instance: Restaurant, created, **kwargs):
    discount = Decimal(instance.discount_percentage or 0)

    for item in instance.menuitem_set.all():
        # Ensure original_price is never modified
        if not item.original_price:  # Set original_price if not already set (only for the first time)
            item.original_price = Decimal(item.base_price or 0.00)  # Ensure this is a Decimal

        original_price = item.original_price 
        if discount == 0:
            item.base_price = original_price  # Set base_price back to original if no discount
            item.discounted_price = Decimal("0.00")  # No discount applied
        else:
            # Apply discount calculation based on original_price
            discounted_value = original_price * discount / Decimal("100.00")  # Ensure the calculation uses Decimal
            item.base_price = original_price - discounted_value  # Adjust base price for discount
            item.discounted_price = original_price - discounted_value  # Store discounted price

        item.save(update_fields=["base_price", "discounted_price", "original_price"])
        
    if not created:
        return

    doordash = DoordashClient()
    created_store = doordash.create_store(
        instance.name, instance.location, instance.phone)
    if created_store.status_code == 200:
        data = created_store.json()
        instance.doordash_external_store_id = data.get('external_store_id')
        instance.save(update_fields=['doordash_external_store_id'])
    


@receiver(post_save, sender=Location)
def create_doordash_store_location(sender, instance: Location, created, **kwargs):
    if not created:
        return

    doordash = DoordashClient()
    created_store = doordash.create_store(
        instance.restaurant.name, instance.details, instance.phone)
    if created_store.status_code == 200:
        data = created_store.json()
        instance.doordash_external_store_id = data.get('external_store_id')
        instance.save(update_fields=['doordash_external_store_id'])


@receiver(post_save, sender=ModifierGroupOrder)
def update_modifiers_group_order(sender, instance: ModifierGroupOrder, created, **kwargs):
    if not created:
        return

    obj = instance

    count = ModifierGroupOrder.objects.filter(
        menu_item=obj.menu_item).order_by('order').last().order if ModifierGroupOrder.objects.filter(
        menu_item=obj.menu_item).exists() else 0

    obj.order = count + 1
    obj.save()
    print('line 59 --> ', obj.order)


@receiver(post_save, sender=ModifiersItemsOrder)
def update_modifiers_item_order(sender, instance: ModifiersItemsOrder, created, **kwargs):
    if not created:
        return

    obj = instance

    count = ModifiersItemsOrder.objects.filter(
        modifier_item=obj.modifier_item).order_by('order').last().order if ModifiersItemsOrder.objects.filter(
        modifier_item=obj.modifier_item).exists() else 0

    obj.order = count + 1
    obj.save()
    print('order is --> ', obj.order)
