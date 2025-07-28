import uuid

from django.contrib.auth import get_user_model
from django.db import models
from django.utils.translation import gettext as _

from accounts.models import Company
from billing.models import Order
from core.models import BaseModel
from food.models import Location, MenuItem, Restaurant
from django.utils.timezone import now

User = get_user_model()


class Cuisine(BaseModel):
    title = models.CharField(max_length=100, verbose_name=_("title"))
    image = models.FileField(
        upload_to="remotekitchen/files/%Y/%m/%d/",
        blank=True,
        null=True,
        verbose_name=_("image")
    )

    def __str__(self):
        return f"{self.title}"

    class Meta:
        ordering = ["-id"]
        verbose_name = _('Cuisine')
        verbose_name_plural = _('Cuisines')


class GroupOrderItem(BaseModel):
    restaurant = models.OneToOneField(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    items = models.ManyToManyField(
        MenuItem, blank=True, verbose_name=_("items"))

    def __str__(self):
        return f"{self.restaurant.name}"


class GroupOrder(BaseModel):
    orders = models.ManyToManyField(
        Order, blank=True, verbose_name=_("confirm orders"))
    group_start_at = models.DateTimeField(
        auto_now_add=True, verbose_name=_("group order started at"))
    group_end_at = models.DateTimeField(
        blank=True, null=True, verbose_name=_("group order end at"))
    joined_users = models.ManyToManyField(
        User, blank=True, verbose_name=_("joined users"))

    company = models.ForeignKey(
        Company, verbose_name=_("company"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )
    invite_code = models.UUIDField(default=uuid.uuid4, unique=True)
    is_taking_order = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.id}"

    class Meta:
        ordering = ["-id"]


class SearchKeyword(BaseModel):
  keyword = models.CharField(max_length=255, verbose_name=_("keyword"))
  user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("created at"))
  result_count = models.IntegerField(default=0, verbose_name=_("result count"))
  
  def __str__(self):
    return f"{self.keyword}"
  




class Favorite(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='favorites')
    item = models.ForeignKey(MenuItem, on_delete=models.CASCADE, related_name='favorites')
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'item')  # Ensures a user can save an item only once

    def __str__(self):
        return f"{self.user.username} - {self.item.name}"
    



# class UserAddressRK(models.Model):
#     user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="addresses")  # User association
#     label = models.CharField(max_length=50)
#     contact_person = models.CharField(max_length=100)
#     gender = models.CharField(max_length=10, choices=[("male", "Male"), ("female", "Female")])
#     phone = models.CharField(max_length=15)
#     address = models.TextField()
#     place_id = models.CharField(max_length=255)
#     is_default = models.BooleanField(default=False)

#     created_at = models.DateTimeField(auto_now_add=True)
#     updated_at = models.DateTimeField(auto_now=True)

#     def __str__(self):
#         return f"{self.label} - {self.contact_person} ({self.user.username})"



class FavoriteRestaurant(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='restaurant_bookmarks')
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE, related_name='bookmarks')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'restaurant')  

    def __str__(self):
        return f"{self.user.username} - {self.restaurant.name}"
    



class Countdown(models.Model):
    start_time = models.DateTimeField(default=now)
    end_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)




class DeliveryFeeRule(models.Model):
    restaurants = models.ManyToManyField(Restaurant, blank=True)  
    first_order_fee = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    second_order_fee = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    third_or_more_fee = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    def __str__(self):
        restaurant_names = ", ".join([restaurant.name for restaurant in self.restaurants.all()])
        return f"Fee Rules for {restaurant_names if restaurant_names else 'All Restaurants'}"