import re
import uuid

from django.contrib.postgres.fields import ArrayField
from django.contrib.sites.models import Site
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from accounts.models import Company, User
from core.models import Address, BaseModel, ExtraData, SluggedModel
from django.db.models import Avg
from decimal import Decimal
import pytz
from datetime import datetime



class TimeTable(BaseModel):
    start_time = models.TimeField(verbose_name=_("Start time"))
    end_time = models.TimeField(verbose_name=_("End time"))

    opening_hour = models.ForeignKey(
        "OpeningHour",
        verbose_name=_("Opening_hour"),
        on_delete=models.CASCADE,
        related_name="opening_hour",
    )

    class Meta:
        verbose_name = _("TimeTable")
        verbose_name_plural = _("TimeTables")

    def __str__(self):
        return f"TimeTable {self.start_time}--{self.end_time}"


class POS_DATA(BaseModel):
    """
    Store point of sale (POS) identifier details here. Whenever an order, menu, or category needs to be created in
    the POS system, store their identifying details here. Additionally, this information should be linked with menus,
    menu items, categories, modifier groups, and timetables/opening hours.

    """

    choices = (("Clover", "Clover"), ("Toast", "Toast"))
    pos_type = models.CharField(
        max_length=50, verbose_name=_("pos company"), choices=choices
    )
    external_id = models.CharField(
        max_length=100, verbose_name=_("external data id")
    )

    def __str__(self) -> str:
        return f"{self.pos_type} --> {self.external_id}"


class OpeningHour(BaseModel):
    class DayType(models.TextChoices):
        MON = "mon", _("MON")
        TUE = "tue", _("TUE")
        WED = "wed", _("WED")
        THU = "thu", _("THU")
        FRI = "fri", _("FRI")
        SAT = "sat", _("SAT")
        SUN = "sun", _("SUN")
        ALL = "all", _("ALL")

    day_index = models.CharField(
        max_length=10, verbose_name=_("Day Index"), choices=DayType.choices
    )

    is_close = models.BooleanField(
        verbose_name=_("Opening Status"), default=False
    )

    class Meta:
        verbose_name = _("Opening Hour")
        verbose_name_plural = _("Opening Hours")

    def __str__(self):
        return self.day_index


class SpecialHour(BaseModel):
    date = models.DateField(blank=True, null=True)
    opens_at = models.TimeField(
        verbose_name=_(
            "Opens at"
        ), blank=True, null=True
    )
    closes_at = models.TimeField(
        verbose_name=_(
            "Closes at"
        ), blank=True, null=True
    )
    is_closed = models.BooleanField(default=False, blank=True)

    def __str__(self) -> str:
        return f"{self.date}::{self.opens_at}::{self.closes_at}"


class OrderMethod(models.TextChoices):
    # SCHEDULED = 'scheduled', _('SCHEDULED')
    DELIVERY = 'delivery', _('DELIVERY')
    RESTAURANT_DELIVERY = 'restaurant_delivery', _('RESTAURANT_DELIVERY')
    PICKUP = 'pickup', _('PICKUP')
    DINE_IN = 'dine_in', _('DINE_IN')


class PaymentMethod(models.TextChoices):
    STRIPE = 'stripe', _('STRIPE')
    CARD = 'card', _('CARD')
    PAYPAL = 'paypal', _('PAYPAL')
    CASH = 'cash', _('CASH')
    WALLET = 'wallet', _('WALLET')


def get_order_method_default():
    return [key[0] for key in OrderMethod.choices]


def get_payment_method_default():
    return [key[0] for key in PaymentMethod.choices]


class RemoteKitchenCuisine(models.Model):
    name = models.CharField(max_length=50, verbose_name="Cuisine Name")
    icon = models.ImageField(upload_to="cuisine_icons/%Y/%m/%d/", blank=True, null=True, verbose_name="Cuisine Icon")

    def __str__(self):
        return f"{self.name}"


class Restaurant(SluggedModel):
    TIME_ZONE_CHOICES = (
        ("-12:00", "-12:00"),
        ("-11:00", "-11:00"),
        ("-10:00", "-10:00"),
        ("-09:00", "-09:00"),
        ("-08:00", "-08:00"),
        ("-07:00", "-07:00"),
        ("-06:00", "-06:00"),
        ("-05:00", "-05:00"),
        ("-04:00", "-04:00"),
        ("-03:00", "-03:00"),
        ("-02:00", "-02:00"),
        ("-01:00", "-01:00"),
        ("+00:00", "+00:00"),
        ("+01:00", "+01:00"),
        ("+02:00", "+02:00"),
        ("+03:00", "+03:00"),
        ("+04:00", "+04:00"),
        ("+05:00", "+05:00"),
        ("+06:00", "+06:00"),
        ("+07:00", "+07:00"),
        ("+08:00", "+08:00"),
        ("+09:00", "+09:00"),
        ("+10:00", "+10:00"),
        ("+11:00", "+11:00"),
        ("+12:00", "+12:00"),
    )
    PAYMENT_ACCOUNTS = (
      ('chatchef', 'ChatChef'),
      ('techchef', 'TechChef'),
    )
    PRIORITY_CHOICES = (
        (1, "Priority 1"),
        (2, "Priority 2"),
    )
    STORE_TYPE_CHOICES = (
        ('restaurant', 'Restaurant'),
        ('salons', 'Salons'),
        ('hotels', 'Hotels'),
        ('medicine', 'Medicine'),
    )   
    name = models.CharField(max_length=255, verbose_name=_("Name"))
    owner = models.ForeignKey(
        User, verbose_name=_("Owner"), on_delete=models.SET_NULL, null=True
    )
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.SET_NULL, null=True
    )
    opening_hours = models.ManyToManyField(
        OpeningHour, verbose_name=_("Opening Hours"), blank=True
    )
    location = models.TextField(verbose_name=_("Location"), blank=True)
    address = models.ForeignKey(
        Address, verbose_name=_(
            'Address'
        ), on_delete=models.SET_NULL, null=True, blank=True
    )
    avatar_image = models.OneToOneField(
        "Image",
        verbose_name=_("Avatar"),
        related_name="avatar_of_restaurant",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    banner_image = models.ManyToManyField(
        "Image",
        verbose_name=_("Banner"),
        related_name="banner_of_restaurant",
        blank=True,
    )
    payment_details = models.ForeignKey(
        "billing.PaymentDetails",
        verbose_name=_("Payment Details"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    phone = models.CharField(
        max_length=20, verbose_name=_("Phone number"), blank=True
    )
    email = models.EmailField(verbose_name=_("Email"), blank=True)
    doordash_external_store_id = models.CharField(
        max_length=100, verbose_name=_("Doordash external store ID"), blank=True
    )

    is_store_close = models.BooleanField(
        default=False, verbose_name=_("otter store closed")
    )

    timezone = models.CharField(
        max_length=10, default="-08:00", choices=TIME_ZONE_CHOICES
    )
    inflation_percent = models.PositiveIntegerField(
        default=0, verbose_name=_("inflation percent")
    )
    reward_point_equivalent = models.FloatField(
        verbose_name=_('Reward point equivalent'), default=1
    )
    bag_price = models.FloatField(default=0, verbose_name=_('bag price'))
    utensil_price = models.FloatField(
        default=0, verbose_name=_('utensil price')
    )
    use_bag_price_on_delivery = models.BooleanField(
        default=False, verbose_name=_('add bag price on delivery')
    )
    order_methods = ArrayField(
        models.CharField(
            max_length=30, verbose_name=_(
                'Order types'
            ), choices=OrderMethod.choices
        ),
        blank=True,
        default=get_order_method_default
    )
    payment_methods = ArrayField(
        models.CharField(
            max_length=30, verbose_name=_(
                'Payment methods'
            ), choices=PaymentMethod.choices
        ),
        blank=True,
        default=get_payment_method_default
    )
    payment_methods_pickup = ArrayField(
        models.CharField(
            max_length=30, verbose_name=_(
                'Payment methods'
            ), choices=PaymentMethod.choices
        ),
        blank=True,
        default=get_payment_method_default
    )
    stripe_fee = models.BooleanField(
        verbose_name=_('Stripe fee'), default=False)
    receive_call_for_order = models.BooleanField(default=False)
    service_fee = models.FloatField(default=0.99)
    use_delivery_inflation = models.BooleanField(default=False)
    accept_scheduled_order = models.BooleanField(default=True)
    latitude = models.FloatField(default=0)
    longitude = models.FloatField(default=0)
    delivery_fee = models.FloatField(
        default=0, verbose_name=_("default delivery fee"))

    slug_keyword_field = "name"

    logo = models.ImageField(
        upload_to="logos/%Y/%m/%d/", verbose_name=_("Logo"), blank=True, null=True, default=None
    )
    restaurant_banner =  models.ImageField(
        upload_to="restaurant_banners/%Y/%m/%d/", verbose_name=_("Restaurant_banner"), blank=True, null=True, default=None
    )
    average_rating = models.FloatField(default=0.0)
    cuisines = models.ManyToManyField(RemoteKitchenCuisine, related_name="restaurants", blank=True)


    is_remote_Kitchen = models.BooleanField(
        default=False, verbose_name=_("is remote kitchen")
    )
    is_chatchef_bd = models.BooleanField(
        default=False, verbose_name=_("is chatchef bd")
    )
    accept_first_second_third_user_reward = models.BooleanField(
        default=False,
        verbose_name="Accept First, Second, and Third User Rewards",
        help_text="Allow the creation of first, second, and third order rewards for users."
    )
    payment_account = models.CharField(
        max_length=50, choices=PAYMENT_ACCOUNTS, default='chatchef',
        help_text="Select the payment account to use for this restaurant."
    )
    
    voucher_restriction = models.BooleanField(
        default=True,
        verbose_name="Voucher Restriction",
        help_text="Restrict the use of vouchers with other vouchers to this restaurant."
    )
    discount_percentage = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0,
        verbose_name=_("restaurant discount percentage"),
        help_text="Percentage discount applied to all menu items of this restaurant"
    )
    store_type = models.CharField(
        max_length=50,
        choices=STORE_TYPE_CHOICES,
        default='restaurant',
        verbose_name=_('Store Type'),
        help_text="Select the type of store."
    )
    auto_accept_orders = models.BooleanField(
        verbose_name=_('Auto Accept Orders'), default=False)
    
    boosted_monthly_sales_count = models.PositiveIntegerField(
        default=0,
        verbose_name=_("Boosted monthly sales count"),
    )

    boosted_total_sales_count = models.PositiveIntegerField(
        default=0,
        verbose_name=_("Boosted total sales count"),
    )

    boosted_average_ticket_size = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        default=0.00,
        verbose_name=_("Boosted average ticket size"),
    )

    boosted_total_gross_revenue = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0.00,
        verbose_name=_("Boosted total gross revenue"),
    )

    priority = models.PositiveSmallIntegerField(
        choices=PRIORITY_CHOICES,
        default=1,
        verbose_name="Restaurant Boost Priority",
    )


    class Meta:
        verbose_name = _("Restaurant")
        verbose_name_plural = _("Restaurants")
        ordering = ["-id"]


    def update_average_rating(self):
        from marketing.models import Review  # Import inside method to avoid circular import
        avg_rating = Review.objects.filter(restaurant=self).aggregate(Avg('rating'))['rating__avg']
        self.average_rating = avg_rating if avg_rating is not None else 0.0
        self.save()
        
    def apply_inflation_to_menu_items(self):
        """
        Applies inflation to the restaurant's menu items if `use_delivery_inflation` is True.
        """
        if self.use_delivery_inflation:
            menu_items = MenuItem.objects.filter(restaurant=self.pk)
            for item in menu_items:
                item.virtual_price = round(
                    (item.base_price / (1 - (self.inflation_percent / 100))), 2
                )
            # Bulk update the menu items to reflect the new virtual prices
            MenuItem.objects.bulk_update(menu_items, ["virtual_price"])

    def clean_timezone(self):
        timezone = self.timezone.strip()
        if not timezone.startswith("+") and not timezone.startswith("-"):
            raise ValidationError("Time zone must start with '+' or '-'")
        if not re.match(r"^[+-]\d{2}:\d{2}$", timezone):
            raise ValidationError(
                "Invalid time zone format. Use '+00:00' format."
            )
        return timezone

    def save(self, *args, **kwargs):
        self.timezone = self.clean_timezone()
        
        # Apply delivery inflation if needed
        if self.use_delivery_inflation:
            self.apply_inflation_to_menu_items()
        
        # Set the timezone to +6 if it's a remote kitchen
        if self.is_remote_Kitchen:
            self.timezone = '+06:00'
        
        # Save the model normally
        super(Restaurant, self).save(*args, **kwargs)


    def __str__(self):
        return self.name



class Location(SluggedModel):
    name = models.TextField(verbose_name=_("Name"))
    details = models.TextField(verbose_name=_("Details"), blank=True)
    address = models.ForeignKey(
        Address, verbose_name=_(
            'Address'
        ), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant,
        verbose_name=_("Restaurant"),
        related_name="locations",
        on_delete=models.CASCADE,
    )
    opening_hours = models.ManyToManyField(
        OpeningHour, verbose_name=_("Hours"), blank=True
    )
    direct_order = models.BooleanField(
        verbose_name=_("Direct order"), default=False
    )
    phone = models.CharField(
        max_length=20, verbose_name=_("Phone number"), blank=True
    )
    email = models.EmailField(verbose_name=_("Email"), blank=True)
    doordash_external_store_id = models.CharField(
        max_length=100, verbose_name=_("Doordash external store ID"), blank=True
    )

    otter_x_store_id = models.CharField(
        max_length=100, verbose_name=_("otter store id"), blank=True, null=True
    )
    otter_x_event_id = models.CharField(
        max_length=100, verbose_name=_("otter event id"), blank=True, null=True
    )
    is_menu_importing = models.BooleanField(
        default=False, verbose_name=_("otter menu importing")
    )
    is_menu_imported = models.BooleanField(
        default=False, verbose_name=_("otter menu imported successfully")
    )
    is_location_closed = models.BooleanField(
        default=False, verbose_name=_("location closed")
    )
    showing = models.PositiveIntegerField(default=1, verbose_name=_("sorting"))
    use_third_party_do = models.BooleanField(default=False)
    third_party_do = models.TextField(blank=True, null=True)
    latitude = models.CharField(max_length=20, default="0.0")
    longitude = models.CharField(max_length=20, default="0.0")

    slug_keyword_field = "name"

    class Meta:
        ordering = ["showing", "-id"]
        verbose_name = _("Location")
        verbose_name_plural = _("Locations")

    def __str__(self):
        return f"{self.restaurant.name} :: {self.name}"


class CuisineType(BaseModel):
    name = models.CharField(max_length=255, verbose_name=_("Name"))
    description = models.TextField(verbose_name=_("Description"), blank=True)

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Cuisine Type")
        verbose_name_plural = _("Cuisine Types")

    def __str__(self):
        return self.name


class Menu(SluggedModel):
    title = models.CharField(max_length=255, verbose_name=_("Title"))
    locations = models.ManyToManyField(
        Location, verbose_name=_("Location"), blank=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    description = models.TextField(verbose_name=_("Description"), blank=True)
    cuisine_types = models.ManyToManyField(
        CuisineType, verbose_name=_("Cuisine Types"), blank=True
    )
    opening_hours = models.ManyToManyField(
        OpeningHour, verbose_name=_("Opening Hours"), blank=True
    )
    special_hours = models.ManyToManyField(
        SpecialHour, verbose_name=_("Special Hours"), blank=True
    )
    otter_menu_id = models.CharField(
        max_length=100, verbose_name=_("otter menu id"), blank=True
    )

    pos_identifier = models.ManyToManyField(
        POS_DATA, verbose_name=_("pos data"), blank=True
    )

    inflation_percent = models.PositiveIntegerField(
        verbose_name=_("inflation percent"), default=0
    )

    slug_keyword_field = "title"
    showing = models.PositiveIntegerField(default=1)
    disabled = models.BooleanField(default=False, verbose_name=_('disabled'))
    modifiers_show_reverse = models.BooleanField(
        default=False, verbose_name=_('modifiers show reverse'))

    class Meta:
        verbose_name = _("Menu")
        verbose_name_plural = _("Menus")
        ordering = ["showing", "-id"]

    def __str__(self):
        return f"{self.restaurant.name} :: {self.title}"


class Category(SluggedModel):
    name = models.CharField(max_length=200, verbose_name=_("Name"))
    description = models.TextField(verbose_name=_("Description"), blank=True)
    menu = models.ForeignKey(
        Menu, verbose_name=_("Menu"), on_delete=models.SET_NULL, null=True
    )
    shared_with = models.ManyToManyField(
        Menu, verbose_name=_("shared with"), blank=True, related_name="categories_list"
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    locations = models.ManyToManyField(
        Location, verbose_name=_("Location"), blank=True
    )
    otter_category_id = models.CharField(
        max_length=100, verbose_name=_("otter category id"), blank=True, null=True
    )

    show_in_overview = models.BooleanField(
        verbose_name=_("Show in overview"), default=True
    )
    image = models.ForeignKey(
        "Image",
        verbose_name=_("Image"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )

    pos_identifier = models.ManyToManyField(
        POS_DATA, verbose_name=_("pos data"), blank=True
    )
    disabled = models.BooleanField(default=False, verbose_name=_("disabled"))
    showing = models.PositiveIntegerField(default=1)

    slug_keyword_field = "name"

    class Meta:
        ordering = ["showing", "id"]
        verbose_name = _("Category")
        verbose_name_plural = _("Categories")

    def __str__(self):
        return self.name


# def save_to_dir(instance, filename):
#     # file will be uploaded to MEDIA_ROOT / user_<id>/<filename>
#     return f'{timezone.now().date().year}/{filename}'


class Image(BaseModel):
    remote_url = models.TextField(verbose_name=_("Remote URL"), blank=True)
    local_url = models.ImageField(
        upload_to="images/%Y/%m/%d/", verbose_name=_("Local URL"), blank=True, null=True
    )
    otter_image_id = models.CharField(
        max_length=255, verbose_name=_("otter image id"), blank=True
    )

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Image")
        verbose_name_plural = _("Images")

    @property
    def working_url(self):
        try:
            if self.local_url and len(self.local_url) > 0:
                return f"{self.local_url.url}"
        except:
            pass
        return self.remote_url

def to_decimal(value):
        return value if isinstance(value, Decimal) else Decimal(str(value))

class MenuItem(SluggedModel):
    from food.managers import MenuItemManager

    name = models.CharField(max_length=255, verbose_name=_("Name"))
    description = models.TextField(verbose_name=_("Description"), blank=True)
    extra_names = models.ManyToManyField(
        ExtraData,
        verbose_name=_("Extra names"),
        related_name=_("name_of_menu"),
        blank=True,
    )
    extra_descriptions = models.ManyToManyField(
        ExtraData,
        verbose_name=_("Extra descriptions"),
        related_name=_("description_of_menu"),
        blank=True,
    )
    menu = models.ForeignKey(
        Menu, verbose_name=_("Menu"), on_delete=models.CASCADE, blank=True, null=True
    )

    base_price = models.FloatField(verbose_name=_("Base price"), default=0)
    virtual_price = models.FloatField(
        verbose_name=_("Virtual price"), default=0, blank=True
    )
    currency = models.CharField(
        max_length=200, verbose_name=_("Currency"), default="USD"
    )

    images = models.ManyToManyField(
        Image, verbose_name=_("Images"), blank=True
    )
    original_image = models.ForeignKey(
        Image,
        verbose_name=_("Original image"),
        related_name="original_image_of",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    category = models.ManyToManyField(
        Category, verbose_name=_("Category"), blank=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    locations = models.ManyToManyField(
        Location, verbose_name=_("Location"), blank=True
    )
    is_available = models.BooleanField(
        verbose_name="Is Available", default=True
    )
    is_available_today = models.BooleanField(
        verbose_name="Is Available Today", default=True
    )
    is_vegan = models.BooleanField(verbose_name="Is Vegan", default=False)
    is_alcoholic = models.BooleanField(
        verbose_name="Is alcoholic", default=False
    )
    is_vegetarian = models.BooleanField(
        verbose_name="Is Vegetarian", default=False
    )
    is_glutenfree = models.BooleanField(
        verbose_name="Is Glutenfree", default=False
    )
    have_nuts = models.BooleanField(verbose_name="Have Nuts", default=False)
    otter_item_id = models.CharField(
        max_length=100, verbose_name=_("otter item id"), blank=True, null=True
    )
    pos_identifier = models.ManyToManyField(
        POS_DATA, verbose_name=_("pos data"), blank=True
    )
    disabled = models.BooleanField(
        default=False, verbose_name="is menu item disabled"
    )
    available_start_time = models.TimeField(
        verbose_name="Available Start Time",
        blank=True, null=True,
        help_text="Time when the item becomes available"
    )
    available_end_time = models.TimeField(
        verbose_name="Available End Time",
        blank=True, null=True,
        help_text="Time when the item is no longer available"
    )
    showing = models.PositiveIntegerField(default=1)
    rating = models.IntegerField(default=0, verbose_name=_('ratings'))
    allow_group_order = models.BooleanField(default=False)
    original_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        default=Decimal("0.00"),
        blank=True,
        null=True,
        verbose_name=_("Original Prices"),
        help_text="Initial price of the item before any discount"
    )
    discounted_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,      # allow NULL in DB
        blank=True,     # allow blank in forms
        default=Decimal("0.00"),
        verbose_name=_("Discounted Price"),
        help_text="Base price after applying the restaurant discount"
    )


    objects = MenuItemManager()

    slug_keyword_field = "name"

   

    class Meta:
        ordering = ["showing", "-id"]
        verbose_name = _("Menu Item")
        verbose_name_plural = _("Menu Items")


    
  
    def save(self, *args, **kwargs):
        # Convert values safely
        base_price = to_decimal(self.base_price)

        # On create: set original_price once
        if self._state.adding:
            self.original_price = base_price

            # Apply discount to base_price from restaurant
            if self.restaurant and self.restaurant.discount_percentage:
                discount = to_decimal(self.restaurant.discount_percentage)
                self.base_price = self.original_price - (
                    self.original_price * discount / Decimal("100.00")
                )

        # Always calculate discounted_price from original_price
        if self.restaurant and self.original_price:
            discount = to_decimal(getattr(self.restaurant, 'discount_percentage', 0))
            self.discounted_price = self.original_price - (
                self.original_price * discount / Decimal("100.00")
            ) if discount > 0 else Decimal("0.00")

        super().save(*args, **kwargs)
    
    def __str__(self):
        return self.name
    



# class MenuItemExtra(BaseModel):
#     menu_item = models.ForeignKey(MenuItem, verbose_name=_('Menu Item'), on_delete=models.CASCADE)
#     names = models.JSONField(verbose_name=_('Names'), default=list)
#     descriptions = models.JSONField(verbose_name=_('Descriptions'), default=list)
#
#     class Meta:
#         verbose_name = _('Menu Item Extra')
#         verbose_name_plural = _('Menu Item Extras')
#
#     def __str__(self):
#         return self.menu_item.name


class ModifierGroup(SluggedModel):
    class RequirementType(models.TextChoices):
        REQUIRED = "required", _("Required")
        OPTIONAL = "optional", _("Optional")

    name = models.CharField(max_length=255, verbose_name=_("Name"))
    description = models.TextField(verbose_name=_("Description"), blank=True)
    requirement_status = models.CharField(
        max_length=15,
        verbose_name=_("Requirement status"),
        default=RequirementType.OPTIONAL,
        blank=True,
    )
    menu = models.ForeignKey(
        Menu, verbose_name=_("Menu"), on_delete=models.SET_NULL, blank=True, null=True
    )

    modifier_items = models.ManyToManyField(
        MenuItem,
        verbose_name=_("Modifier items"),
        related_name="modifier_items_related",
    )
    used_by = models.ManyToManyField(MenuItem, verbose_name=_("Used by"))

    modifier_limit = models.IntegerField(
        verbose_name=_("Modifier limit"), default=-1, blank=True
    )
    item_limit = models.IntegerField(
        verbose_name=_("Item limit"), default=-1, blank=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    locations = models.ManyToManyField(
        Location, verbose_name=_("Location"), blank=True
    )

    pos_identifier = models.ManyToManyField(
        POS_DATA, verbose_name=_("pos data"), blank=True
    )
    otter_id = models.CharField(
        max_length=255, verbose_name=_("otter identifier"), blank=True
    )
    disabled = models.BooleanField(default=False, verbose_name=_("disabled"))
    is_available = models.BooleanField(
        verbose_name="Is Available", default=True
    )
    is_available_today = models.BooleanField(
        verbose_name="Is Available Today", default=True
    )

    slug_keyword_field = "name"

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Modifier Group")
        verbose_name_plural = _("Modifier Groups")

    def __str__(self):
        return self.name

    def __save__(self, *args, **kwargs):
        if not self.slug:
            self.slug = self.get_slug(self.name, self.uid)
        super().save(*args, **kwargs)


class ModifierGroupOrder(models.Model):
    menu_item = models.ForeignKey(MenuItem, on_delete=models.CASCADE)
    modifier_group = models.ForeignKey(ModifierGroup, on_delete=models.CASCADE)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ('menu_item', 'modifier_group')
        ordering = ['order']

    def __str__(self) -> str:
        return f'menu id {self.menu_item.id} --> modifier id {self.modifier_group.id}'


class ModifiersItemsOrder(models.Model):
    menu_item = models.ForeignKey(MenuItem, on_delete=models.CASCADE)
    modifier_item = models.ForeignKey(
        ModifierGroup, on_delete=models.CASCADE, related_name="modifiers_item_list")
    order = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ('menu_item', 'modifier_item')
        ordering = ['order']

    def __str__(self) -> str:
        return f'menu id {self.menu_item.id} --> modifier id {self.modifier_item.id}'


class RestaurantOMSUsagesTracker(BaseModel):
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_("restaurant")
    )
    location = models.ForeignKey(
        Location, on_delete=models.CASCADE, verbose_name=_("location")
    )
    oms_version = models.CharField(
        max_length=30, verbose_name=_("oms version")
    )
    last_updated = models.DateTimeField(
        auto_now=True, verbose_name=_("last updated")
    )

    def __str__(self) -> str:
        return f"{self.restaurant.name} {self.location.name} using {self.oms_version}"

    class Meta:
        ordering = ["-id"]



class VisitHistory(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    date = models.DateField()

    class Meta:
        unique_together = ('user', 'restaurant', 'date')


class ItemVisitHistorySingle(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    item = models.ForeignKey(MenuItem, on_delete=models.CASCADE)
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    date = models.DateField()

    class Meta:
        unique_together = ('user', 'item', 'restaurant', 'date')




