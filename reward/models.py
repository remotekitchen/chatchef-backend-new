import datetime
from decimal import Decimal, InvalidOperation
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from accounts.models import Company, User
from core.models import BaseModel
from core.utils import generate_random_string
from food.models import Category, Location, MenuItem, Restaurant
from reward.managers import UserRewardManager
from reward.utils.reward_calculation import RewardCalculation



class RewardGroup(BaseModel):
    class AppliesFor(models.TextChoices):
        DELIVERY = "delivery", _("DELIVERY")
        PICKUP = "pickup", _("PICKUP")
        DINE_IN = "dine_in", _("DINE_IN")
    


    class ValidityType(models.TextChoices):
        UNLIMITED = "unlimited", _("UNLIMITED")
        DAYS_AFTER_REWARDED = "days_after_rewarded", _("DAYS_AFTER_REWARDED")
        SPECIFIC_DATE = "special_date", _("SPECIFIC_DATE")

    name = models.CharField(max_length=250, verbose_name=_("Name"))
    description = models.TextField(verbose_name=_("Description"), blank=True)
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.SET_NULL, null=True, blank=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True, blank=True
    )

    # conditions
    applies_for = ArrayField(
        models.CharField(
            max_length=12,
            verbose_name=_("Applies for"),
            choices=AppliesFor.choices,
            default=AppliesFor.DELIVERY,
        ),
        blank=True,
        default=list,
    )
    validity_type = models.CharField(
        max_length=25,
        verbose_name=_("Validity Type"),
        choices=ValidityType.choices,
        default=ValidityType.DAYS_AFTER_REWARDED,
    )
    # Applicable for DAYS_AFTER_REWARDED
    validity_days = models.PositiveIntegerField(
        verbose_name=_("Validity Days"), default=0
    )
    # Applicable for SPECIFIC_DATE
    validity_date = models.DateField(
        verbose_name=_("Validity Date"), null=True, blank=True
    )
    location = models.ForeignKey(
        Location,  
        verbose_name=_("Location"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )

    deleted = models.BooleanField(default=False)

    class Meta:
        verbose_name = _('Reward Group')
        verbose_name_plural = _('Reward Groups')
        ordering = ['-id']
        indexes = [
            models.Index(fields=['validity_type']),
            models.Index(fields=['validity_date']),
            models.Index(fields=['deleted']),
        ]


    def __str__(self):
        if self.restaurant is not None:
            return f'{self.name}::{self.restaurant.name}'
        else:
            return f'{self.name}::No Restaurant Assigned'


# Restaurant Owner Decide how much points needed
class RewardLevel(BaseModel):
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    reward_level = models.PositiveIntegerField(
        verbose_name=_('Reward Level'), default=0)
    min_points = models.PositiveIntegerField(
        verbose_name=_('Min points'), default=0)
    max_points = models.PositiveIntegerField(
        verbose_name=_('Max points'), default=0)
    logo = models.ImageField(
        upload_to="images/%Y/%m/%d/", verbose_name=_("Logo Image"), blank=True, null=True
    )
    background_image = models.ImageField(
        upload_to="images/%Y/%m/%d/", verbose_name=_("Background Image"), blank=True, null=True
    )

    class Meta:
        ordering = ['-min_points']
        verbose_name = _('Reward level')
        verbose_name_plural = _('Reward levels')

    def __str__(self):
        return f'{self.restaurant or ""}::{self.min_points}-{self.max_points}'


class RewardManage(BaseModel):
    reward_level = models.ForeignKey(
        RewardLevel,
        verbose_name=_('Reward Level'),
        related_name='reward_manages',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    # company = models.ForeignKey(Company, verbose_name=_('Company'), on_delete=models.SET_NULL, null=True)
    restaurant = models.ForeignKey(Restaurant, verbose_name=_(
        'Restaurant'), on_delete=models.CASCADE)
    reward_group = models.ForeignKey(RewardGroup, verbose_name=_(
        'Reward Group'), on_delete=models.SET_NULL, null=True)
    points_required = models.PositiveIntegerField(
        verbose_name=_('Points Required'), default=0)

    class Meta:
        verbose_name = _('RewardManage')
        verbose_name_plural = _('RewardManages')
        ordering = ['-id']

    def __str__(self):
        return f'{self.restaurant.name}:: Total Reward point: {self.points_required}'


class AdditionalCondition(models.Model):
    class ConditionType(models.TextChoices):
        MINIMUM_AMOUNT = "minimum_amount", _("MINIMUM_AMOUNT")
        TIME_OF_DAY = "time_of_day", _("TIME_OF_DAY")
        SPECIFIC_ITEM_IN_CART = "specific_item_in_cart", _(
            "SPECIFIC_ITEM_IN_CART")

    reward_group = models.ForeignKey(
        RewardGroup, verbose_name=_("Reward Group"), on_delete=models.CASCADE
    )
    condition_type = models.CharField(
        max_length=35,
        verbose_name=_("Condition Type"),
        choices=ConditionType.choices,
        default=ConditionType.MINIMUM_AMOUNT,
    )
    # Applicable for MINIMUM_AMOUNT condition
    amount = models.FloatField(verbose_name=_("Amount"), default=0)
    # Applicable for SPECIFIC_ITEM_IN_CART condition
    items = models.ManyToManyField(
        MenuItem, verbose_name=_("Items"), blank=True)
    # Applicable for TIME_OF_DAY condition
    start_time = models.TimeField(verbose_name=_(
        "Start time"), null=True, blank=True)
    end_time = models.TimeField(verbose_name=_(
        "End time"), null=True, blank=True)

    class Meta:
        verbose_name = _("Additional Condition")
        verbose_name_plural = _("Additional Conditions")
        ordering = ["-id"]


class Reward(BaseModel):
    class RewardType(models.TextChoices):
        SINGLE_DISH = "single_dish", _("SINGLE_DISH")
        MULTIPLE_DISH = "multiple_dish", _("MULTIPLE_DISH")
        BOGO = "bogo", _("BOGO")
        BXGY = "bxgy", _("BXGY")
        COUPON = "coupon", _("COUPON")
        REWARD_POINT = "reward_point", _("REWARD_POINT")

    class OfferType(models.TextChoices):
        FREE = "free", _("FREE")
        FLAT = "flat", _("FLAT")
        PERCENTAGE = "percentage", _("PERCENTAGE")

    class LimitType(models.TextChoices):
        ONE_DISH = "one_dish", _("ONE_DISH")
        LIMITED = "limited", _("LIMITED")
        FULL_MEAL = "full_meal", _("FULL_MEAL")

    class BogoType(models.TextChoices):
        ANY_DISH = "any_dish", _("ANY_DISH")
        SELECTED_CATEGORY = "selected_category", _("SELECTED_CATEGORY")
        SELECTED_DISHES = "selected_dishes", _("SELECTED_DISHES")

    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True, blank=True
    )
    reward_group = models.ForeignKey(
        RewardGroup, verbose_name=_("Reward Group"), on_delete=models.CASCADE, null=True
    )
    reward_type = models.CharField(
        max_length=25,
        verbose_name=_("Reward Type"),
        choices=RewardType.choices,
        default=RewardType.SINGLE_DISH,
    )
    bogo_type = models.CharField(
        max_length=20,
        verbose_name=_("Bogo Type"),
        choices=BogoType.choices,
        default=BogoType.SELECTED_DISHES,
    )
    items = models.ManyToManyField(
        MenuItem, verbose_name=_("Menu items"), blank=True)
    categories = models.ManyToManyField(
        Category, verbose_name=_("Categories"), blank=True
    )
    offer_type = models.CharField(
        max_length=25,
        verbose_name=_("Offer Type"),
        choices=OfferType.choices,
        default=OfferType.PERCENTAGE,
    )
    amount = models.FloatField(verbose_name=_("Amount"), default=0)
    limit_type = models.CharField(
        max_length=25,
        verbose_name=_("Limit Type"),
        choices=LimitType.choices,
        default=LimitType.LIMITED,
    )
    # min and max dishes are application if limit_type is limited
    min_dishes = models.PositiveIntegerField(
        verbose_name=_("Minimum dishes"), default=0
    )
    max_dishes = models.PositiveIntegerField(
        verbose_name=_("Maximum dishes"), default=0
    )

    is_free_delivery = models.BooleanField(
        verbose_name=_("Is Free Delivery"), default=False
    )
    delivery_discount = models.FloatField(
        verbose_name=_("Delivery discount"), default=0
    )
    reward_points_worth = models.PositiveIntegerField(
        verbose_name=_('Reward points worth of'), default=0)

    class Meta:
        verbose_name = _("Reward")
        verbose_name_plural = _("Rewards")
        ordering = ["-id"]

    def __str__(self):
        return f"{self.reward_group.name}::{self.id}"


class UserReward(BaseModel):
    class Audience(models.TextChoices):
        NONE = "none", _("None")
        ALL = "all", _("All")
        FIRST_ORDER = "first_order", _("First Order")
        SECOND_ORDER = "second_order", _("Second Order")
        THIRD_ORDER = "third_order", _("Third Order")
        
    class PlatformChoices(models.TextChoices):
        ALL = "all", _("ALL")
        CHATCHEF = "chatchef", _("CHATCHEF")
        REMOTEKITCHEN = "remotekitchen", _("REMOTEKITCHEN")
        
    user = models.ForeignKey(
        User, verbose_name=_("User"), on_delete=models.SET_NULL, null=True, blank=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE, null=True, blank=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True, blank=True
    )
    code = models.CharField(max_length=150, verbose_name=_("Code"), blank=True)
    reward_type = models.CharField(
        max_length=150, verbose_name=_("Reward Type"), blank=True
    )
    amount = models.FloatField(verbose_name=_("Amount"), default=0)
    reward = models.ForeignKey(
        Reward, verbose_name=_("Reward"), on_delete=models.SET_NULL, null=True
    )
    audience = models.CharField(
        max_length=50,
        verbose_name=_("Audience"),
        choices=Audience.choices,
        default=Audience.NONE,
        null=True,
        blank=True,
    )
    is_claimed = models.BooleanField(
        verbose_name=_("Is claimed"), default=False)
    expiry_date = models.DateField(verbose_name=_(
        "Expiry date"), null=True, blank=True)
    given_for_not_order_last_x_days = models.BooleanField(default=False)
    objects = UserRewardManager()
    platform = models.CharField(
        max_length=20,
        verbose_name=_("Platform"),
        choices=PlatformChoices.choices,
        default=PlatformChoices.ALL
    )

    # redeemObjects = RedeemGenerator()

    class Meta:
        verbose_name = _("User Reward")
        verbose_name_plural = _("User Rewards")
        ordering = ["-id"]

    def __str__(self):
        try:
            return self.user.email
        except:
            return f"{self.id}"

    def save(self, *args, **kwargs):
        if not self.code:
            self.code = generate_random_string(
                include_numbers=False, include_punctuations=False)
        self.expiry_date = self.get_expiry_date()
        super().save(*args, **kwargs)

    def apply_reward(self, order_list, subtotal, order_method, delivery_fee, redeem=False, loyalty=False):
        calculator = RewardCalculation()
        discount, fee = calculator.apply_reward(
            order_list,
            subtotal,
            self.reward,
            order_method,
            delivery_fee,
            awarded_at=self.created_date,
            redeem=redeem,
            loyalty=loyalty
        )
        # if is_paid:
        #     self.is_claimed = True
        #     self.save(update_fields=['is_claimed'])
        return discount, fee

    # @property
    def get_expiry_date(self):
        dt = self.created_date or timezone.now()
        return dt.date() + datetime.timedelta(
            days=self.reward.reward_group.validity_days - 1) \
            if self.reward.reward_group.validity_type == RewardGroup.ValidityType.DAYS_AFTER_REWARDED \
            else self.reward.reward_group.validity_date \
            if self.reward.reward_group.validity_type == RewardGroup.ValidityType.SPECIFIC_DATE else None


class LocalDeal(BaseModel):
  
    class DealType(models.TextChoices):
        SPECIAL_DISCOUNT = "special_discount", _("Special Discount")
        NORMAL_DISCOUNT = "normal_discount", _("Normal Discount")
    class OfferType(models.TextChoices):
        PERCENTAGE = "percentage", _("Percentage")
        FLAT = "flat", _("Flat")
        BOGO = "bogo", _("BOGO")
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    menu_item = models.ForeignKey(MenuItem, on_delete=models.CASCADE)
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True)
    main_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, help_text="Base price of the menu item")
    discount_amount = models.DecimalField(max_digits=6, decimal_places=2, default=0, help_text="Total discount amount applied to the item")
    deal_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, help_text="Final price user pays")
    offer_type = models.CharField(max_length=20, choices=OfferType.choices, default=OfferType.PERCENTAGE)
    deal_type = models.CharField(max_length=20, choices=DealType.choices, default=DealType.NORMAL_DISCOUNT, help_text="Type of the deal || special discount must be special then normal discount")
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    purchase_limit = models.PositiveIntegerField(null=True, blank=True)
    times_purchased = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.menu_item.name} ({self.restaurant.name})"

    def is_expired(self):
        return timezone.now() > self.end_time

    def save(self, *args, **kwargs):
        try:
            # Safely get the base price from the related MenuItem
            base_price = Decimal(str(self.menu_item.base_price))
        except (AttributeError, InvalidOperation):
            base_price = Decimal('0.00')  # fallback if missing or invalid

        self.main_price = base_price

        # Calculate deal_price
        try:
            if self.offer_type == self.OfferType.PERCENTAGE:
                self.deal_price = base_price * (1 - Decimal(self.discount_amount) / 100)
            elif self.offer_type == self.OfferType.FLAT:
                self.deal_price = max(base_price - Decimal(self.discount_amount), Decimal('0.00'))
            elif self.offer_type == self.OfferType.BOGO:
                self.deal_price = base_price
            else:
                self.deal_price = base_price
        except (InvalidOperation, ZeroDivisionError):
            self.deal_price = base_price  # fallback

        super().save(*args, **kwargs)







class RetentionConfig(models.Model):
    reward_group = models.ForeignKey(RewardGroup, on_delete=models.CASCADE)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Retention Config"
        verbose_name_plural = "Retention Configs"
        ordering = ['-created_at']

    def __str__(self):
        return f"RetentionConfig: {self.reward_group.name}"




class NotificationLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    tier = models.CharField(max_length=50)
    channel = models.CharField(max_length=10)  # push / sms
    message_content = models.TextField()
    sent_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20)





class Campaign(models.Model):
    name = models.CharField(max_length=255)
    reward_amount = models.DecimalField(max_digits=10, decimal_places=2)  # e.g. 600.00
    target_cac = models.DecimalField(max_digits=10, decimal_places=2)  # Target cost per acquisition
    completion_rate_base = models.FloatField(default=1.0)  # θₙ
    cut_decay_alpha = models.FloatField(default=0.95)  # α
    cut_decay_beta = models.FloatField(default=0.85)   # β
    drop_items = models.JSONField(default=list)  # ['diamond', 'coin', 'key', 'gold']
    drop_cycle = models.IntegerField(default=5)  # rotate every N drops
    item_thresholds = models.JSONField(default=dict)  # e.g. {"diamond": 10, "coin": 20}
    exchange_rates = models.JSONField(default=dict)  # e.g. {"gold_to_diamond": 0.1}
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.reward_amount}৳)"



class Task(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE)
    task_progress_real = models.FloatField(default=0)
    task_progress_fake = models.FloatField(default=0)
    current_drop_item = models.CharField(max_length=100, blank=True, null=True)
    drop_counter = models.IntegerField(default=0)
    item_inventory = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

class Cut(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE)
    cutter = models.ForeignKey(User, on_delete=models.CASCADE)
    contribution = models.FloatField()
    is_new_user = models.BooleanField(default=False)
    item_dropped = models.CharField(max_length=100)
    item_quantity = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

class RewardRedemption(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    reward_value = models.DecimalField(max_digits=10, decimal_places=2)
    item_used = models.CharField(max_length=100)
    redeemed_at = models.DateTimeField(auto_now_add=True)

