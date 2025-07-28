import random

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from rest_framework.exceptions import ParseError, PermissionDenied

from accounts.models import Company, User
from core.models import BaseModel
from food.models import Image, Location, MenuItem, Restaurant, Menu
from marketing.managers import FissionCampaignManager
from reward.models import Reward, RewardGroup
from reward.utils.reward_calculation import RewardCalculation
from django.core.validators import MinValueValidator, MaxValueValidator
from django.contrib.postgres.fields import ArrayField


# Activation Campaign Models
class Duration(BaseModel):
    start_date = models.DateTimeField(
        verbose_name=_('Start Date'), default=timezone.now)
    end_date = models.DateTimeField(
        verbose_name=_('End Date'), default=timezone.now)

    class Meta:
        verbose_name = _('Duration')
        verbose_name_plural = _('Durations')
        


class SpendXSaveYPromoOption(BaseModel):
    name = models.CharField(max_length=250, verbose_name=_('Name'))
    min_spend = models.FloatField(verbose_name=_('Min spend'), default=0)
    save_amount = models.FloatField(verbose_name=_('Save amount'), default=0)

    class Meta:
        verbose_name = _('Spend X Save Y Promotion Option')
        verbose_name_plural = _('Spend X Save Y Promotion Options')

    def __str__(self):
        return self.name


class ActivationCampaign(BaseModel):
    """
        Base model for SpendXSaveY, Voucher, Bogo, Group ordering campaigns
    """

    class Audience(models.TextChoices):
        ALL = 'all', _('ALL')
        MEMBERS = 'members', _('MEMBERS')

    name = models.CharField(max_length=250, verbose_name=_('Promotion name'))
    company = models.ForeignKey(Company, verbose_name=_(
        'Company'), on_delete=models.CASCADE, null=True, blank=True)
    restaurant = models.ForeignKey(Restaurant, verbose_name=_(
        'Restaurant'), on_delete=models.SET_NULL, null=True, blank=True)
    location = models.ForeignKey(Location, verbose_name=_(
        'Location'), on_delete=models.SET_NULL, null=True, blank=True)
    menu = models.ForeignKey(
        Menu,  
        verbose_name=_('Menu'),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    durations = models.ManyToManyField(
        Duration, verbose_name=_('Durations'), blank=True)
    audience = models.CharField(max_length=20, verbose_name=_('Audience'), choices=Audience.choices,
                                default=Audience.ALL)

    class Meta:
        verbose_name = _('Activation Campaign')
        verbose_name_plural = _('Activation Campaigns')

    def update_cost(self, initial_cost):
        pass

    def __str__(self):
        return f'{self.name}::{self.company.name}'


class SpendXSaveYManager(ActivationCampaign):
    class Meta:
        verbose_name = _('Spend X Save Y Manager')
        verbose_name_plural = _('Spend X Save Y Managers')


class SpendXSaveY(ActivationCampaign):
    manager = models.ForeignKey(SpendXSaveYManager, verbose_name=_(
        'Manager'), on_delete=models.SET_NULL, null=True)
    min_spend = models.FloatField(verbose_name=_('Min spend'), default=0)
    save_amount = models.FloatField(verbose_name=_('Save amount'), default=0)

    class Meta:
        verbose_name = _('Spend X Save Y')
        verbose_name_plural = _('Spend X Save Y Campaigns')

    def update_cost(self, initial_cost):
        return initial_cost - self.save_amount \
            if initial_cost >= self.min_spend \
            else initial_cost


class Voucher(ActivationCampaign):
    class Audience(models.TextChoices):
        ALL = 'all', _('ALL')
        MEMBERS = 'members', _('MEMBERS')
        FIRST_ORDER = 'first_order', _('FIRST_ORDER')
        SECOND_ORDER = 'second_order', _('SECOND_ORDER')
        THIRD_ORDER = 'third_order', _('THIRD_ORDER')

    reward = models.ForeignKey(
        Reward, on_delete=models.SET_NULL, null=True, blank=True)
    applied_users = models.ManyToManyField(
        User, verbose_name=_('Applied Users'),
        blank=True
    )
    amount = models.FloatField(verbose_name=_('Amount'), default=0)
    minimum_spend = models.FloatField(
        verbose_name=_('Minimum Spend'), default=0
    )
    max_redeem_value = models.FloatField(
        verbose_name=_('Maximum Redeem Value'), null=True, blank=True
    )
    voucher_code = models.CharField(
        max_length=50, verbose_name=_('Voucher Code')
    )
    image = models.ForeignKey(Image, verbose_name=_(
        'Image'), on_delete=models.SET_NULL, null=True, blank=True)
    available_to = models.CharField(
        max_length=20, verbose_name=_('Available To'),
        choices=Audience.choices,
        default=None,  
        null=True, 
        blank=True  
    )
    is_one_time_use = models.BooleanField(default=False)
    
    is_global = models.BooleanField(
        default=False,
        help_text="If True, this voucher can be used by all users."
    )
    is_ht_voucher = models.BooleanField(
        default=False,
        help_text="If True, this voucher is a HT voucher."
    )
    ht_voucher_percentage_borne_by_restaurant = models.FloatField(
        default=0,
        help_text="Percentage of the voucher amount that is borne by the restaurant."
    )
    
    # number of times the voucher can be used
    max_uses = models.PositiveIntegerField(
        default=1,
        help_text="Maximum number of times this voucher can be used."
    )

    notification_sent = models.BooleanField(
        default=False,
        help_text="Has 48-hour expiry notification been sent?"
)

    last_notification_sent_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the last reminder notification was sent"
    )

    class Meta:
        verbose_name = _('Voucher')
        verbose_name_plural = _('Vouchers')

    def __str__(self):
        return self.name

    def update_cost(self, initial_cost):
        discount = 0
        if initial_cost >= self.minimum_spend:
            discount = min(initial_cost * (self.amount / 100),
                           self.max_redeem_value)
        return initial_cost - discount

    def apply_reward(self, order_list, subtotal, order_method, delivery_fee, restaurant, user=None, redeem=False, bogo_amount=0, bxgy_amount=0):
        if user is not None:
            if not user.is_authenticated and self.available_to != self.Audience.ALL:
                raise PermissionDenied('User is not authenticated')
            if user.is_old_user(restaurant=restaurant) and self.available_to == self.Audience.FIRST_ORDER:
                raise PermissionDenied(
                    'This voucher is only for first orders!')
            if self.available_to == self.Audience.SECOND_ORDER:
                if not user.is_second_order_user(restaurant=restaurant):
                    raise PermissionDenied(
                        'This voucher is only for second orders!')
            if self.available_to == self.Audience.THIRD_ORDER:
                if not user.is_third_order_user(restaurant=restaurant):
                    raise PermissionDenied(
                        'This voucher is only for third orders!')
            if self.is_one_time_use:
                already_applied_for_voucher = user in self.applied_users.all()
                if already_applied_for_voucher:
                    raise PermissionDenied(
                        'You already used this voucher!')

        discount, fee = 0, delivery_fee
        if self.reward:
            calculator = RewardCalculation()
            discount, fee = calculator.apply_reward(
                order_list,
                subtotal,
                self.reward,
                order_method,
                delivery_fee,
                awarded_at=self.created_date,
                redeem=redeem,
                bogo_amount=bogo_amount,
                bxgy_amount=bxgy_amount
            )
        else:
            if self.minimum_spend > subtotal:
                raise ParseError(
                    f"Minium order amount is {self.minimum_spend} to apply this voucher"
                )
            if self.minimum_spend <= subtotal:
                discount = min(
                    self.max_redeem_value,
                    subtotal * (self.amount / 100)
                )

        return discount, fee
      
    def get_ht_contribution(self, discount):
      restaurant_share = (self.ht_voucher_percentage_borne_by_restaurant / 100) * discount
      ht_share = discount - restaurant_share
      return restaurant_share, ht_share
    

class PlatformCouponExpiryLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    voucher = models.ForeignKey(Voucher, on_delete=models.SET_NULL, null=True)
    coupon_code = models.CharField(max_length=50)
    coupon_value = models.FloatField()
    expiry_date = models.DateField(null=True, blank=True)

    sent_at = models.DateTimeField()
    status = models.CharField(max_length=20)
    source = models.CharField(max_length=50, default="platform")


class Bogo(ActivationCampaign):
    items = models.ManyToManyField(
        MenuItem, verbose_name=_('Menu Items'), blank=True)
    is_disabled = models.BooleanField(default=False)
    inflate_percent = models.IntegerField(default=0)

    class Meta:
        verbose_name = _('Bogo')
        verbose_name_plural = _('Bogo Campaigns')
        
class BxGy(ActivationCampaign):
    items = models.ManyToManyField(
        MenuItem,
        verbose_name=_('Menu Items'),
        blank=True,
        related_name="bxgy_campaigns",
        help_text="Items that customers need to buy to qualify for the offer."
    )
    
  #   free_items = models.ManyToManyField(
  #       MenuItem,
  #       verbose_name=_('Free Items'),
  #       blank=True,
  #       related_name="bxgy_free_items",
  #       help_text="Items that can be given for free if different from the bought items."
  # )

    
    is_disabled = models.BooleanField(
        default=False,
        help_text="Disable this campaign without deleting it."
    )
    
    discount_percent = models.PositiveIntegerField(
        default=100,  # Default is 100% discount (free item)
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Discount percentage applied to the 'get' item(s). 100% means free."
    )
    
    applies_to_different_items = models.BooleanField(
        default=True,
        help_text="If True, 'get' items can be different from 'buy' items."
    )
    
    created_at = models.DateTimeField(default=timezone.now,  blank=True, null=True)
    updated_at = models.DateTimeField(default=timezone.now,  blank=True, null=True)
    
    class Meta:
        verbose_name = _('Buy X Get Y Campaign')
        verbose_name_plural = _('Buy X Get Y Campaigns')
        
class BxGyBuyItem(models.Model):
    campaign = models.ForeignKey(BxGy, on_delete=models.CASCADE, related_name='buy_items')
    buy_items = ArrayField(models.IntegerField(), help_text="List of MenuItem IDs to buy", null=True, blank=True)  # List of buy item IDs
    quantity = models.PositiveIntegerField(default=1)

class BxGyFreeItem(models.Model):
    buy_item_relation = models.ForeignKey(BxGyBuyItem, on_delete=models.CASCADE, related_name='free_items')
    free_items = ArrayField(models.IntegerField(), help_text="List of MenuItem IDs free", null=True, blank=True)
    quantity = models.PositiveIntegerField(default=1)

class GroupPromotionOption(BaseModel):
    people = models.IntegerField(verbose_name=_('People'), default=2)
    discount = models.IntegerField(verbose_name=_('Discount'), default=0)

    class Meta:
        verbose_name = _('Group Promotion Option')
        verbose_name_plural = _('Group Promotion Options')


class GroupPromotion(ActivationCampaign):
    promo_options = models.ManyToManyField(
        GroupPromotionOption, verbose_name=_('Promo Options'))

    class Meta:
        verbose_name = _('Group Promotion')
        verbose_name_plural = _('Group Promotions')


# Retention Campaigns
class LoyaltyProgram(BaseModel):
    class RewardType(models.TextChoices):
        DISCOUNT = 'discount', _('DISCOUNT')
        FREE_MEAL = 'free_meal', _('FREE_MEAL')

    name = models.CharField(max_length=250, verbose_name=_('Name'))
    is_enabled = models.BooleanField(
        verbose_name=_('Is enabled'), default=True)

    company = models.ForeignKey(Company, verbose_name=_(
        'company'), on_delete=models.CASCADE)
    restaurant = models.ForeignKey(Restaurant, verbose_name=_(
        'Restaurant'), on_delete=models.CASCADE)
    location = models.ManyToManyField(
        Location, verbose_name=_('Location'), blank=True)

    validity_days = models.IntegerField(
        verbose_name=_('Validity Days'), default=1)
    covers_delivery_fees = models.BooleanField(
        verbose_name=_('Covers Delivery'), default=False)
    reward_type = models.CharField(
        max_length=15,
        verbose_name=_('Reward type'),
        choices=RewardType.choices,
        default=RewardType.DISCOUNT
    )
    discount_amount = models.FloatField(
        verbose_name=_('Discount amount'), default=1)
    min_orders = models.IntegerField(
        verbose_name=_('Minimum Orders'), default=0)

    class Meta:
        verbose_name = _('Loyalty Program')
        verbose_name_plural = _('Loyalty Programs')

    def __str__(self):
        return f'{self.name} :: {self.restaurant.name}'


class BaseGiftCampaign(BaseModel):
    is_active = models.BooleanField(verbose_name=_('Is active'), default=True)
    company = models.ForeignKey(Company, verbose_name=_(
        'company'), on_delete=models.CASCADE)
    restaurant = models.ForeignKey(Restaurant, verbose_name=_(
        'Restaurant'), on_delete=models.SET_NULL, null=True)
    location = models.ForeignKey(Location, verbose_name=_(
        'Location'), on_delete=models.SET_NULL, null=True)
    received_by = models.ManyToManyField(
        User, verbose_name=_('User'), blank=True)

    class Meta:
        abstract = True


class BirthdayGift(BaseGiftCampaign):
    class GiftOptionType(models.TextChoices):
        SpendXSaveY = 'SpendXSaveY', _('SpendXSaveY')
        Voucher = 'Voucher', _('Voucher')
        Bogo = 'Bogo', _('Bogo')
        GroupPromotion = 'GroupPromotion', _('GroupPromotion')

    membership_only = models.BooleanField(
        verbose_name=_('Membership Only'), default=False)
    has_condition = models.BooleanField(
        verbose_name=_('Has condition'), default=False)
    minimum_spent = models.IntegerField(
        verbose_name=_('Minimum Spent'), default=0)
    gift_option_type = models.CharField(max_length=20, verbose_name=_('Gift Option Type'),
                                        choices=GiftOptionType.choices, default=GiftOptionType.SpendXSaveY)
    # Dynamic foreign key for Activation Campaigns
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    gift_option = GenericForeignKey('content_type', 'object_id')

    class Meta:
        verbose_name = _('Birthday Gift')
        verbose_name_plural = _('Birthday gifts')

    # def __str__(self):
    #     return f'{self.restaurant.name}::{self.location.name}'


class GiftCard(BaseGiftCampaign):
    allow_custom = models.BooleanField(
        verbose_name=_('Allow custom'), default=False)
    amounts = models.JSONField(verbose_name=_('Amounts'), default=list)

    class Meta:
        verbose_name = _('Gift Card')
        verbose_name_plural = _('Gift Cards')


class MembershipCard(BaseGiftCampaign):
    name = models.CharField(max_length=250, verbose_name=_('Name'))
    duration = models.PositiveIntegerField(
        verbose_name=_('Duration'), default=1)
    price = models.FloatField(verbose_name=_('Price'))
    # usage_time = models.OneToOneField(Duration, verbose_name=_(
    #     'Usage time'), on_delete=models.SET_NULL, null=True)
    start_time = models.TimeField(verbose_name=_(
        'Start time'), blank=True, null=True)
    end_time = models.TimeField(verbose_name=_(
        'End time'), blank=True, null=True)
    anytime = models.BooleanField(verbose_name=_('Anytime'), default=False)
    discount = models.FloatField(verbose_name=_('Discount'), default=0)
    limit_per_month = models.PositiveIntegerField(
        verbose_name=_('Limit per month'), default=0)
    dishes = models.ManyToManyField(
        MenuItem, verbose_name=_('Dishes'), blank=True)

    class Meta:
        verbose_name = _('Membership Card')
        verbose_name_plural = _('Membership Cards')

    def __str__(self):
        return self.name


# Fission campaigns
class FissionPrize(BaseModel):
    class PrizeNameTypes(models.TextChoices):
        BOGO = 'bogo', _("BOGO")
        BXGY = 'bxgy', _("BXGY")
        PERCENTAGE = 'percentage', _("PERCENTAGE")
        FIXED = 'fixed', _("FIXED")

    prize_name = models.CharField(max_length=250, verbose_name=_(
        'prize name'), choices=PrizeNameTypes.choices, default=PrizeNameTypes.PERCENTAGE)
    amount = models.FloatField(verbose_name=_("amount"), blank=True, null=True)
    probability = models.FloatField(verbose_name=_("probability"))
    reward_group = models.ForeignKey(RewardGroup, verbose_name=_(
        "reward_group"), on_delete=models.SET_NULL, null=True)

    class Meta:
        verbose_name = _('Fission Prize')
        verbose_name_plural = _('Fission Prizes')


# Fission Campaign

class FissionCampaign(BaseModel):
    class Availability(models.TextChoices):
        AFTER_EVERY_ORDER = 'after_every_order', _('AFTER_EVERY_ORDER')
        ONCE_EVERY_USER = 'once_every_user', _('ONCE_EVERY_USER')
        AFTER_SIGN_UP = 'after_sign_up', _('AFTER_SIGN_UP')
        AFTER_JOINS_GROUP = 'after_joins_group', _('AFTER_JOINS_GROUP')
        ONCE_EVERY_WEEK = 'once_every_week', _('ONCE_EVERY_WEEK')

    class ValidityType(models.TextChoices):
        REPEATING = 'repeating', _('REPEATING')
        TIME_LIMIT = 'time_limit', _('TIME_LIMIT')

    prizes = models.ManyToManyField(
        FissionPrize, verbose_name=_('Fission prizes'), blank=True
    )

    color = models.CharField(
        max_length=15, verbose_name=_('Color'), blank=True
    )
    background_color = models.CharField(
        max_length=15, verbose_name=_('Background Color'), blank=True
    )
    logo = models.ImageField(
        upload_to="images/%Y/%m/%d/", verbose_name=_("Logo Image"), blank=True, null=True
    )
    background_image = models.ImageField(
        upload_to="images/%Y/%m/%d/", verbose_name=_("Background Image"), blank=True, null=True
    )

    company = models.ForeignKey(Company, verbose_name=_(
        'Company'), on_delete=models.CASCADE)
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_('Restaurant'), on_delete=models.SET_NULL, null=True
    )

    is_active = models.BooleanField(verbose_name=_('Is Enabled'), default=True)
    availability = models.CharField(
        max_length=25, verbose_name=_('Availability'), choices=Availability.choices,
        default=Availability.AFTER_EVERY_ORDER
    )
    validity_type = models.CharField(
        max_length=30, verbose_name=_('Validity Type'), choices=ValidityType.choices,
        default=ValidityType.TIME_LIMIT
    )
    # If the validity_type is time_limit
    durations = models.ManyToManyField(
        Duration, verbose_name=_('Durations'), blank=True
    )
    # If the validity_type is repeating, it's a list of weekday index. 0 based index starts from Monday
    weekdays = models.JSONField(verbose_name=_('Weekdays'), default=list)

    max_flip_per_user = models.PositiveIntegerField(
        verbose_name=_('Max flip per user'), default=1)
    max_flip_period = models.CharField(
        max_length=20, verbose_name=_('Max flip period'), default='weekly')

    last_used_time = models.DateTimeField(
        verbose_name=_('Last used flip'), null=True, blank=True)
    last_week_users = models.ManyToManyField(
        User, verbose_name=_('Last week users'), blank=True)

    objects = FissionCampaignManager()

    class Meta:
        ordering = ['-id']
        verbose_name = _('Fission Campaign')
        verbose_name_plural = _('Fission Campaigns')

    def get_random_prize(self):
        """
            return a prize from the prizes under the campaign based on weighted probability.
        """
        objs = list(self.prizes.all())
        weights = list(self.prizes.values_list('probability', flat=True))
        random_obj = random.choices(objs, weights=weights, k=1)
        return random_obj[0]


class Rating(BaseModel):
    menuItem = models.ForeignKey(
        MenuItem, on_delete=models.CASCADE, null=True, blank=True, verbose_name=_("menu items"), related_name="ratings")
    rating = models.IntegerField(default=0, verbose_name=_('ratings'))

    user = models.ManyToManyField(
        User, blank=True, verbose_name=_("customer"))

    is_approved = models.BooleanField(
        default=False, verbose_name=_("is approved"))

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Rating")
        verbose_name_plural = _("Ratings")

    def __str__(self) -> str:
        return f"rating --> {self.menuItem}"


# used for reviews and blog posts



class Review(BaseModel):
    user = models.ForeignKey(User, on_delete=models.CASCADE) 
    restaurant = models.ForeignKey(
        Restaurant,
        on_delete=models.CASCADE,
        related_name="reviews",
        null=True  # Allow existing reviews without restaurants
    )
    review = models.TextField(null=True, blank=True, verbose_name="Review")
    menuItem = models.ForeignKey(MenuItem, on_delete=models.CASCADE, null=True, blank=True, verbose_name=_("menu items"))
    is_approved = models.BooleanField(default=False)
    is_pinned = models.BooleanField(default=False)
    likes = models.ManyToManyField(User, related_name='liked_reviews', blank=True)
    dislikes = models.ManyToManyField(User, related_name='disliked_reviews', blank=True)
    rating = models.FloatField(default=0)
    # restaurant_id = models.IntegerField(default=0)   
    order_id = models.IntegerField(blank=True, null=True)     


    def like(self, user):
        if self.dislikes.filter(id=user.id).exists():
            self.dislikes.remove(user)
        self.likes.add(user)

    def dislike(self, user):
        if self.likes.filter(id=user.id).exists():
            self.likes.remove(user)
        self.dislikes.add(user)

    def __str__(self):
        return f"Review by {self.user}"
    
class Comment(models.Model):
    review = models.ForeignKey(Review, on_delete=models.CASCADE, related_name='comments')
    user = models.ForeignKey(User, on_delete=models.CASCADE)  
    text = models.TextField(verbose_name=_("Comment text"))
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='replies')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Comment by {self.user} on {self.review}"


class EmailConfiguration(models.Model):
    restaurant = models.OneToOneField(Restaurant, on_delete=models.CASCADE)
    email_host = models.CharField(max_length=255, verbose_name=_("email host"))
    email_port = models.PositiveIntegerField(verbose_name=_("email port"))
    email_use_tls = models.BooleanField(
        default=True, verbose_name=_("email use tls"))
    email_host_user = models.EmailField(verbose_name=_("email host user"))
    email_host_password = models.CharField(
        max_length=255, verbose_name=_("email host password"))

    def __str__(self):
        return f"{self.restaurant}"

    class Meta:
        ordering = ["-id"]


class EmailHistory(BaseModel):
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, default=None)
    program = models.CharField(max_length=599, verbose_name=_(
        "program"), null=True, blank=True)
    audience = models.CharField(max_length=255, verbose_name=_(
        "audience"), null=True, blank=True)

    subject = models.CharField(max_length=255, verbose_name=_("subject"))
    message = models.TextField(verbose_name=_(
        "message"), null=True, blank=True)
    html_content = models.TextField(verbose_name=_(
        "html_content"), null=True, blank=True)
    scheduled_time = models.DateTimeField(
        verbose_name=_("scheduled time"), null=True, blank=True)
    sender_email = models.CharField(
        max_length=255, verbose_name=_("sender email"), blank=True)


class ContactUsData(BaseModel):
    email = models.EmailField(max_length=255)
    subject = models.CharField(max_length=255)
    description = models.TextField()

    def __str__(self) -> str:
        return f"{self.email} is trying to contact us"


class DemoData(BaseModel):
    class ROLE_TYPE(models.TextChoices):
        OWNER = "owner", _("OWNER")
        MANAGER = "manager", _("MANAGER")

    class LOOKING_FOR_CHOICES(models.TextChoices):
        DO = "do", _("DO")
        ATSM = "atsm", _("ATSM")
        WEBSITE = "website", _("WEBSITE")

    role = models.CharField(
        max_length=15, choices=ROLE_TYPE.choices, default=ROLE_TYPE.OWNER, verbose_name=_("role"))
    name = models.CharField(max_length=245, verbose_name=_("name"))
    email = models.EmailField(max_length=245, verbose_name=_("email"))
    phone = models.CharField(max_length=20, verbose_name=_("phone"))
    restaurant_name = models.CharField(
        max_length=245, verbose_name=_("restaurant name"))
    looking_for = models.CharField(
        max_length=100, verbose_name=_("service looking for"), choices=LOOKING_FOR_CHOICES.choices, default=LOOKING_FOR_CHOICES.DO)

    def __str__(self) -> str:
        return f"{self.id}"

    class Meta:
        ordering = ['-id']


class SalesMail(BaseModel):
    email = models.EmailField(max_length=245, unique=True)

    def __str__(self) -> str:
        return f"{self.email}"

    class Meta:
        ordering = ["-id"]


# class GiftCard(BaseModel): 
#   restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
#   location = models.ForeignKey(Location, on_delete=models.CASCADE)
#   userName = models.CharField(max_length=255)
#   email = models.EmailField(max_length=255)
#   amount = models.FloatField()
  
#   def __str__(self):
#     return f"{self.userName} - {self.amount}"


class AutoReplyToComments(models.Model):
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE, verbose_name=_("Restaurant"))
    location = models.ForeignKey(Location, on_delete=models.CASCADE, verbose_name=_("Location"))
    auto_reply_to_good_comments = models.BooleanField(default=False, verbose_name=_("Auto Reply to Good Comments"))
    auto_reply_to_bad_comments = models.BooleanField(default=False, verbose_name=_("Auto Reply to Bad Comments"))
    voucher_amount = models.IntegerField(default=0, verbose_name=_("Voucher Amount"))

    def __str__(self):
        return f"AutoReply settings for {self.restaurant.name} at {self.location.name}"
    


