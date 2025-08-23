import hashlib
import uuid
from django.utils import timezone
from allauth.socialaccount.models import SocialAccount
from django.contrib.auth.models import AbstractUser
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext_lazy as _
from rest_framework.exceptions import APIException, PermissionDenied
from rest_framework.generics import get_object_or_404
import secrets

from accounts.managers import UserManager
from core.models import BaseAddress, BaseModel
from core.utils import get_logger

logger = get_logger()


# Create your models here.


class User(AbstractUser):
    class RoleType(models.TextChoices):
        OWNER = 'owner', _('Owner')
        EMPLOYEE = 'employee', _('Employee')
        HOTEL_OWNER = 'hotel_owner', _('Hotel Owner')
        NA = 'n/a', _('N/A')

    username = None
    email = models.EmailField(verbose_name=_('Email address'), unique=True)
    phone = models.CharField(
        max_length=20, verbose_name=_('Phone number'), blank=True)
    company = models.ForeignKey(to='Company', verbose_name=_(
        'Company'), on_delete=models.SET_NULL, null=True,blank=True)
    role = models.CharField(max_length=15, verbose_name=_(
        'Role'), choices=RoleType.choices, default=RoleType.NA)
    address = models.TextField(verbose_name=_('Address'), blank=True)

    date_of_birth = models.DateField(verbose_name=_(
        "date of birth"), blank=True, null=True)

    reward_points = models.PositiveIntegerField(
        verbose_name=_('Reward points'), default=0)
    direct_order_only = models.BooleanField(
        verbose_name=_('Direct order only'), default=True)
    is_email_verified = models.BooleanField(
        verbose_name=_('is email verified'), default=False)
    is_phone_verified = models.BooleanField(
        verbose_name=_('is phone verified'), default=False)
    is_blocked = models.BooleanField(default=False, verbose_name=_("Blocked"))

    agree = models.BooleanField(verbose_name=_('Agree'), default=False)
    uid = models.UUIDField(verbose_name=_(
        'verify id'), unique=True, blank=True, null=True)
    objects = UserManager()

    is_sales = models.BooleanField(verbose_name=_('Is_Sales'), default=False)
    referred_by = models.ForeignKey(
        'self', verbose_name=_('Referred by'), null=True, blank=True,
        on_delete=models.SET_NULL, related_name='referrals'
    )  
    super_power = models.BooleanField(
        verbose_name=_('Super power'), default=False)

    is_get_600 = models.BooleanField(
        verbose_name=_('Is get 600'), default=False)
    
    hotel_admin = models.BooleanField(
        verbose_name=_('Hotel Admin'),
        default=False
    )
    order_count_total_rk = models.PositiveIntegerField(
        default=0, verbose_name=_("Total Order Count")
    )

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []

    def __str__(self):
        return self.email

    def get_full_name(self):
        return super().get_full_name()

    def get_google_profile_data(self):
        social_account = SocialAccount.objects.filter(user=self).first()
        try:
            return social_account.extra_data
        except (AttributeError, Exception):
            return {}

    def is_old_user(self, restaurant):
        from billing.models import Order
        q_exp = Q(user=self, restaurant=restaurant)
        q_exp &= (Q(payment_method=Order.PaymentMethod.CASH) | Q(is_paid=True))
        return Order.objects.filter(q_exp).exists()
    
    def order_count(self, restaurant):
        from billing.models import Order
        q_exp = Q(user=self, restaurant=restaurant)
        q_exp &= (Q(payment_method=Order.PaymentMethod.CASH) | Q(is_paid=True))
        return Order.objects.filter(q_exp).count()

    def is_second_order_user(self, restaurant):
        return self.order_count(restaurant) == 1  # One completed order means this is their second order.

    def is_third_order_user(self, restaurant):
        return self.order_count(restaurant) == 2  # Two completed orders means this is their third order.
    
    
    @property
    def hotel_count(self):
        from hotel.models import Hotel  # Adjust import if needed
        return Hotel.objects.filter(owner=self).count()



class Company(BaseModel):
    name = models.CharField(max_length=255, verbose_name=_('Name'), blank=True)
    # owner = models.ForeignKey(User, verbose_name=_('Owner'), related_name='owned_companies',
    # on_delete=models.SET_NULL, null=True)
    # employees = models.ManyToManyField(User, verbose_name=_('Employees'), related_name='companies')
    uid = models.UUIDField(verbose_name=_('Company uid'),
                           default=uuid.uuid4, editable=False, unique=True)
    register_code = models.CharField(
        max_length=255, verbose_name=_('Employ Register Code'), blank=True, null=True, unique=True)

    class Meta:
        verbose_name = _('Company')
        verbose_name_plural = _('Companies')

    def __str__(self):
        return f'{self.name}'


class RestaurantUser(BaseModel):
    from food.models import Restaurant
    from marketing.models import FissionCampaign

    user = models.ForeignKey(User, verbose_name=_(
        "User"), on_delete=models.CASCADE)
    restaurant = models.ForeignKey(Restaurant, verbose_name=_(
        "Restaurant"), on_delete=models.SET_NULL, null=True)
    last_used_fission = models.DateTimeField(
        verbose_name=_("Last Used Fission"), null=True, blank=True)
    reward_points = models.PositiveIntegerField(
        verbose_name=_("Reward Points"), default=0)
    available_lucky_draws = models.ManyToManyField(
        FissionCampaign, verbose_name=_('Available Lucky Draws'), blank=True)
    points_spent = models.PositiveIntegerField(
        verbose_name=_('Points Spent'), default=0)
    remain_point = models.PositiveIntegerField(verbose_name=_(
        'Remain Points'), default=0, null=True, blank=True)
    rewards_category = models.CharField(
        max_length=15, verbose_name=_('Level'), default='Level-0')
    next_level = models.PositiveIntegerField(verbose_name=_(
        'Next  Level'), null=True, blank=True, default=0)
    reward_level = models.PositiveIntegerField(
        verbose_name=_('Reward Level'), default=0)
    is_blocked = models.BooleanField(default=False)
    

    def save(self, *args, **kwargs):
        self.calculate_reward_levels()
        super().save(*args, **kwargs)

    def calculate_next_level(self):
        level_gaps = [50, 70, 100, 250, 500, 900]
        for gap in level_gaps:
            if self.reward_points <= gap:
                self.next_level = gap - self.reward_points
                break

    def calculate_reward_levels(self):
        from reward.models import RewardLevel

        # level_thresholds = [0, 50, 70, 100, 250, 500, 900]
        level_ranges = RewardLevel.objects.filter(restaurant=self.restaurant).order_by('min_points').values(
            'min_points', 'max_points', 'reward_level'
        )
        for level in level_ranges:
            """
                Checking if current reward point is in range
            """
            if level.get('min_points') <= self.reward_points <= level.get('max_points'):
                self.reward_level = level.get('reward_level')

            """
                If current reward points is less than min_points calculate next level and break
            """
            if level.get('min_points') > self.reward_points:
                self.next_level = level.get('min_points') - self.reward_points
                break

    def redeem_reward_points(self, reward_manage, location):
        from reward.models import RewardManage, UserReward

        try:
            points_required = reward_manage.points_required
            print(points_required, self.reward_points)
            if self.reward_points >= points_required:
                # deduct points from total reward points
                # Update points_spent
                # self.points_spent = points_required
                # self.remain_point = self.reward_points
                user_rewards = UserReward.objects.create_from_reward_group(self.user, reward_manage.reward_group,
                                                                           self.restaurant.id,
                                                                           location, None)
                self.reward_points -= points_required
                self.save(update_fields=['reward_points'])
                return user_rewards
            else:
                # Handle insufficient points
                raise PermissionDenied("Insufficient reward points to redeem")

        # except RestaurantUser.DoesNotExist:
        #     # Handle the case where the RestaurantUser for the current user does not exist
        #     logger.error(f'RedeemGenerator: RestaurantUser does not exist for user {self.user}')
        # except ValueError as ve:
        #     # Handle insufficient points or other value-related errors
        #     logger.error(f'RedeemGenerator: {ve}')
        except Exception as e:
            logger.error(f'Redeem Generator  has error :: {e}')
            raise e

    class Meta:
        verbose_name = _('Restaurant User')
        verbose_name_plural = _('Restaurant Users')

    def __str__(self):
        return f'{self.user.email}::{self.restaurant}'


class UserAddress(BaseAddress):
    class GenderChoices(models.TextChoices):
        MALE = 'male', _('Male')
        FEMALE = 'female', _('Female')

    is_default = models.BooleanField(
        verbose_name=_('Is default'), default=False
    )
    user = models.ForeignKey(
        User, verbose_name=_('User'), on_delete=models.CASCADE
    )
    phone = models.CharField(
        max_length=20,
        verbose_name=_('Phone'),
        blank=True,
    )
    label = models.CharField(
        max_length=50, verbose_name=_('Label')
    )
    gender = models.CharField(
        max_length=10,
        choices=GenderChoices.choices,
        default=GenderChoices.MALE, 
        verbose_name=_('Gender'),
        null=True,
        blank=True
    )
    contact_person = models.CharField(
        max_length=100,
        verbose_name=_('Contact Person'),
        blank=True,
        null=True
    )

    lat = models.FloatField(
        verbose_name=_('Latitude'),
        null=True,
        blank=True
    )
    lng = models.FloatField(
        verbose_name=_('Longitude'),
        null=True,
        blank=True
    )

    class Meta:
        verbose_name = _('User Address')
        verbose_name_plural = _('User Addresses')

    def __str__(self):
        return f'{self.user.email} :: {self.id}'


class Otp(BaseModel):
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, blank=True, null=True, verbose_name=_("user"))
    otp = models.PositiveIntegerField(default=0)
    phone = models.CharField(max_length=15)
    email = models.EmailField(blank=True, null=True)
    is_used = models.BooleanField(default=False, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = _('OTP')
        verbose_name_plural = _('OTPs')

    def __str__(self) -> str:
        return f'{self.otp} | is_used: {self.is_used}'
    def is_expired(self):
        return self.expires_at and timezone.now() > self.expires_at
    
    class Meta:
        indexes = [
            models.Index(fields=['user', 'is_used']),
            models.Index(fields=['otp', 'email']),
            models.Index(fields=['expires_at']),
            models.Index(fields=['created_date']),
        ]



class Contacts(BaseModel):
    sender = models.ForeignKey(
        RestaurantUser, on_delete=models.CASCADE, verbose_name=_("sender"), related_name="restaurant_user_contacts_sender")
    receiver = models.ForeignKey(
        RestaurantUser, on_delete=models.CASCADE, verbose_name=_("receiver"), related_name="restaurant_user_contacts_receiver")

    def save(self, *args, **kwargs):
        if not self.pk:
            if Contacts.objects.filter(
                models.Q(sender=self.sender, receiver=self.receiver)
                | models.Q(sender=self.receiver, receiver=self.sender)
            ).exists():
                raise ValueError(
                    "A connection already exists between these two users.")
        super().save(*args, **kwargs)

class UserEvent(models.Model):
    EVENT_CHOICES = [
        ('app_open', 'App Open'),
        ('app_install', 'App Install'),
        ('onboarding_completed', 'Onboarding Completed'),
        ('order_created', 'Order Created'),
        ('order_completed', 'Order Completed'),
        ('store_clicked', 'Store Clicked'),
        ('cart_abandoned', 'Cart Abandoned'),
        ('referral_shared', 'Referral Shared'),
        ('referral_signup', 'Referral Signup'),
        ('push_sent', 'Push Sent'),
        ('push_clicked', 'Push Clicked'),
        ('spin_invite_clicked', 'Spin Invite Clicked'),
        ('spin_install', 'Spin Install'),
         ('cut', 'Cut'),
            ('reward_redeemed', 'Reward Redeemed'),
             ('item_exchanged', 'Item Exchanged'),
        ('deal_clicked', 'Deal Clicked'),
        ('deal_ordered', 'Deal Ordered'),
    ]
    user = models.ForeignKey(
        User, on_delete=models.CASCADE,
        null=True, blank=True
    )
    event_name = models.CharField(max_length=50, choices=EVENT_CHOICES)
    event_time = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(blank=True, null=True)
    platform = models.CharField(max_length=30, blank=True, null=True)

    def __str__(self):
        return f"{self.user or 'Guest'} - {self.event_name} at {self.event_time}"


# Per-event daily counts
class DAURecord(models.Model):
    date = models.DateField()
    event_name = models.CharField(max_length=50, default="app_open")
    count = models.PositiveIntegerField()

    class Meta:
        unique_together = ('date', 'event_name')

    def __str__(self):
        return f"{self.date} - {self.event_name}: {self.count}"


# Frequency Segments
class UserEngagementSegment(models.Model):
    SEGMENT_CHOICES = [
        ('daily_active', 'Daily Active'),
        ('weekly_active', 'Weekly Active'),
        ('monthly_active', 'Monthly Active'),
        ('lapsed', 'Lapsed'),
    ]
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    segment = models.CharField(max_length=20, choices=SEGMENT_CHOICES)
    last_updated = models.DateField(auto_now=True)

    def __str__(self):
        return f"{self.user} - {self.segment}"


# Churn Status
class UserChurnStatus(models.Model):
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('at_risk', 'At Risk'),
        ('churned', 'Churned'),
    ]
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    last_activity_date = models.DateField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    last_updated = models.DateField(auto_now=True)

    def __str__(self):
        return f"{self.user} - {self.status} since {self.last_activity_date}"


# Cohort Retention
class UserCohort(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    cohort_label = models.CharField(max_length=20)
    signup_date = models.DateField()

class CohortRetentionRecord(models.Model):
    cohort_label = models.CharField(max_length=20)
    day_offset = models.PositiveIntegerField()
    retained_users = models.PositiveIntegerField()

# Conversion Funnel
class ConversionFunnelRecord(models.Model):
    date = models.DateField()
    opened_app = models.PositiveIntegerField()
    placed_order = models.PositiveIntegerField()
    conversion_rate = models.FloatField()
    

class BlockedPhoneNumber(models.Model):
    phone = models.CharField(max_length=20, unique=True)
    reason = models.TextField(blank=True, null=True)  # Optional reason why blocked
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.phone
class QRScan(models.Model):
    device_id = models.CharField(max_length=255, unique=True)  # ensure one scan per device
    ip_address = models.GenericIPAddressField()
    user_agent = models.TextField(blank=True, null=True)
    ref = models.CharField(max_length=100, blank=True, null=True)  # Optional
    timestamp = models.DateTimeField()  # Use BDT manually

    def __str__(self):
        return f"{self.device_id} | {self.timestamp}"




class Customer(models.Model):
    email = models.EmailField(unique=True)
    name = models.CharField(max_length=255)
    stripe_customer_id = models.CharField(max_length=255, unique=True, null=True, blank=True)  

    def __str__(self):
        return self.email


class Subscription(models.Model):
    STATUS_CHOICES = [
        ("active", "Active"),
        ("inactive", "Inactive"),
        ("canceled", "Canceled"),
    ]
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    plan_id = models.CharField(max_length=255)
    stripe_subscription_id = models.CharField(max_length=255, unique=True, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="active")
    current_period_start = models.DateTimeField(null=True, blank=True)
    current_period_end = models.DateTimeField(null=True, blank=True)


    def __str__(self):
        return f"{self.customer.email} - {self.plan_id}"



class CancellationRequest(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("processed", "Processed"),
    ]
    subscription = models.ForeignKey(Subscription, on_delete=models.CASCADE)
    reason = models.TextField()
    details = models.TextField(blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Cancellation - {self.subscription.id}"



class UserSession(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    session_start = models.DateTimeField(auto_now_add=True)
    session_end = models.DateTimeField(null=True, blank=True)
    platform = models.CharField(max_length=30, blank=True, null=True)  # Android / iOS / Web
    device_info = models.TextField(blank=True, null=True)
    app_version = models.CharField(max_length=20, blank=True, null=True)

    def duration_in_seconds(self):
        if self.session_end:
            return (self.session_end - self.session_start).total_seconds()
        return None

    def __str__(self):
        return f"{self.user or 'Guest'} session on {self.platform}"



class FeedbackPrompt(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    prompt_time = models.DateTimeField(auto_now_add=True)
    rating = models.PositiveSmallIntegerField(null=True, blank=True)  # 1 to 5
    skipped = models.BooleanField(default=False)
    source = models.CharField(max_length=100, blank=True, null=True)  # e.g. "home_screen", "after_order"
    platform = models.CharField(max_length=30, blank=True, null=True)  # Android / iOS / Web

    def __str__(self):
        return f"{self.user or 'Guest'}: {'⭐'+str(self.rating) if self.rating else ' Skipped'}"






class RedZone(models.Model):
    name = models.CharField(max_length=100)
    center_lat = models.FloatField(verbose_name=_('Latitude'))
    center_lon = models.FloatField(verbose_name=_('Longitude'))
    radius_km = models.FloatField(default=1.0)
    allowed_restaurants = models.ManyToManyField("food.Restaurant", blank=True)  
    coverage_area = models.CharField(max_length=200, blank=True, null=True)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name



# models.py
class QuickLoginUser(models.Model):
    name = models.CharField(max_length=100, blank=True, help_text="Full name of the user")
    email = models.EmailField(unique=True)
    password = models.CharField(max_length=128)
    label = models.CharField(max_length=100, blank=True, help_text="Optional description or role")
    fcm_token = models.CharField(max_length=256, blank=True, null=True)
    token = models.CharField(max_length=64, blank=True, null=True, unique=True)
    is_active = models.BooleanField(default=True, help_text="Disable to prevent login")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name or self.email} ({self.label})"

    def save(self, *args, **kwargs):
        if not self.token:
            self.token = secrets.token_hex(32)
        super().save(*args, **kwargs)
