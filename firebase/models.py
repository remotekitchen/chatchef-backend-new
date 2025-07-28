from django.db import models

from accounts.models import User, Company
from core.models import BaseModel
from django.utils.translation import gettext_lazy as _
from django.utils import timezone

from food.models import Image, Restaurant


class Platform(models.TextChoices):
    WEB = 'web', _('WEB')
    ANDROID = 'android', _('ANDROID')
    IOS = 'ios', _('IOS')


class BasePushToken(BaseModel):
    device_id = models.CharField(max_length=100, verbose_name=_('Device ID'), blank=True)
    platform = models.CharField(max_length=15, verbose_name=_('Platform'), choices=Platform.choices,
                                default=Platform.WEB)
    push_token = models.TextField(verbose_name=_('Push token'), blank=True)

    class Meta:
        abstract = True

    def __str__(self):
        return self.push_token


class FirebasePushToken(BasePushToken):
    user = models.ForeignKey(User, verbose_name=_('User'), on_delete=models.SET_NULL, null=True, blank=True)

    class Meta:
        ordering = ['-id']
        verbose_name = _('Firebase Push Token')
        verbose_name_plural = _('Firebase Push Tokens')

    def __str__(self):
        username = self.user.email if self.user is not None else ''
        return f'{username}::{self.push_token}'


class CompanyPushToken(BasePushToken):
    company = models.ForeignKey(Company, verbose_name=_('Company'), on_delete=models.SET_NULL, null=True, blank=True)

    class Meta:
        ordering = ['-id']
        verbose_name = _('Company Push Token')
        verbose_name_plural = _('Company Push Tokens')

    def __str__(self):
        return f'{self.company.name}::{self.push_token}'


class NotificationTemplate(BaseModel):
    key = models.CharField(max_length=150, verbose_name=_('Key'))
    title = models.CharField(max_length=120, verbose_name=_('Title'), blank=True)

    notification_title = models.CharField(verbose_name=_('Notification Title'), max_length=250)
    notification_body = models.TextField(verbose_name=_('Notification body'), blank=True)
    notification_image = models.ForeignKey(
        Image, verbose_name=_('Notification Image'), on_delete=models.SET_NULL,
        null=True, blank=True
    )
    click_action = models.CharField(verbose_name=_('Click Action'), max_length=200, blank=True)

    data = models.JSONField(verbose_name=_('Data'), blank=True, default=dict)

    class Meta:
        ordering = ['id']
        verbose_name = _('Push Template')
        verbose_name_plural = _('Push Templates')

    def __str__(self):
        return self.key



class TokenFCM(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    token = models.CharField(max_length=255, unique=True)
    device_type = models.CharField(
        max_length=10,
        choices=[("web", "Web"), ("ios", "iOS")],
        default="web"  # Set a default value
    )
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user} - {self.device_type}"




class PromotionalCampaign(models.Model):
    CATEGORY_CHOICES = (
        ("BOGO", "Buy One Get One"),
        ("BXGY", "Buy X Get Y"),
        ("FIRST_ORDER_DISCOUNT", "First Order Discount"),
        ("PERCENTAGE_DISCOUNT", "Percentage Discount"),
        ("CASHBACK", "Cashback Offer"),
        ("FREE_DELIVERY", "Free Delivery"),
        ("FLASH_SALE", "Flash Sale"),
        ("LIMITED_TIME_OFFER", "Limited Time Offer"),
        ("HOLIDAY_SPECIAL", "Holiday Special"),
    )

    restaurant = models.ForeignKey(
        Restaurant,  # Keep direct model reference
        on_delete=models.CASCADE,
        related_name="promotional_campaigns",
        null=True,  # ✅ Allows NULL values in the database
        blank=True,  # ✅ Allows leaving it empty in Django admin/forms
        verbose_name="Restaurant",
    )
    title = models.CharField(max_length=255, verbose_name="Title")  # Mandatory
    message = models.TextField(verbose_name="Message/Body", )  # Mandatory
    category = models.CharField(
        max_length=50, 
        choices=CATEGORY_CHOICES, 
        verbose_name="Category",
        default="GENERAL",
    )  # Mandatory with predefined choices
    campaign_image = models.ImageField(
        upload_to="promotional_campaign_images/", null=True, blank=True, verbose_name="Image"
    )  # Optional image field
    schedule_times = models.JSONField(default=dict, verbose_name="Schedule Times") 
    is_active = models.BooleanField(default=True, verbose_name="Is Active")  # New field

    def __str__(self):
        return f"{self.title} - {self.category}"

    def get_recipients(self):
        """
        Returns a list of device tokens or user IDs who should receive the notification.
        This assumes the Restaurant model has a related `subscribers` field with user profiles.
        """
        subscribers = self.restaurant.subscribers.all()  # Assuming a ManyToMany field in Restaurant
        return [user.device_token for user in subscribers if user.device_token]  # Return valid tokens