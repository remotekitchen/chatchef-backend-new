
from django.db import models
from django.utils.translation import gettext as _

from core.models import BaseModel
from food.models import Location, Restaurant


# Create your models here.
class QrCode(BaseModel):
    restaurant = models.OneToOneField(
        Restaurant, verbose_name=_("restaurant"), on_delete=models.CASCADE)
    location = models.OneToOneField(
        Location, verbose_name=_("location"), on_delete=models.CASCADE)
    table_qrlink = models.CharField(
        max_length=255, verbose_name=_('table_qr'), blank=True, null=True)
    table_qrlink_scanned = models.IntegerField(
        verbose_name=_('table_qrlink_scanned'), default=0)
    banner_qrlink = models.CharField(
        max_length=255, verbose_name=_('banner_qrlink'), blank=True, null=True)
    banner_qrlink_scanned = models.IntegerField(
        verbose_name=_('table_qrlink_scanned'), default=0)
    social_qrlink = models.CharField(
        max_length=255, verbose_name=_('social_qrlink'), blank=True, null=True)
    social_qrlink_scanned = models.IntegerField(
        verbose_name=_('table_qrlink_scanned'), default=0)
    poster_qrlink = models.CharField(
        max_length=255, verbose_name=_('poster_qrlink'), blank=True, null=True)
    poster_qrlink_scanned = models.IntegerField(
        verbose_name=_('table_qrlink_scanned'), default=0)
    business_card_qrlink = models.CharField(
        max_length=255, verbose_name=_('business_card_qrlink'), blank=True, null=True)
    business_card_qrlink_scanned = models.IntegerField(
        verbose_name=_('business_card_qrlink_scanned'), default=0)
    flyer_qrlink = models.CharField(
        max_length=255, verbose_name=_('flyer_qrlink'), blank=True, null=True)
    flyer_qrlink_scanned = models.IntegerField(
        verbose_name=_('flyer_qrlink_scanned'), default=0)
    coupon_qrlink = models.CharField(
        max_length=255, verbose_name=_('flyer_qrlink'), blank=True, null=True)
    coupon_qrlink_scanned = models.IntegerField(
        verbose_name=_('coupon_qrlink_scanned'), default=0)

    def __str__(self) -> str:
        return f"{self.restaurant.name} --> {self.location.name}"
