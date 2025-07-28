import re

from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel
from food.models import Restaurant


class PosDetails(BaseModel):
    TIME_ZONE_CHOICES = (
        ('-12:00', '-12:00'),
        ('-11:00', '-11:00'),
        ('-10:00', '-10:00'),
        ('-09:00', '-09:00'),
        ('-08:00', '-08:00'),
        ('-07:00', '-07:00'),
        ('-06:00', '-06:00'),
        ('-05:00', '-05:00'),
        ('-04:00', '-04:00'),
        ('-03:00', '-03:00'),
        ('-02:00', '-02:00'),
        ('-01:00', '-01:00'),
        ('+00:00', '+00:00'),
        ('+01:00', '+01:00'),
        ('+02:00', '+02:00'),
        ('+03:00', '+03:00'),
        ('+04:00', '+04:00'),
        ('+05:00', '+05:00'),
        ('+06:00', '+06:00'),
        ('+07:00', '+07:00'),
        ('+08:00', '+08:00'),
        ('+09:00', '+09:00'),
        ('+10:00', '+10:00'),
        ('+11:00', '+11:00'),
        ('+12:00', '+12:00'),
    )

    POS_TYPE = (('clover', 'clover'), ('toast', 'toast'))

    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_('restaurant id'))
    pos_type = models.CharField(
        max_length=50, choices=POS_TYPE, verbose_name=_('pos provide'))
    pos_merchant_id = models.CharField(
        max_length=255, verbose_name=_('pos merchant or store id'))
    employ_id = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('employ id'))
    store_location = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('store location'))
    timezone = models.CharField(
        max_length=10, choices=TIME_ZONE_CHOICES, null=True, blank=True, verbose_name=_('store timezone'))
    authenticated_code = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('authenticated code'))
    app_id = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('app id'))
    app_secret = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('app secret'))
    partner_key = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('partner key secret'))
    access_token = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('access token'))
    refresh_token = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('refresh token'))
    event_id = models.CharField(
        max_length=255, null=True, blank=True, verbose_name=_('event id'))

    class Meta:
        verbose_name = _("Restaurant Pos Details")
        verbose_name_plural = _("Restaurants Pos Details")
        ordering = ["-id"]

    def clean_timezone(self):
        timezone = self.timezone.strip()
        if not timezone.startswith('+') and not timezone.startswith('-'):
            raise ValidationError("Time zone must start with '+' or '-'")
        if not re.match(r'^[+-]\d{2}:\d{2}$', timezone):
            raise ValidationError(
                "Invalid time zone format. Use '+00:00' format.")
        return timezone

    def save(self, *args, **kwargs):
        self.timezone = self.clean_timezone()
        super(PosDetails, self).save(*args, **kwargs)

    def __str__(self) -> str:
        return f"Pos Details For --> {self.restaurant} --> {self.pos_type}"


class POS_logs(BaseModel):
    logs = models.TextField(null=True, blank=True, verbose_name=_('logs'))

    def __str__(self) -> str:
        return f"{self.id} created at --> {self.created_date} updated at --> {self.modified_date}"

    class Meta:
        verbose_name = _("Pos Log")
        verbose_name_plural = _("Pos Logs")
        ordering = ["-id"]
