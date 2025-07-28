from django.contrib.auth import get_user_model
from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel
from food.models import Location, Restaurant

User = get_user_model()


class VisitorAnalytics(BaseModel):
    class VISITOR_SOURCE_TYPE(models.TextChoices):
        FACEBOOK = 'facebook', _("FACEBOOK")
        WHATS_APP = 'whats_app', _("WHATS_APP")
        INSTAGRAM = "instagram", _("INSTAGRAM")
        GOOGLE = "google", _("GOOGLE")
        TABLE = 'table', _("TABLE")
        BANNER = 'banner', _("BANNER")
        BUSINESS_CARD = 'business_card', _("BUSINESS_CARD")
        POSTER = 'poster', _("POSTER")
        FLYER = 'flyer', _("FLYER")
        COUPON = "coupon", _("COUPON")
        NA = "na", _("NA")

    class COUNT_TYPE(models.TextChoices):
        DO = "do", _("DO")
        CART = "cart", _("CART")
        ORDER_CONFIRM = "order_confirm", _("ORDER_CONFIRM")
        PAYMENT_CONFIRM = "payment_confirm", _("PAYMENT_CONFIRM")

    user = models.ForeignKey(
        User, verbose_name=_("User"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_("restaurant"))
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )
    source = models.CharField(
        max_length=50, choices=VISITOR_SOURCE_TYPE.choices, default=VISITOR_SOURCE_TYPE.NA, verbose_name="source")
    count = models.CharField(
        max_length=50, choices=COUNT_TYPE.choices, default=COUNT_TYPE.DO, verbose_name=_("count"))

    def __str__(self) -> str:
        return f"{self.id}"
