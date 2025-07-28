from django.contrib.auth import get_user_model
from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel
from food.models import Location, Restaurant

User = get_user_model()


class Referral(BaseModel):
    user = models.OneToOneField(
        User, on_delete=models.CASCADE, verbose_name=_("user"))
    invited_users = models.ManyToManyField(
        User, blank=True, verbose_name=_("invited users"), related_name="invited_users")
    joined_users = models.ManyToManyField(
        User, blank=True, verbose_name=_("joined users"), related_name="joined_user")
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_("restaurant"))
    location = models.ForeignKey(
        Location, on_delete=models.CASCADE, verbose_name=_("location"))

    class Meta:
        ordering = ["-id"]

    def __str__(self) -> str:
        return f"user --> {self.user.id} --> restaurant --> {self.restaurant.id}"


class StaffReferral(BaseModel):
    refer = models.ForeignKey(
        Referral, on_delete=models.CASCADE, verbose_name=_("refer"))
    total = models.PositiveIntegerField(default=0)
    month = models.PositiveIntegerField(default=0)
    today = models.PositiveIntegerField(default=0)

    def __str__(self) -> str:
        return f"{self.refer.user.first_name}"


class InviteCodes(BaseModel):
    class STATUS(models.TextChoices):
        PENDING = "pending"
        ACCEPTED = "accepted"

    refer = models.ForeignKey(
        Referral, on_delete=models.CASCADE, verbose_name=_("refer"))
    code = models.CharField(max_length=255, unique=True)
    status = models.CharField(
        max_length=10, choices=STATUS.choices, default=STATUS.PENDING)

    def __str__(self) -> str:
        return f"{self.refer.id}"
