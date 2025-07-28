from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel
from food.models import Restaurant


# Create your models here.


class GroupInvitationOR(BaseModel):
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_("restaurant"))
    platform = models.CharField(max_length=255, verbose_name=_("platform"))
    name = models.CharField(max_length=255, verbose_name=_("name"))
    group_link = models.CharField(
        max_length=255, verbose_name=_("group link"), unique=True)
    is_active = models.BooleanField(default=False, verbose_name=_("is active"))

    def __str__(self) -> str:
        return f"group link for --> {self.restaurant.name}"


class CustomerInfo(BaseModel):
    restaurant = models.ManyToManyField(Restaurant, verbose_name=_("restaurant"), blank=True)
    name = models.CharField(max_length=255, verbose_name=_("name"), blank=True)
    contact_no = models.CharField(max_length=15, verbose_name=_("contact"))
    email = models.CharField(max_length=50, verbose_name=_('mail'), blank=True)
    is_subscribed = models.BooleanField(default=True, verbose_name=_('is subscribed'))
    is_memeber= models.BooleanField(default=False, verbose_name=_("is_member"))

class whatsAppCampaignHistory(BaseModel):
    restaurant=models.ManyToManyField(Restaurant,verbose_name=_("restaurant"), blank=False)
    audience=models.CharField(max_length=200, verbose_name=_("audience"),blank=True,null=True)
    msg_header=models.CharField(max_length=59, verbose_name=_("header"),blank=True,null=True)
    msg_type=models.CharField(max_length=255, verbose_name=_("msg_type"), blank=True,null=True)
    img_link=models.CharField(max_length=255, verbose_name=_("img _link"), blank=True,null=True)
    msg_to = models.CharField(max_length=255, verbose_name=_("msg_to"), blank=True,null=True)
    body = models.CharField(max_length=255, verbose_name=_("body"), blank=True,null=True)
    url= models.CharField(max_length=255, verbose_name=_("url"), blank=True,null=True)
    time=models.DateTimeField(verbose_name=_("time"),blank=True,null=True)


class EmailCampaignHistory(BaseModel):
    restaurant=models.ManyToManyField(Restaurant,verbose_name=_("restaurant"), blank=False)
    audience=models.CharField(max_length=200, verbose_name=_("audience"),blank=True,null=True)
    msg_from=models.CharField(max_length=59, verbose_name=_("msg_from"),blank=True,null=True)
    from_name=models.CharField(max_length=59, verbose_name=_("from_name"),blank=True,null=True)
    msg_to=models.CharField(max_length=255, verbose_name=_("msg_to"), blank=True,null=True)
    subject=models.CharField(max_length=255, verbose_name=_("subject"), blank=True,null=True)
    textPart = models.CharField(max_length=255, verbose_name=_("textPart"), blank=True,null=True)
    htmlPart = models.CharField(max_length=255, verbose_name=_("htmlPart"), blank=True,null=True)
    url= models.CharField(max_length=255, verbose_name=_("url"), blank=True,null=True)
    time=models.DateTimeField(verbose_name=_("time"),blank=True,null=True)