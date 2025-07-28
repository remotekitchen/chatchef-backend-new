from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel


class ImageUrl(BaseModel):
    dish_name = models.TextField(verbose_name=_('Dish name'), blank=True, null=True)
    dish_description = models.TextField(verbose_name=_('Dish description'), blank=True, null=True)
    price = models.TextField(verbose_name=_('Price'), blank=True, null=True)
    weblink = models.TextField(verbose_name=_('Web link'), blank=True, null=True)
    drivelink = models.TextField(verbose_name=_('Drive link'), blank=True, null=True)
    store_name = models.TextField(verbose_name=_('Store Name'), blank=True, null=True)
    platform = models.TextField(verbose_name=_('Platform'), blank=True, null=True)
    rating = models.TextField(verbose_name=_('Rating'), blank=True, null=True)
    star = models.TextField(verbose_name=_('Star'), blank=True, null=True)
    address = models.TextField(verbose_name=_('Address'), blank=True, null=True)
    store_link = models.TextField(verbose_name=_('Store link'), blank=True, null=True)
    cuisine_category = models.TextField(verbose_name=_('Cuisine category'), blank=True, null=True)

    class Meta:
        verbose_name = "Image Url"
        verbose_name_plural = "Image Urls"

    def __str__(self):
        return self.dish_name
