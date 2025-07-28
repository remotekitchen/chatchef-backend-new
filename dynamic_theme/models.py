from django.db import models
from django.utils.translation import gettext_lazy as _

from food.models import Location, Restaurant


class Theme(models.Model):
    restaurant = models.ForeignKey(
        Restaurant,
        verbose_name=_("Restaurant"),
        related_name="restaurant_theme",
        on_delete=models.CASCADE,
    )
    location = models.ForeignKey(
        Location,
        verbose_name=_("location"),
        related_name="location_theme",
        on_delete=models.CASCADE,
    )
    # Common properties

    primary_color = models.CharField(
        max_length=150, default="pink", blank=True, null=True)
    secondary_color = models.CharField(
        max_length=150, default="#F7DDA0", blank=True, null=True)
    positive_color = models.CharField(
        max_length=150, default="green", blank=True, null=True)
    danger_color = models.CharField(
        max_length=150, default="red", blank=True, null=True)
    warning_color = models.CharField(
        max_length=150, default="yellow", blank=True, null=True)
    card_color = models.CharField(
        max_length=150, default="#fff", blank=True, null=True)
    background_color = models.CharField(
        max_length=150, default="#e8e8e8", blank=True, null=True)
    text_color = models.CharField(
        max_length=150, default="#000", blank=True, null=True)
    stock_color = models.CharField(
        max_length=150, default="#fff", blank=True, null=True)
    disable_color = models.CharField(
        max_length=150, default="gray", blank=True, null=True)
    button_text_color = models.CharField(
        max_length=150, default="#fff", blank=True, null=True)
    button_hover_text_color = models.CharField(
        max_length=150, default="#000", blank=True, null=True)
    button_hover_bg_color = models.CharField(
        max_length=150, default="#F7DDA0", blank=True, null=True)
    button_bg_color = models.CharField(
        max_length=150, default="red", blank=True, null=True)
    is_active = models.BooleanField(_("is_active"), default=False)
    
    


    def __str__(self):
        return f"Python Theme  for restaurant:{self.restaurant} -- location:{self.location}"
