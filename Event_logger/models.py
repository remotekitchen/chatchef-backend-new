from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel
from food.models import Location, Restaurant


# Create your models here.
class Action_logs(BaseModel):
    action = models.CharField(
        max_length=255, verbose_name=_('action'), null=True, blank=True)
    restaurant = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_('restaurant'), null=True, blank=True)
    location = models.ForeignKey(
        Location, on_delete=models.CASCADE, verbose_name=_('location'), null=True, blank=True)
    logs = models.TextField(null=True, blank=True, verbose_name=_('logs'))

    def __str__(self) -> str:
        return f"action log -> {self.id}"

    class Meta:
        verbose_name = _("Action Log")
        verbose_name_plural = _("Action Logs")
        ordering = ["-id"]


def actionDeleter():
    print('running')
    actions = Action_logs.objects.all()[:900]
    for action in actions:
        action.delete()
        print('deleted')
