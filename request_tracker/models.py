from django.contrib.auth import get_user_model
from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel

User = get_user_model()


# Create your models here.
class RequestTracker(BaseModel):
    user = models.ForeignKey(User, on_delete=models.CASCADE,
                             blank=True, null=True, verbose_name=_("requested user"))
    path = models.CharField(max_length=255, blank=True,
                            null=True, verbose_name=_("requested path"))
    method = models.CharField(
        max_length=10, blank=True, null=True, verbose_name=_("requested method"))
    status_code = models.CharField(
        max_length=10, blank=True, null=True, verbose_name=_("request status code"))
    request_headers = models.TextField(
        blank=True, null=True, verbose_name=_("requested header"))
    request_body = models.TextField(
        blank=True, null=True, verbose_name=_("requested body"))
    response_headers = models.TextField(
        blank=True, null=True, verbose_name=_("response headers"))
    response_body = models.TextField(
        blank=True, null=True, verbose_name=_("response body"))
    ip = models.CharField(max_length=50, blank=True,
                          null=True, verbose_name=_("request ip"))
    mac = models.CharField(max_length=100, blank=True,
                           null=True, verbose_name=_("requested mac"))
    browser = models.CharField(
        max_length=255, blank=True, null=True, verbose_name=_("requested browser"))
    os = models.CharField(max_length=255, blank=True,
                          null=True, verbose_name=_("requested os"))

    def __str__(self):
        return f"{self.method} {self.path} - {self.status_code} at - {self.created_date}"

    class Meta:
        ordering = ["-id"]
