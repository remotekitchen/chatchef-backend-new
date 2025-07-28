from django.db import models
from django.utils.translation import gettext_lazy as _

from core.models import BaseModel

# Create your models here.


class OtterAuth(BaseModel):
    token = models.CharField(max_length=1500,
                             verbose_name=_("otter auth token"))

    def __str__(self):
        return f"token {self.id} --> {self.created_date}"
