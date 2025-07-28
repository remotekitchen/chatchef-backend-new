from django.contrib import admin

from .models import POS_logs, PosDetails

# Register your models here.
admin.site.register(PosDetails)
admin.site.register(POS_logs)
