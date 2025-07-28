from django.contrib import admin

from integration.models import Onboarding, Platform


@admin.register(Platform)
class PlatformAdmin(admin.ModelAdmin):
    list_display = ['name', 'client_id', 'client_secret']


admin.site.register(Onboarding)
