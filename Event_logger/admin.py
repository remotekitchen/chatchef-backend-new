from django.contrib import admin

from .models import Action_logs


@admin.register(Action_logs)
class OrderItemAdmin(admin.ModelAdmin):
    list_display = ['action', 'restaurant', 'location']
