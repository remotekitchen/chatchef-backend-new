from django.contrib import admin

from .models import Theme

@admin.register(Theme)
class ThemeAdmin(admin.ModelAdmin):
    list_display = ['restaurant', 'location', 'primary_color',
                    'secondary_color', 'is_active']
    search_fields = ['restaurant', 'location', 'is_active']
