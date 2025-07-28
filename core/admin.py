from django.contrib import admin

from .models import Address, AppStore, Constant


@admin.register(Constant)
class ConstantAdmin(admin.ModelAdmin):
    list_display = ['key', 'value']
    search_fields = ['key', 'value']


admin.site.register(AppStore)
admin.site.register(Address)
