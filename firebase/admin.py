from django.contrib import admin

from firebase.models import FirebasePushToken, NotificationTemplate, CompanyPushToken, PromotionalCampaign
from django.utils.translation import gettext_lazy as _
from django.utils.safestring import mark_safe
from django.utils.html import format_html

from django import forms
from django.forms import modelformset_factory, TimeInput
from  firebase.models import TokenFCM
@admin.register(FirebasePushToken)
class FirebasePushTokenAdmin(admin.ModelAdmin):
    list_display = ['user', 'push_token']
    search_fields = ['user__email']
    raw_id_fields = ['user']


@admin.register(CompanyPushToken)
class CompanyPushTokenAdmin(admin.ModelAdmin):
    list_display = ['company', 'push_token']
    search_fields = ['company__name']
    raw_id_fields = ['company']


@admin.register(NotificationTemplate)
class PushTemplateAdmin(admin.ModelAdmin):
    list_display = ['title', 'key', 'created_date']
    search_fields = ['key', 'title']
    readonly_fields = ['created_date', 'modified_date']
    fieldsets = [
        [_('Basic info'), {'fields': ['key', 'title']}],
        [_('Notification'), {
            'fields': ['notification_title', 'notification_body', 'notification_image', 'click_action'],
        }],
        [_('Data'), {'fields': ['data']}],
        [_('Important dates'), {'fields': ['created_date', 'modified_date']}],
    ]
    # actions = ['send_notification']

    # def send_notification(self, request, queryset):
    #     for template in queryset:
    #         send_notification_to_topic(template)
    #
    # send_notification.short_description = 'Send these notifications'



@admin.register(PromotionalCampaign)
class PromotionalCampaignAdmin(admin.ModelAdmin):
    list_display = ("title", "restaurant", "category", "is_active")  # Updated fields
    list_filter = ("is_active", "restaurant", "category")  # Added category filter
    search_fields = ("title", "restaurant__name", "category")  # Updated for correct fields

    fieldsets = (
        ("Campaign Details", {
            "fields": (
                "title",  # Mandatory
                "category",  # Moved below title (Mandatory)
                "message",  # Mandatory
                ("restaurant",),  # Optional
                "campaign_image",  # Optional
                "schedule_times",  # Mandatory
                "is_active",  # Mandatory
            ),
            "description": "Fields marked as (Mandatory) must be filled. (Optional) fields can be left blank."
        }),
    )

    

@admin.register(TokenFCM)
class TokenFCMAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'user_email', 'token', 'device_type')  # Remove 'updated_at' here
    search_fields = ('token', 'user__id', 'user__email')
    list_filter = ('device_type',)  # Remove 'updated_at' here

    def user_email(self, obj):
        return obj.user.email
    user_email.short_description = 'User Email'

    fieldsets = (
        ("Token Details", {
            "fields": (
                "user",  # Mandatory
                "token",  # Mandatory
                "device_type",  # Mandatory
            ),
            "description": "Fields marked as (Mandatory) must be filled. (Optional) fields can be left blank."
        }),
    )



