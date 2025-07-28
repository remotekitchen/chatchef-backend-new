from django.contrib import admin
from .models import *


@admin.register(RewardGroup)
class RewardGroupAdmin(admin.ModelAdmin):
    search_fields = ["name"]



@admin.register(Reward)
class RewardAdmin(admin.ModelAdmin):
    search_fields = ['reward_group__name']



@admin.register(AdditionalCondition)
class AdditionalConditionAdmin(admin.ModelAdmin):
    pass


@admin.register(UserReward)
class UserRewardAdmin(admin.ModelAdmin):
    list_display = ['user', 'reward']
    search_fields = ['user__email', 'code']

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('user', 'reward')


@admin.register(LocalDeal)
class LocalDealAdmin(admin.ModelAdmin):
    pass

@admin.register(RewardManage)
class RewardManageAdmin(admin.ModelAdmin):
    pass


@admin.register(NotificationLog)
class NotificationLogAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "get_user_email",
        "tier",
        "channel",
        "sent_at",
        "status",
    )

    def get_user_email(self, obj):
        return obj.user.email
    get_user_email.short_description = "User Email"


@admin.register(RetentionConfig)
class RetentionConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'reward_group', 'is_active', 'created_at')
    list_filter = ('is_active', 'created_at')
    search_fields = ('reward_group__name',)
    ordering = ('-created_at',)
    list_per_page = 50