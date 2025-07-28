from django.contrib import admin

from marketing.models import (BirthdayGift, Bogo, BxGy, ContactUsData, DemoData,
                              Duration, EmailConfiguration, EmailHistory,
                              FissionCampaign, FissionPrize, GiftCard,
                              LoyaltyProgram, Rating, Review, SalesMail,
                              SpendXSaveY, SpendXSaveYManager,
                              SpendXSaveYPromoOption, Voucher,PlatformCouponExpiryLog, Comment, AutoReplyToComments)


@admin.register(Duration)
class DurationAdmin(admin.ModelAdmin):
    pass


@admin.register(SpendXSaveYManager)
class SpendXSaveYManagerAdmin(admin.ModelAdmin):
    pass


@admin.register(SpendXSaveY)
class SpendXSaveYAdmin(admin.ModelAdmin):
    list_display = ['min_spend', 'save_amount']


@admin.register(Voucher)
class VoucherAdmin(admin.ModelAdmin):
    list_display = ['voucher_code', 'get_applied_users', 'amount']
    search_fields = ['voucher_code']

    def get_applied_users(self, obj):
        users = obj.applied_users.all()
        return ", ".join([user.email for user in users]) if users else "-"
    get_applied_users.short_description = "Applied Users"

@admin.register(PlatformCouponExpiryLog)
class PlatformCouponExpiryLogAdmin(admin.ModelAdmin):
    pass

@admin.register(Comment)
class CommentAdmin(admin.ModelAdmin):
    pass
@admin.register(Bogo)
class BogoAdmin(admin.ModelAdmin):
    pass
@admin.register(BxGy)
class BxGyAdmin(admin.ModelAdmin):
    pass

@admin.register(SpendXSaveYPromoOption)
class SpendXSaveYPromoOptionAdmin(admin.ModelAdmin):
    pass


@admin.register(LoyaltyProgram)
class LoyaltyProgramAdmin(admin.ModelAdmin):
    pass


@admin.register(FissionPrize)
class FissionPrizeAdmin(admin.ModelAdmin):
    pass


admin.site.register(FissionCampaign)


@admin.register(BirthdayGift)
class BirthdayGiftAdmin(admin.ModelAdmin):
    pass


@admin.register(GiftCard)
class GiftCardAdmin(admin.ModelAdmin):
    pass


admin.site.register(Rating)
admin.site.register(Review)
admin.site.register(EmailConfiguration)
admin.site.register(EmailHistory)
admin.site.register(ContactUsData)
admin.site.register(DemoData)
admin.site.register(SalesMail)
admin.site.register(AutoReplyToComments)
