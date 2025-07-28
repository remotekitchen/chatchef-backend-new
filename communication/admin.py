from django.contrib import admin

from communication.models import GroupInvitationOR,CustomerInfo,whatsAppCampaignHistory

# Register your models here.
admin.site.register(GroupInvitationOR)
admin.site.register(CustomerInfo)
admin.site.register(whatsAppCampaignHistory)
