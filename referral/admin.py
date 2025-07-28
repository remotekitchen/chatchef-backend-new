from django.contrib import admin

from referral.models import InviteCodes, Referral, StaffReferral

# Register your models here.
admin.site.register(Referral)
admin.site.register(StaffReferral)
admin.site.register(InviteCodes)
