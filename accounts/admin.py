from django.contrib import admin
from django.contrib.admin import display
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.translation import gettext_lazy as _

from accounts.models import Company, Otp, RestaurantUser, User, UserAddress,BlockedPhoneNumber
from accounts.models import  QRScan
from accounts.models import Customer, Subscription, CancellationRequest


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    list_display = ['email', 'first_name',
                    'last_name', 'company', 'role', 'is_staff','is_blocked','hotel_admin' , 'hotel_count']
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("email", "password1", "password2", "referred_by"),
            },
        ),
    )
    fieldsets = [
        [None, {'fields': ['password']}],
        [_('Personal info'), {'fields': [
            'first_name', 'last_name', 'email', "phone","referred_by","order_count_total_rk"]}],
        [_('Permissions'), {
            'fields': ['is_email_verified', 'is_phone_verified', 'is_active', 'is_staff', 'is_superuser','is_blocked', 'groups', 'user_permissions'],
        }],
        [_('Company info'), {
            'fields': ['company', 'role', 'is_sales'],
        }],
           [_('Special Flags'), {
            'fields': ['super_power', 'is_get_600','hotel_admin'],
        }],
        [_('Important dates'), {'fields': ['last_login', 'date_joined']}],
    ]
    date_hierarchy = 'date_joined'
    ordering = ['-id']
    search_fields = ("first_name", "last_name", "email")

    @admin.display(description="Hotel Count")
    def hotel_count(self, obj):
        return obj.hotel_count

@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ['name', 'owner']

    @display(description='Owner')
    def owner(self, obj: Company):
        return ', '.join(list(obj.user_set.values_list('email', flat=True)))


@admin.register(RestaurantUser)
class RestaurantUserAdmin(admin.ModelAdmin):
    list_display = ['restaurant', 'user', 'rewards_category',
                    'reward_points', 'points_spent', 'next_level', 'remain_point']
    search_fields = ['user__email']

    @display(description='Restaurant')
    def restaurant(self, obj: RestaurantUser):
        return obj.Restaurant.name

    @display(description='User')
    def user(self, obj: RestaurantUser):
        return obj.user.email


@admin.register(UserAddress)
class UserAddressAdmin(admin.ModelAdmin):
    list_display = ['user', 'street_number', 'street_name', 'city', 'country']
    search_fields = ['user__username']


@admin.register(Otp)
class OtpAdmin(admin.ModelAdmin):
    list_display = ['otp', 'phone', 'is_used']
    search_fields = ['phone']



@admin.register(BlockedPhoneNumber)
class BlockedPhoneNumberAdmin(admin.ModelAdmin):
    list_display = ("phone", "reason", "created_at")
    search_fields = ("phone",)
@admin.register(QRScan)
class QRScanAdmin(admin.ModelAdmin):
    list_display = ('ref', 'device_id', 'ip_address', 'timestamp')
    search_fields = ('ref', 'device_id', 'ip_address', 'user_agent')
    list_filter = ('ref', 'timestamp')
    ordering = ('-timestamp',)



@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ("email", "name", "stripe_customer_id")
    search_fields = ("email", "name", "stripe_customer_id")


@admin.register(Subscription)
class SubscriptionAdmin(admin.ModelAdmin):
    list_display = ("customer", "plan_id", "status", "current_period_start", "current_period_end")
    search_fields = ("customer__email", "plan_id", "stripe_subscription_id")
    list_filter = ("status",)


@admin.register(CancellationRequest)
class CancellationRequestAdmin(admin.ModelAdmin):
    list_display = ("subscription", "status", "created")
    search_fields = ("subscription__customer__email", "reason", "details")
    list_filter = ("status", "created")