import uuid

from django.contrib import admin
from django.contrib.admin import display
from django.forms import CharField, Textarea

from billing.models import (BillingProfile, DeliveryFeeAssociation,
                            ExternalPaymentInformation, Invoice, InvoiceItem,
                            Order, OrderedModifiers, OrderItem,
                            OrderModifiersItems, OrderReminder, PaymentDetails,
                            PaymentMethods, PayoutHistory,PayoutHistoryForHungry,
                            PaypalCapturePayload, Purchase, RaiderAppAuth,
                            RestaurantFee, StripeCapturePayload,
                            StripeConnectAccount, Transactions, UberAuthModel,
                            Wallet, UnregisteredGiftCard, RestaurantContract)

admin.site.register(PaypalCapturePayload)


@admin.register(BillingProfile)
class BillingProfileAdmin(admin.ModelAdmin):
    list_display = ['company', 'payout_account_id',
                    'last_payout_date', 'payout_frequency']

    @display(description='Company')
    def company(self, obj: BillingProfile):
        return obj.company.name


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ['order_id', 'is_paid', 'customer', 'status', 'order_method', "delivery_platform","ubereats_requested_time",
                    'restaurant', 'subtotal', 'receive_date']
    actions = ['duplicate']
    search_fields = ['customer', 'order_id']
    list_filter = ['order_method', 'status']

    @admin.action(description="Duplicate order")
    def duplicate(self, request, queryset):
        for order in queryset:
            order.pk = None
            order.order_id = uuid.uuid4()
            order.save()



@admin.register(OrderItem)
class OrderItemAdmin(admin.ModelAdmin):
    list_display = ['order', 'menu_item', 'quantity']


@admin.register(PaymentDetails)
class PaymentDetailsAdmin(admin.ModelAdmin):
    pass


@admin.register(Purchase)
class PaymentDetailsAdmin(admin.ModelAdmin):
    pass


@admin.register(StripeCapturePayload)
class PaymentDetailsAdmin(admin.ModelAdmin):
    pass


@admin.register(Invoice)
class InvoiceAdmin(admin.ModelAdmin):
    pass


@admin.register(InvoiceItem)
class InvoiceItemAdmin(admin.ModelAdmin):
    pass


@admin.register(DeliveryFeeAssociation)
class DeliveryFeeAssociationAdmin(admin.ModelAdmin):
    pass


@admin.register(StripeConnectAccount)
class StripeConnectAccountAdmin(admin.ModelAdmin):
    pass


@admin.register(RestaurantFee)
class RestaurantFeeAdmin(admin.ModelAdmin):
    list_display = [
        'restaurant',
        'max_distance',
        'delivery_fee',
        'service_fee'
    ]
    
@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ['user', 'balance', 'wallet_address', 'restaurant']
    search_fields = ['user__email', 'user__phone']
    

@admin.register(PayoutHistoryForHungry)
class PayoutHistoryForHungryAdmin(admin.ModelAdmin):
    list_display = ('id', 'restaurant', 'location', 'payout_amount', 'net_revenue', 'gross_revenue', 'is_paid', 'is_mailed')
    list_filter = ('is_paid', 'is_mailed', 'restaurant', 'location')
    search_fields = ('restaurant__name', 'location__name', 'id')
    readonly_fields = ('id',)  # ID should not be editable
    ordering = ('-id',)  # Ordering based on ID (default is descending)

    fieldsets = (
        ('Basic Info', {
            'fields': ('restaurant', 'location', 'orders', 'statement_start_date', 'statement_end_date')
        }),
        ('Financial Details', {
            'fields': ('payout_amount', 'net_revenue', 'gross_revenue', 'amount_to_restaurant', 'ht_profit', 'selling_price_inclusive_of_tax', 'commission_percentage', 'commission_amount', 'service_fee_to_restaurant', 'service_fee_to_hungrytiger')
        }),
        ('Delivery & Discounts', {
            'fields': ('delivery_fees', 'delivery_fees_expense', 'customer_absorbed_delivery_fees', 'tax', 'discount', 'bogo_discount', 'restaurant_discount')
        }),
        ('Additional Fees & Adjustments', {
            'fields': ('container_fees', 'bag_fees', 'tips_for_restaurant', 'adjustments', 'adjustments_note')
        }),
        ('Invoices & Status', {
            'fields': ('invoice', 'pdf', 'is_paid', 'is_mailed')
        }),
    )

    actions = ['mark_as_paid', 'mark_as_mailed']

    def mark_as_paid(self, request, queryset):
        queryset.update(is_paid=True)
    mark_as_paid.short_description = "Mark selected payouts as Paid"

    def mark_as_mailed(self, request, queryset):
        queryset.update(is_mailed=True)
    mark_as_mailed.short_description = "Mark selected payouts as Mailed"
# test comment


@admin.register(RestaurantContract)
class RestaurantContractAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'restaurant',
        'commission_percentage',
        'bogo_bear_by_restaurant',
        'restaurant_discount_percentage',
        'restaurant_accepted_discount',       # ✅ Show accepted discount in list view
        'restaurant_voucher_codes_display',   # ✅ Show a readable version of voucher codes
    )
    
    list_filter = ('restaurant',)
    search_fields = ('restaurant__name',)

    # ✅ Optional: Custom method to display voucher codes in list view
    def restaurant_voucher_codes_display(self, obj):
        if isinstance(obj.restaurant_voucher_codes, list):
            return ", ".join(obj.restaurant_voucher_codes)
        return "-"
    restaurant_voucher_codes_display.short_description = "Voucher Codes"

    # ✅ Optional: Customize the admin form field to use a TextArea widget
    # where users can enter a JSON array like ["HT10", "SUMMER20"]
    def formfield_for_dbfield(self, db_field, request, **kwargs):
        # ✅ Customize only the 'restaurant_voucher_codes' field
        if db_field.name == "restaurant_voucher_codes":
            kwargs["widget"] = Textarea(attrs={"rows": 3, "cols": 60})
            return CharField(required=False, **kwargs)
        return super().formfield_for_dbfield(db_field, request, **kwargs)



admin.site.register(OrderedModifiers)
admin.site.register(OrderModifiersItems)
admin.site.register(ExternalPaymentInformation)
admin.site.register(OrderReminder)
admin.site.register(Transactions)
admin.site.register(PaymentMethods)
admin.site.register(UberAuthModel)
admin.site.register(PayoutHistory)
admin.site.register(RaiderAppAuth)
admin.site.register(UnregisteredGiftCard)

