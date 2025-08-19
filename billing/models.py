import hashlib
import uuid
from datetime import timedelta
import qrcode
from io import BytesIO
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.utils.translation import gettext_lazy as _
import django.conf
import pytz
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from threading import Thread
from accounts.models import Company, RestaurantUser, User
from core.models import Address, BaseModel
from food.models import POS_DATA, Location, MenuItem, ModifierGroup, Restaurant
from integration.models import Platform
from marketing.models import Bogo, SpendXSaveY, Voucher, BxGy
from reward.models import UserReward, LocalDeal
from rest_framework import serializers
import random
# from billing.utilities.oms_notification import send_order_cancelled_notification_helper
from django.db.models import JSONField  # Django 3.1+ for generic DBs
import json
from django.utils import timezone
import json, logging

from firebase.models import CompanyPushToken


from firebase.utils.fcm_helper import FCMHelper
from firebase.models import Platform as FirebasePlatform
from django.db import transaction
from django.db.models import Q, F
# rider_ratings/models.py
from django.conf import settings
from django.db import models
from django.db.models import UniqueConstraint
from django.utils import timezone


import string
from firebase.utils.fcm_helper import  get_dynamic_message, send_push_notification
logger = logging.getLogger(__name__)


class BillingProfile(BaseModel):
    class PayoutFrequency(models.IntegerChoices):
        DAILY = 1, _("DAILY")
        WEEKLY = 7, _("WEEKLY")

    company = models.OneToOneField(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    stripe_connect_account = models.OneToOneField(
        "StripeConnectAccount",
        verbose_name=_("Stripe Connect Account"),
        on_delete=models.SET_NULL,
        null=True,
    )
    payout_account_id = models.TextField(
        verbose_name=_("Payout Account ID"), blank=True
    )
    last_payout_date = models.DateTimeField(
        verbose_name=_("Last Payout Date"), default=timezone.now, blank=True
    )
    payout_frequency = models.IntegerField(
        verbose_name=_("Payout Frequency"),
        choices=PayoutFrequency.choices,
        default=PayoutFrequency.WEEKLY,
        blank=True,
    )

    currency = models.CharField(
        max_length=10, verbose_name=_("currency"), default="USD", blank=True
    )

    class Meta:
        verbose_name = _("Billing Profile")
        verbose_name_plural = _("Billing Profiles")

    def __str__(self):
        return self.company.name


class Order(BaseModel):
    class StatusChoices(models.TextChoices):
        
        PENDING = "pending", _("PENDING")
        ACCEPTED = "accepted", _("ACCEPTED")
        SCHEDULED_ACCEPTED = "scheduled_accepted", _("SCHEDULED_ACCEPTED")
        NOT_READY_FOR_PICKUP = "not_ready_for_pickup", _(
            "NOT_READY_FOR_PICKUP"
        )
        WAITING_FOR_DRIVER = "waiting_for_driver", _("WAITING_FOR_DRIVER")
        DRIVER_ASSIGNED = 'driver_assigned', _('DRIVER_ASSIGNED')
        READY_FOR_PICKUP = "ready_for_pickup", _("READY_FOR_PICKUP")
        RIDER_CONFIRMED = "rider_confirmed", _("RIDER_CONFIRMED")
        RIDER_CONFIRMED_PICKUP_ARRIVAL = "rider_confirmed_pickup_arrival", _(
            "RIDER_CONFIRMED_PICKUP_ARRIVAL"
        )
        RIDER_ON_THE_WAY = "rider_on_the_way", _("RIDER_ON_THE_WAY")
        RIDER_PICKED_UP = "rider_picked_up", _("RIDER_PICKED_UP")
        RIDER_CONFIRMED_DROPOFF_ARRIVAL = "rider_confirmed_dropoff_arrival", _(
            "RIDER_CONFIRMED_DROPOFF_ARRIVAL"
        )
        COMPLETED = "completed", _("COMPLETED")
        CANCELLED = "cancelled", _("CANCELLED")
        REJECTED = "rejected", _("REJECTED")
        MISSING = "missing", _("MISSING")
        NA = "n/a", "N/A"

    class RefundStatusChoices(models.TextChoices):
        APPLICABLE = "applicable", _("Applicable") # Default state
        REQUESTED = "requested", _("Requested")
        APPROVED = "approved", _("Approved")
        NOT_APPLICABLE = "not_applicable", _("Not_Applicable")  
        IN_PROCESS = "in_process", _("In_Process")
        REFUNDED = "refunded", _("Refunded")
        DECLINED = "declined", _("Declined")


    # class StatusBeforeCancelled(StatusChoices):
    #     NA = 'n/a', _('N/A')

    class DeliveryPlatform(models.TextChoices):
        DOORDASH = "doordash", _("DOORDASH")
        UBEREATS = "ubereats", _("UBEREATS")
        RAIDER_APP = "raider_app", _("RAIDER_APP")
        NA = "n/a", "N/A"

    class OrderMethod(models.TextChoices):
        # SCHEDULED = 'scheduled', _('SCHEDULED')
        DELIVERY = "delivery", _("DELIVERY")
        RESTAURANT_DELIVERY = "restaurant_delivery", _("RESTAURANT_DELIVERY")
        PICKUP = "pickup", _("PICKUP")
        DINE_IN = "dine_in", _("DINE_IN")
        LOCAL_DEAL = "local_deal", _("LOCAL_DEAL")

    class SchedulingType(models.TextChoices):
        ASAP = "asap", _("ASAP")
        FIXED_TIME = "fixed_time", _("FIXED_TIME")

    class OrderingType(models.TextChoices):
        INTERNAL = "internal", _("INTERNAL")
        EXTERNAL = "external", _("EXTERNAL")

    class PaymentMethod(models.TextChoices):
        STRIPE = "stripe", _("STRIPE")
        CARD = "card", _("CARD")
        PAYPAL = "paypal", _("PAYPAL")
        CASH = "cash", _("CASH")
        WALLET = "wallet", _("WALLET")

    def validate_future_date(value, instance=None):
        value_utc = value.astimezone(pytz.utc)
        if value_utc and value_utc <= timezone.now() + timedelta(minutes=30):
            raise ValidationError(_("Scheduled time must be in the future."))
        return value_utc

    user = models.ForeignKey(
        User, verbose_name=_("user"), on_delete=models.SET_NULL, null=True, blank=True
    )
    customer = models.TextField(verbose_name=_("customer"), blank=True)
    email = models.EmailField(verbose_name=_("email"), blank=True)

    company = models.ForeignKey(
        Company, verbose_name=_("company"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )

    order_id = models.UUIDField(
        verbose_name=_("Order ID"), default=uuid.uuid4, unique=True
    )
    doordash_external_delivery_id = models.UUIDField(
        verbose_name=_("Doordash External Delivery ID"), default=uuid.uuid4, unique=True
    )
    uber_delivery_id = models.TextField(
        verbose_name=_('Uber Delivery ID'), blank=True
    )
    status = models.CharField(
        max_length=50,
        verbose_name=_("Status"),
        choices=StatusChoices.choices,
        default=StatusChoices.PENDING,
    )
    status_before_cancelled = models.CharField(
        max_length=50,
        verbose_name=_("Status before cancelled"),
        choices=StatusChoices.choices,
        default=StatusChoices.NA,
        blank=True,
    )
    purchase = models.ForeignKey(
        "Purchase",
        verbose_name=_("Purchase"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    refund_amount = models.FloatField(
        verbose_name=_("Refund Amount"), default=0, blank=True
    )
    refund_reason = models.TextField(
        verbose_name=_("Refund Reason"), blank=True
    )
    refund_status = models.CharField(
        max_length=20,
        verbose_name=_("Refund Status"),
        choices=RefundStatusChoices.choices,
        default=RefundStatusChoices.APPLICABLE,  
    )
    quantity = models.PositiveIntegerField(
        verbose_name=_("Quantity"), default=1
    )
    subtotal = models.FloatField(verbose_name=_("Subtotal"), default=0)
    delivery_fee = models.FloatField(verbose_name=_("Delivery fee"), default=0)
    original_delivery_fee = models.FloatField(
        verbose_name=_("original delivery fee"), default=0
    )
    delivery_discount = models.FloatField(
        verbose_name=_("Delivery discount"), default=0
    )
    tax = models.FloatField(verbose_name=_("Tax"), default=0)
    convenience_fee = models.FloatField(
        verbose_name=_("Convenience Fee"), default=0
    )
    discount = models.FloatField(verbose_name=_("Discount"), default=0)
    discount_hungrytiger = models.FloatField(
    verbose_name=_("HungryTiger Discount"), null=True, blank=True, default=0
    )
    bogo_discount = models.FloatField(
        verbose_name=_("bogo discount"), default=0)
    bxgy_discount = models.FloatField(
        verbose_name=_("bxgy discount, "), default=0)
    reward_points = models.PositiveIntegerField(default=0)
    stripe_fee = models.FloatField(verbose_name=_("Stripe Fee"), default=0)
    total = models.FloatField(verbose_name=_("Total"), default=0)
    tips = models.FloatField(verbose_name=_("Tips"), default=0)
    tips_for_restaurant = models.FloatField(
        verbose_name=_("Tips for restaurant"), default=0
    )
    currency = models.CharField(
        max_length=200, verbose_name=_("Currency"), default="cad"
    )
    bag_price = models.FloatField(default=0, verbose_name=_("bag price"))
    is_bag = models.BooleanField(default=False, verbose_name=_("is bag"))
    utensil_quantity = models.PositiveIntegerField(
        default=0, verbose_name=_("utensil quantity")
    )
    utensil_price = models.FloatField(
        default=0, verbose_name=_("utensil price")
    )
    is_paid = models.BooleanField(verbose_name=_("Is paid"), default=False)

    receive_date = models.DateTimeField(
        verbose_name=_("Receive Date"), default=timezone.now
    )
    receive_date_ht = models.DateTimeField(
        verbose_name=_("Receive Date (HT)"),
        blank=True,
        null=True,
        help_text=_("Additional receive date for HT (HungryTiger) or specific purpose")
    )
    pickup_time = models.DateTimeField(
        verbose_name=_("Pickup Time"), blank=True, null=True
    )
    delivery_time = models.DateTimeField(
        verbose_name=_("Delivery time"), blank=True, null=True
    )
    restaurant_accepted_time = models.DateTimeField(
    verbose_name=_("Restaurant Accepted Time"), blank=True, null=True
    )

    rider_accepted_time = models.DateTimeField(
        verbose_name=_("Rider Accepted Time"), blank=True, null=True
    )

    rider_pickup_time = models.DateTimeField(
        verbose_name=_("Rider Pickup Time"), blank=True, null=True
    )
    solid_voucher_code = models.CharField(
        max_length=200,
        verbose_name=_("Solid Voucher Code"),
        blank=True,
        null=True
    )
    ubereats_requested_time = models.DateTimeField(null=True, blank=True)

    
    prep_time = models.IntegerField(
        verbose_name=_('Prep time'), default=30  # prep_time in minutes
    )
    is_completed_by_restaurant = models.BooleanField(default=False)

    delivery_platform = models.CharField(
        max_length=30,
        verbose_name=_("Delivery platform"),
        choices=DeliveryPlatform.choices,
        default=DeliveryPlatform.NA,
        blank=True,
    )

    # Addresses
    pickup_address = models.TextField(
        verbose_name=_("Pickup Address"), blank=True
    )
    dropoff_address = models.TextField(
        verbose_name=_("Dropoff Address"), blank=True
    )
    pickup_address_details = models.ForeignKey(
        Address,
        verbose_name=_("Pickup Address Details"),
        related_name="order_pickup",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    dropoff_address_details = models.ForeignKey(
        Address,
        verbose_name=_("Dropoff Address Details"),
        related_name="order_dropoff",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    dropoff_location = models.JSONField(
        verbose_name=_("Dropoff location"), default=dict
    )
    dropoff_phone_number = models.CharField(
        max_length=20, verbose_name=_("Dropoff phone number"), blank=True
    )
    dropoff_contact_first_name = models.TextField(
        verbose_name=_("Dropoff contact first name"), blank=True
    )
    dropoff_contact_last_name = models.TextField(
        verbose_name=_("Dropoff contact last name"), blank=True
    )

    # Delivery platform data
    tracking_url = models.URLField(verbose_name=_("Tracking URL"), blank=True)
    support_reference = models.CharField(
        max_length=100, verbose_name=_("Support Reference"), blank=True
    )
    dasher_dropoff_phone_number = models.CharField(
        max_length=20, verbose_name=_("Dasher Dropoff Phone Number"), blank=True
    )
    dasher_pickup_phone_number = models.CharField(
        max_length=20, verbose_name=_("Dasher Pickup phone number"), blank=True
    )

    cancellation_reason = models.TextField(
        verbose_name=_("Cancellation reason"), blank=True
    )

    # Applied offers
    voucher = models.ForeignKey(
        Voucher,
        verbose_name=_("Voucher"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    bogo = models.ForeignKey(
        Bogo, verbose_name=_("Bogo"), on_delete=models.SET_NULL, null=True, blank=True
    )
    bxgy = models.ForeignKey(
        BxGy, verbose_name=_("BxGy"), on_delete=models.SET_NULL, null=True, blank=True
    )
    reward_coupon = models.ForeignKey(
        UserReward,
        verbose_name=_("Reward Coupon"),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    driver_info = models.JSONField(
        verbose_name=_("Driver Info"), default=dict, blank=True
    )
    cancellation_reason = models.TextField(
        verbose_name=_("Cancellation Reason"),
        blank=True,
        null=True,
    )
    special_discount = models.FloatField(
        verbose_name=_("Special Discount"),
        null=True,
        blank=True,
        default=0,
    )
    special_discount_reason = models.TextField(
        verbose_name=_("Special Discount Reason"),
        blank=True,
        null=True,
    )
    # spend_x_save_y = models.ForeignKey(
    #     SpendXSaveY,
    #     verbose_name=_("Spend X Save Y offer"),
    #     on_delete=models.SET_NULL,
    #     null=True,
    #     blank=True,
    # )

    payment_method = models.CharField(
        max_length=30,
        verbose_name=_("Payment method"),
        choices=PaymentMethod.choices,
        default=PaymentMethod.STRIPE,
    )
    order_method = models.CharField(
        max_length=20,
        verbose_name=_("Order method"),
        choices=OrderMethod.choices,
        default=OrderMethod.DELIVERY,
    )
    scheduling_type = models.CharField(
        max_length=20,
        verbose_name=_("scheduling type"),
        choices=SchedulingType.choices,
        default=SchedulingType.ASAP,
    )
    scheduled_time = models.DateTimeField(
        verbose_name=_("scheduled time"),
        validators=[validate_future_date],
        blank=True,
        null=True,
    )

    extra = models.JSONField(verbose_name=_("Extra"), default=dict)
    pos_data = models.ManyToManyField(
        POS_DATA, verbose_name=_("pos details"), blank=True
    )
    order_type = models.CharField(
        max_length=255,
        verbose_name=_("order type"),
        choices=OrderingType.choices,
        default=OrderingType.INTERNAL,
    )
    checkout_note = models.CharField(
        max_length=255,
        verbose_name=_("checkout note"),
        blank=True,
        null=True
    )
    order_cancellation_reason = serializers.CharField(
        max_length=255,
        required=False,
        default="Order Cancelled by Customer",
        allow_blank=True,
    )
    external_platform = models.ForeignKey(
        Platform,
        on_delete=models.CASCADE,
        blank=True,
        null=True,
        verbose_name=_("partners identifier"),
    )
    order_item_meta_data = models.JSONField(
        verbose_name=_("order item meta data"), default=dict)
    raider_id = models.CharField(max_length=255, blank=True, null=True)
    is_group_order = models.BooleanField(default=False)
    restaurant_discount_percentage = models.FloatField(
        verbose_name=_("Restaurant Discount Percentage at Order Time"),
        null=True,
        blank=True,
        default=0
    )
    local_deal = models.ManyToManyField(LocalDeal, blank=True, verbose_name=_("Local Deal"))
    qr_code = models.ImageField(
        verbose_name=_("QR Code"),
        upload_to="qr_codes/%Y/%m/%d/",
        blank=True,
        null=True,
    )
    qr_code_value = models.CharField(
        max_length=10,
        verbose_name=_("QR Code Value"),
        unique=True,
        null=True,
        blank=True,
    )
    is_local_deal_redeemed = models.BooleanField(
        verbose_name=_("Is Local Deal Redeemed"), default=False
    )
    redeemed_at = models.DateTimeField(
        verbose_name=_("Redeemed At"), blank=True, null=True
    )
    delivery_man = models.CharField(
        max_length=255,
        verbose_name=_("Delivery Man"),
        null=True,
        blank=True,
        default=None
    )

    admin_received_cash = models.DecimalField(
        verbose_name=_("Admin Received Cash"),
        max_digits=10,
        decimal_places=2,
        blank=True,
        null=True
    )

    lucky_flip_gift = models.JSONField(
        verbose_name=_("Lucky Flip Gift"),
        blank=True,
        null=True,
        default=dict,
        help_text=_("Optional lucky-flip gift details, including name and price."),
    )

    commission_amount = models.FloatField(
        verbose_name=_("Commission Amount"), 
        null=True, 
        blank=True, 
        default=0
    )
    commission_percentage = models.FloatField(
        verbose_name=_("Commission Percentage"), 
        null=True, 
        blank=True, 
        default=0
    )
    restaurant_discount = models.FloatField(
        verbose_name=_("Restaurant Discount"), 
        null=True, 
        blank=True, 
        default=0
    )
    ht_delivery_fee_expense = models.FloatField(
    verbose_name=_("HT Delivery Fee Expense"), null=True, blank=True, default=0
    )

    on_time_guarantee_opted_in = models.BooleanField(default=False)
    on_time_guarantee_fee = models.FloatField(default=6)

    auto_apply_on_time_reward = models.BooleanField(default=True)
    used_on_time_reward = models.DecimalField(max_digits=10, decimal_places=2, default=0)



    def cancel_by_customer(self, reason="Order Cancelled by Customer"):
        # Only allow cancellation if order is still pending
        if self.status != self.StatusChoices.PENDING:
            raise ValueError("Order cannot be cancelled after acceptance")

        self.status_before_cancelled = self.status
        self.status = self.StatusChoices.CANCELLED
        self.cancellation_reason = reason
        self.save(update_fields=['status', 'status_before_cancelled', 'cancellation_reason'])

        # Notify customer and OMS about cancellation
        self.notify_customer_order_cancelled()
        self.notify_oms_order_cancelled()

    def notify_customer_order_cancelled(self):
        # Implement your notification logic (email, push, etc.) here
        pass

    def notify_oms_order_cancelled(self):
      if self.restaurant is not None:
            for token_obj in CompanyPushToken.objects.filter(company=self.restaurant.company):
                token = token_obj.push_token
                platform = token_obj.platform

            platform_key = "webpush" if platform == FirebasePlatform.WEB else "android"

            fcm_payload = {
                "message": {
                    platform_key: {
                        "data": {},  # Add extra data if needed
                        "notification": {
                            "title": "Order Cancelled",
                            "body": f"Your order {self.order_id} has been cancelled. Reason: {self.cancellation_reason}",
                            "image": None,
                            "click_action": None,
                        },
                    },
                    "data": {
                        "order": str(self.id),
                        "status": self.status,
                        "cancellation_reason": self.cancellation_reason,
                        "location": str(self.location.id) if self.location else "",
                    },
                    "token": token,
                }
            }

            print("Before Successfully send OMS notification")
            fcm_helper = FCMHelper()
            fcm_helper.send_notification(fcm_payload)
            # print("Successfully send OMS notification")
            print("Successfully send OMS notification", self.restaurant)




    class Meta:
        ordering = ["-id"]
        verbose_name = _("Order")
        verbose_name_plural = _("Orders")

    def __str__(self):
        return str(self.order_id)
      
      
    def is_refund_applicable(self):
        print(self.refund_status, '------------------> Refund Status')
        return self.status != self.StatusChoices.COMPLETED
    
    @staticmethod
    def generate_unique_10_digit_code(model_class, field_name='qr_code_value'):
        while True:
            code = ''.join(random.choices('0123456789', k=10))
            # Check if code already exists in the model for that field
            if not model_class.objects.filter(**{field_name: code}).exists():
                return code     
      
    def generate_qr_code(self):
        # Your existing QR code generation logic here
        # Example:
        code = self.generate_unique_10_digit_code(Order, 'qr_code_value')
        self.qr_code_value = code

        qr_data = code
        qr = qrcode.make(qr_data)

        qr_image = BytesIO()
        qr.save(qr_image, format='PNG')
        qr_image.seek(0)

        self.qr_code = InMemoryUploadedFile(
            qr_image, None, f"{self.order_id}_qr.png", 'image/png', qr_image.tell(), None
        )
        # Save the model with updated QR code fields
        self.save(update_fields=['qr_code', 'qr_code_value'])
    
  
    def save(self, *args, **kwargs):
        if self.company is None:
            self.company = self.restaurant.company

        if self.pickup_address == "" and self.location is not None:
            self.pickup_address = self.location.details
            
        # ✅ Set restaurant_discount_percentage on creation only
        if not self.pk and self.restaurant:
            self.restaurant_discount_percentage = self.restaurant.discount_percentage or 0
        # if self.doordash_external_delivery_id is None:
        #     self.doordash_external_delivery_id = self.order_id

        super().save(*args, **kwargs)

        # ---------- SAFE HELPERS (add-only) ----------
    @property
    def has_canceled_items(self) -> bool:
        """
        True if any line is fully canceled or partially canceled.
        Non-breaking: only reads OrderItem flags.
        """
        return self.orderitem_set.filter(Q(is_canceled=True) | Q(canceled_quantity__gt=0)).exists()

    @property
    def active_items(self):
        """
        Items that still have deliverable quantity.
        Non-breaking: read-only queryset.
        """
        return self.orderitem_set.exclude(is_canceled=True).exclude(canceled_quantity__gte=F("quantity"))

    @property
    def net_total(self) -> float:
        """
        What the user should pay after cancellations.
        Non-breaking: does not mutate original 'total'.
        """
        try:
            return max(float(self.total or 0) - float(self.refund_amount or 0), 0.0)
        except Exception:
            return float(self.total or 0)

    # ---------- REFUND MATH (add-only) ----------
    def _allocate_refund_for(self, item, qty: int) -> float:
        """
        Simple, predictable proration:
        - base: unit_price * qty
        - optional tax share proportional to subtotal (keeps discounts/tips untouched)
        Non-breaking: only used when you call apply_item_cancellations().
        """
        qty = int(qty or 0)
        if qty <= 0:
            return 0.0

        unit = float(getattr(item, "unit_price", 0) or 0)
        line_sub = unit * qty

        tax_share = 0.0
        try:
            sub = float(self.subtotal or 0)
            tax = float(self.tax or 0)
            if sub > 0:
                tax_share = (line_sub / sub) * tax
        except Exception:
            tax_share = 0.0

        return round(line_sub + tax_share, 2)

    # ---------- MAIN ENTRYPOINT (add-only) ----------
    @transaction.atomic
    def apply_item_cancellations(self, item_qty_pairs, *, cancelled_by="restaurant", reason: str = ""):
        """
        item_qty_pairs: iterable of (OrderItem, qty_to_cancel) — qty_to_cancel may be full or partial.
        """
        total_refund_delta = 0.0
        event_details = []  # ← collect canceled line snapshots for notification

        for it, qty in item_qty_pairs:
            remaining_before = getattr(it, "active_quantity", 0)
            cancel_qty = qty if qty is not None else remaining_before
            if cancel_qty and cancel_qty > 0:
                # Raises ValidationError if invalid
                it.cancel(qty=cancel_qty, by=cancelled_by, reason=reason)

                actually_canceled = min(cancel_qty, remaining_before)
                line_refund = self._allocate_refund_for(it, actually_canceled)
                total_refund_delta += line_refund

                # per-item refund ledger (best-effort)
                try:
                    it.refund_amount = (it.refund_amount or 0) + line_refund
                    it.save(update_fields=["refund_amount"])
                except Exception:
                    pass

                # snapshot for this event
                event_details.append({
                    "order_item_id": it.id,
                    "name": getattr(it.menu_item, "name", "Item"),
                    "qty": int(actually_canceled),
                    "unit_price": float(getattr(it, "unit_price", 0.0) or 0.0),
                    "refund": float(line_refund),
                    "reason": (it.cancellation_reason or reason or ""),
                })

        if total_refund_delta <= 0:
            return  # nothing changed

        # order-level refund bookkeeping
        self.refund_amount = float(self.refund_amount or 0) + float(total_refund_delta)

        # payment-method-specific handling (keeps prior semantics)
        if self.payment_method in [self.PaymentMethod.STRIPE, self.PaymentMethod.CARD, self.PaymentMethod.PAYPAL]:
            self.refund_status = self.RefundStatusChoices.REQUESTED  # keep original total intact
        else:
            self.total = max(float(self.total or 0) - float(total_refund_delta), 0.0)

        # full cancel → flip status
        if not self.active_items.exists():
            if self.status not in [self.StatusChoices.CANCELLED, self.StatusChoices.REJECTED, self.StatusChoices.COMPLETED]:
                self.status_before_cancelled = self.status
                self.status = self.StatusChoices.CANCELLED
                if reason and not self.cancellation_reason:
                    self.cancellation_reason = reason

        self.save(update_fields=[
            "refund_amount", "refund_status", "total",
            "status", "status_before_cancelled", "cancellation_reason"
        ])

        # 🔔 Notify customer (items + reason + refund); never break request
        try:
            self.notify_customer_items_canceled(canceled_lines=event_details, reason=reason, refund_delta=total_refund_delta)
        except Exception:
            pass

    # ---------- Optional notifier (add-only, safe no-op if you don't wire tokens) ----------
    # in Order model
    def _get_customer_tokens(self) -> list[str]:
        """
        Fetch raw FCM tokens for this order's user.
        Keep it simple: your send_push_notification accepts a flat token list.
        """
        try:
            if not self.user_id:
                return []
            # adjust the import path if your TokenFCM lives elsewhere
            from firebase.models import TokenFCM
            return list(
                TokenFCM.objects
                    .filter(user_id=self.user_id)
                    .values_list("token", flat=True)
            )
        except Exception:
            logger.exception("Failed to fetch tokens for user_id=%s", self.user_id)
            return []


    def notify_partial_cancellation(self, reason: str = "", refund_delta: float = 0.0):
        """
        Back-compat shim: if older code calls this without item details,
        send a short message that items were adjusted.
        """
        try:
            self.notify_customer_items_canceled(
                canceled_lines=[],  # unknown items in the old path
                reason=reason,
                refund_delta=refund_delta
            )
        except Exception:
            pass

    # def notify_customer_items_canceled(self, *, canceled_lines: list[dict], reason: str = "", refund_delta: float = 0.0):
    #     """
    #     Sends a push listing which items/qty were canceled and why — using your
    #     send_push_notification(tokens, data) helper.
    #     """
    #     print(f"[FCM] notify called for order={self.id} user_id={self.user_id}")
    #     if not self.user_id:
    #         logger.info("[FCM] order=%s has no user_id; skip customer push", self.id)
    #         return

    #     tokens = self._get_customer_tokens()
    #     if not tokens:
    #         logger.info("[FCM] no tokens for user_id=%s; skip", self.user_id)
    #         return

    #     # Short readable summary for notification body
    #     if canceled_lines:
    #         summary = ", ".join([f"{x.get('name','Item')} x{x.get('qty',0)}" for x in canceled_lines])[:180]
    #     else:
    #         summary = "Some items were adjusted"

    #     title = "Items canceled from your order"
    #     body  = f"{summary}. Adjustment: {refund_delta:.2f}. {reason}".strip()

    #     # Build the payload that YOUR helper expects
    #     data = {
    #         "campaign_title": title,
    #         "campaign_message": body,
    #         "campaign_image": "",                 # optional: put a banner/restaurant logo URL if you have one
    #         "campaign_category": "order_update",  # handy to filter on the client
    #         "campaign_is_active": "true",
    #         "restaurant_name": getattr(self.restaurant, "name", "") or "",
    #         "screen": "order_details",            # let the app deep-link to the order screen
    #         "id": str(self.id),                   # internal db id if you need it on client
    #         "order_id": str(self.order_id),       # public order uuid for deep-linking
    #     }

    #     try:
    #         # adjust the import path to wherever you placed this helper
    #         res = send_push_notification(tokens, data)
    #         logger.info("[FCM] cancellation push sent: success=%s failed=%s", res.get("successful"), res.get("failed"))
    #     except Exception as e:
    #         logger.exception("Failed to send cancellation push for order=%s: %s", self.id, e)
            
class OrderModifiersItems(BaseModel):
    modifiersOrderItems = models.ForeignKey(
        MenuItem,
        on_delete=models.CASCADE,
        verbose_name=_("modifier item"),
        related_name="modifier_item",
        blank=True,
    )
    quantity = models.PositiveIntegerField(
        verbose_name=_("Quantity"), default=1, blank=True
    )

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Order Modifiers Item")
        verbose_name_plural = _("Order Modifiers Items")


class OrderedModifiers(BaseModel):
    modifiers = models.ForeignKey(
        ModifierGroup,
        on_delete=models.CASCADE,
        verbose_name=_("modifiers details"),
        related_name="modifiers",
        blank=True,
    )
    modifiersItems = models.ManyToManyField(
        OrderModifiersItems,
        verbose_name=_("modifiers items"),
        related_name="modifiers_items",
        blank=True,
    )
    quantity = models.PositiveIntegerField(
        verbose_name=_("Quantity"), default=1
    )

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Ordered Modifiers")
        verbose_name_plural = _("Ordered Modifiers")


CANCELLED_BY_CHOICES = [
    ("restaurant", "Restaurant"),
    ("customer", "Customer"),
    ("admin", "Admin"),
    ("system", "System"),
]

class OrderItem(BaseModel):
    order = models.ForeignKey(
        Order, verbose_name=_(
            "Order"
        ), on_delete=models.CASCADE
    )
    menu_item = models.ForeignKey(
        MenuItem, verbose_name=_("Menu Item"), on_delete=models.SET_NULL, null=True
    )
    quantity = models.PositiveIntegerField(
        verbose_name=_("Quantity"), default=1
    )
    modifiers = models.ManyToManyField(
        OrderedModifiers, verbose_name=_("modifiers"), blank=True
    )

    is_canceled = models.BooleanField(default=False, db_index=True)
    canceled_quantity = models.PositiveIntegerField(default=0)          # 0..quantity
    canceled_by = models.CharField(max_length=20, choices=CANCELLED_BY_CHOICES, blank=True, null=True)
    cancellation_reason = models.CharField(max_length=255, blank=True, null=True)
    canceled_at = models.DateTimeField(blank=True, null=True)

    # Optional: per-item refund accounting (kept simple)
    refund_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    class Meta:
        ordering = ["-id"]
        verbose_name = _("Order Item")
        verbose_name_plural = _("Order Items")

    def __str__(self):
        name = getattr(self.menu_item, "name", "Unknown item")
        return f"{self.order.order_id} :: {name}"


    @property
    def active_quantity(self) -> int:
        """How many units are still being prepared/delivered."""
        q = max(int(self.quantity or 0) - int(self.canceled_quantity or 0), 0)
        return 0 if self.is_canceled else q

    @property
    def unit_price(self) -> float:
        """Base price used across the app (kept same as before)."""
        return float(getattr(self.menu_item, "base_price", 0) or 0)

    @property
    def total_cost(self) -> float:
        """
        🔄 CHANGED: now uses active_quantity so existing places that rely on
        total_cost automatically exclude canceled units.
        """
        return self.unit_price * self.active_quantity
    
    def cancel(self, *, qty: int | None = None, by: str = "restaurant", reason: str = ""):
        """
        Soft-cancel this line (full or partial). Idempotent and safe.
        - qty=None or qty>=remaining → cancel the remainder.
        - qty>remaining → raises ValidationError.
        """
        if self.is_canceled and (self.active_quantity == 0):
            # Already fully canceled; nothing to do
            return

        remaining = self.active_quantity if self.active_quantity else (self.quantity - self.canceled_quantity)
        if qty is None:
            qty = remaining

        if qty <= 0:
            raise ValidationError("Cancel quantity must be > 0.")
        if qty > remaining:
            raise ValidationError("Cannot cancel more than remaining quantity.")

        self.canceled_quantity = (self.canceled_quantity or 0) + qty
        if self.canceled_quantity >= (self.quantity or 0):
            self.is_canceled = True

        self.canceled_by = by
        if reason:
            self.cancellation_reason = reason
        self.canceled_at = timezone.now()
        # Per-item refund_amount is computed by service layer (kept here as a field)
        self.save(update_fields=[
            "is_canceled", "canceled_quantity", "canceled_by",
            "cancellation_reason", "canceled_at"
        ])

class Purchase(BaseModel):
    # ref: https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.products

    class PurchaseState(models.TextChoices):
        PURCHASED = "purchased", _("PURCHASED")
        CANCELLED = "cancelled", _("CANCELLED")
        PENDING = "pending", _("PENDING")
        REFUNDED = "refunded", _("REFUNDED")

    class PurchaseType(models.TextChoices):
        SANDBOX = "sandbox", _("SANDBOX")
        PRODUCTION = "production", _("PRODUCTION")

    user = models.ForeignKey(
        to=User, verbose_name=_("User"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    # order = models.OneToOneField(Order, verbose_name=_('Order'), on_delete=models.SET_NULL, null=True)
    purchase_token = models.TextField(verbose_name=_("Purchase Token"))
    purchase_time = models.DateTimeField(verbose_name=_("Purchase Time"))
    purchase_state = models.CharField(
        max_length=30,
        verbose_name=_("Purchase State"),
        choices=PurchaseState.choices,
        default=PurchaseState.PENDING,
    )
    purchase_type = models.CharField(
        max_length=30,
        verbose_name=_("Purchase Type"),
        choices=PurchaseType.choices,
        blank=True,
        null=True,
        default=PurchaseType.PRODUCTION,
    )
    region = models.CharField(
        verbose_name=_(
            "Region Code"
        ), max_length=50, blank=True
    )
    extra = models.JSONField(verbose_name=_("Extra"), default=dict, blank=True)

    class Meta:
        verbose_name = _("Purchase")
        verbose_name_plural = _("Purchases")
        ordering = ["-id"]


class BasePaymentCapturePayload(BaseModel):
    payload = models.JSONField(verbose_name=_("Payload"), default=dict)
    uid = models.CharField(max_length=250, verbose_name=_("UID"), blank=True)
    purchase = models.OneToOneField(
        Purchase, verbose_name=_("Purchase"), on_delete=models.SET_NULL, null=True
    )

    class Meta:
        abstract = True


class PaypalCapturePayload(BasePaymentCapturePayload):
    user = models.ForeignKey(
        verbose_name=_("User"),
        to=User,
        related_name="paypal_payloads",
        on_delete=models.SET_NULL,
        null=True,
    )

    class Meta:
        verbose_name = _("Paypal Capture Payload")
        verbose_name_plural = _("Paypal Capture Payloads")


class StripeCapturePayload(BasePaymentCapturePayload):
    user = models.ForeignKey(
        verbose_name=_("User"),
        to=User,
        related_name="stripe_payloads",
        on_delete=models.SET_NULL,
        null=True,
    )

    class Meta:
        verbose_name = _("Stripe Capture Payload")
        verbose_name_plural = _("Stripe Capture Payloads")


class PaymentDetails(BaseModel):
    title = models.CharField(
        max_length=250, verbose_name=_("Title"), blank=True
    )
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )

    paypal_email = models.EmailField(
        verbose_name=_("Paypal Email"), blank=True
    )
    paypal_merchant_id = models.CharField(
        max_length=20, verbose_name=_("Paypal Merchant ID"), blank=True
    )

    account_holder_name = models.CharField(
        max_length=250, verbose_name=_("Account Holder Name"), blank=True
    )
    country = models.CharField(
        max_length=40, verbose_name=_("Country"), blank=True
    )
    currency = models.CharField(
        max_length=10, verbose_name=_("Currency"), blank=True
    )
    routing_number = models.CharField(
        max_length=50, verbose_name=_("Routing Number"), blank=True
    )
    account_number = models.CharField(
        max_length=50, verbose_name=_("Account Number"), blank=True
    )

    class Meta:
        verbose_name = _("Payment Details")
        verbose_name_plural = _("Payment Details")


class Invoice(BaseModel):
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    invoice_id = models.UUIDField(
        verbose_name=_("Invoice ID"), default=uuid.uuid4, unique=True
    )
    invoice_date = models.DateTimeField(
        verbose_name=_("Invoice date"), default=timezone.now
    )
    due_date = models.DateTimeField(
        verbose_name=_("Due date"), null=True, blank=True
    )
    is_paid = models.BooleanField(verbose_name=_("Is paid"), default=False)
    total_amount = models.FloatField(verbose_name=_("Total amount"), default=0)

    class Meta:
        verbose_name = _("Invoice")
        verbose_name_plural = _("Invoices")

    def __str__(self):
        return self.invoice_id


class InvoiceItem(BaseModel):
    invoice = models.ForeignKey(
        Invoice, verbose_name=_("Invoice"), on_delete=models.CASCADE
    )
    item = models.CharField(
        max_length=250, verbose_name=_("Menu Item"), blank=True
    )
    quantity = models.PositiveIntegerField(
        verbose_name=_("Quantity"), default=0
    )
    unit_price = models.FloatField(verbose_name=_("Unit price"), default=0)
    subtotal = models.FloatField(verbose_name=_("Subtotal"), default=0)

    class Meta:
        verbose_name = _("Invoice Item")
        verbose_name_plural = _("Invoice Items")

    def __str__(self):
        return f"{self.invoice.invoice_id} :: {self.item}"


class StripeConnectAccount(BaseModel):
    user = models.OneToOneField(
        User, verbose_name=_("User"), on_delete=models.CASCADE
    )
    company = models.OneToOneField(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    stripe_id = models.TextField(verbose_name=_("Stripe ID"))
    account_details = models.JSONField(
        verbose_name=_("Account details"), default=dict, blank=True
    )
    charges_enabled = models.BooleanField(
        verbose_name=_("Charges enabled"), default=False
    )

    class Meta:
        verbose_name = _("Stripe Connect Account")
        verbose_name_plural = _("Stripe Connect Accounts")

    def __str__(self):
        return self.stripe_id


class DeliveryFeeAssociation(BaseModel):
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    restaurant = models.OneToOneField(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.CASCADE
    )
    delivery_fee = models.FloatField(verbose_name=_("Delivery_fee"), default=0)
    convenience_fee = models.FloatField(
        verbose_name=_("Convenience_fee"), default=0
    )
    tax_rate = models.FloatField(verbose_name=_("Tax Rate"), default=0)
    alcoholic_tax_rate = models.FloatField(
        verbose_name=_("Alcoholic Tax Rate"), default=0
    )

    discount = models.FloatField(verbose_name=_("Discount"), default=0)
    minimum_order_amount = models.FloatField(
        verbose_name=_("Minimum Order Amount"), default=0
    )
    use_tax = models.BooleanField(verbose_name=_("Use Tax"), default=0)

    class Meta:
        verbose_name = _("Delivery Fees Association")

    def __str__(self) -> str:
        return f"delivery fees for --> {self.restaurant.name}"


class Payout(BaseModel):
    company = models.ForeignKey(
        Company, verbose_name=_("Company"), on_delete=models.CASCADE
    )
    uid = models.TextField(verbose_name=_("Payout ID"), blank=True)
    amount = models.FloatField(verbose_name=_("Amount"), default=0)
    chatchef_commission = models.FloatField(
        verbose_name=_("Chatchef commission"), default=0
    )
    details = models.JSONField(verbose_name=_("Details"), default=dict)

    class Meta:
        verbose_name = _("Payout")
        verbose_name_plural = _("Payouts")

    def __str__(self):
        return self.uid


class OrderReminder(BaseModel):
    def validate_future_date(value, instance=None):
        if value and value <= timezone.now():
            raise ValidationError(_("Scheduled time must be in the future."))

    email = models.EmailField(verbose_name=_("email"), blank=True, null=True)
    phone = models.CharField(
        max_length=20, verbose_name=_("phone"), blank=True, null=True
    )
    user = models.ForeignKey(
        User, on_delete=models.SET_NULL, verbose_name=_("user"), blank=True, null=True
    )
    is_ordered = models.BooleanField(default=False)
    is_rescheduled = models.BooleanField(default=False)
    is_mailed = models.BooleanField(default=False)
    is_sms_send = models.BooleanField(default=False)
    company = models.ForeignKey(
        Company, verbose_name=_("company"), on_delete=models.SET_NULL, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )

    order_data = models.JSONField(verbose_name=_("order data"), default=dict)
    reminder_time = models.DateTimeField(
        verbose_name=_("scheduled time"), validators=[validate_future_date]
    )

    def __str__(self):
        return f"{self.phone}"

    class Meta:
        ordering = ["-id"]

    def save(self, *args, **kwargs):
        if self.company is None:
            self.company = self.restaurant.company

        super().save(*args, **kwargs)


class ExternalPaymentInformation(BaseModel):
    processingStatusChoices = (
        ("COLLECTABLE", "COLLECTABLE"),
        ("PROCESSED", "PROCESSED"),
    )
    paymentMethodChoices = (
        ("CASH", "CASH"),
        ("CARD", "CARD"),
        ("UNKNOWN", "UNKNOWN"),
        ("OTHER", "OTHER"),
        ("CHEQUE", "CHEQUE"),
    )
    paymentAuthorizerChoices = (
        ("UNKNOWN_TYPE", "UNKNOWN_TYPE"),
        ("OTHER_TYPE", "OTHER_TYPE"),
        ("MASTERCARD", "MASTERCARD"),
        ("MASTERCARD_MAESTRO", "MASTERCARD_MAESTRO"),
        ("MASTERCARD_DEBIT", "MASTERCARD_DEBIT"),
        ("VISA", "VISA"),
        ("VISA_DEBIT", "VISA_DEBIT"),
        ("AMEX", "AMEX"),
        ("VISA_ELECTORN", "VISA_ELECTORN"),
        ("DINERS", "DINERS"),
        ("ELO", "ELO"),
        ("ELO_DEBIT", "ELO_DEBIT"),
        ("HIPERCARD", "HIPERCARD"),
        ("BANRICOMPRAS", "BANRICOMPRAS"),
        ("BANRICOMPRAS_DEBIT", "BANRICOMPRAS_DEBIT"),
        ("NUGO", "NUGO"),
        ("GOODCARD", "GOODCARD"),
        ("VERDECARD", "VERDECARD"),
        ("CARNET", "CARNET"),
        ("CHEF_CARD", "CHEF_CARD"),
        ("GER_CC_CREDITO", "GER_CC_CREDITO"),
        ("TERMINAL_BANCARIA", "TERMINAL_BANCARIA"),
        ("DEBIT", "DEBIT"),
        ("QR_CODE", "QR_CODE"),
        ("RAPPI_PAY", "RAPPI_PAY"),
        ("DISCOVER", "DISCOVER"),
        ("VALE_GREEN_CARD_PAPEL", "VALE_GREEN_CARD_PAPEL"),
        ("VALE_GREEN_CARD_CARD", "VALE_GREEN_CARD_CARD"),
        ("VALE_REFEISUL", "VALE_REFEISUL"),
        ("VALE_VEROCARD", "VALE_VEROCARD"),
        ("VALE_VR_SMART", "VALE_VR_SMART"),
        ("VALE_SODEXO", "VALE_SODEXO"),
        ("VALE_TICKET_RESTAURANTE", "VALE_TICKET_RESTAURANTE"),
        ("VALE_ALELO", "VALE_ALELO"),
        ("VALE_BEN_VIS", "VALE_BEN_VIS"),
        ("VALE_COOPER_CARD", "VALE_COOPER_CARD"),
        ("NUTRICARD_REFEICAO_E_ALIMENTACAO", "NUTRICARD_REFEICAO_E_ALIMENTACAO"),
        ("APPLE_PAY_MASTERCARD", "APPLE_PAY_MASTERCARD"),
        ("APPLE_PAY_VISA", "APPLE_PAY_VISA"),
        ("APPLE_PAY_AMEX", "APPLE_PAY_AMEX"),
        ("GOOGLE_PAY_ELO", "GOOGLE_PAY_ELO"),
        ("GOOGLE_PAY_MASTERCARD", "GOOGLE_PAY_MASTERCARD"),
        ("GOOGLE_PAY_VISA", "GOOGLE_PAY_VISA"),
        ("MOVILE_PAY", "MOVILE_PAY"),
        ("MOVILE_PAY_AMEX", "MOVILE_PAY_AMEX"),
        ("MOVILE_PAY_DINERS", "MOVILE_PAY_DINERS"),
        ("MOVILE_PAY_ELO", "MOVILE_PAY_ELO"),
        ("MOVILE_PAY_HIPERCARD", "MOVILE_PAY_HIPERCARD"),
        ("MOVILE_PAY_MASTERCARD", "MOVILE_PAY_MASTERCARD"),
        ("MOVILE_PAY_VISA", "MOVILE_PAY_VISA"),
        ("IFOOD_CORP", "IFOOD_CORP"),
        ("LOOP_CLUB", "LOOP_CLUB"),
        ("PAYPAL", "PAYPAL"),
        ("PSE", "PSE"),
        ("PIX", "PIX"),
    )
    order = models.ForeignKey(
        Order,
        on_delete=models.CASCADE,
        verbose_name=_("order"),
        related_name="externalPayments",
        null=True,
        blank=True,
    )
    value = models.FloatField(verbose_name=_("order value"))
    processingStatus = models.CharField(
        max_length=50,
        choices=processingStatusChoices,
        verbose_name=_("processing status"),
    )
    paymentMethod = models.CharField(
        max_length=50, choices=paymentMethodChoices, verbose_name=_("payment method")
    )
    paymentAuthorizer = models.CharField(
        max_length=50,
        choices=paymentAuthorizerChoices,
        blank=True,
        null=True,
        verbose_name=_("payment authorizer"),
    )

    class Meta:
        verbose_name = _("External Payment Information")
        verbose_name_plural = _("External Payments Information")

    def __str__(self):
        return f"{self.order} :: {self.processingStatus}"


class Wallet(BaseModel):
    user = models.OneToOneField(
        User, on_delete=models.CASCADE, verbose_name=_("wallet user")
    )
    pin = models.CharField(
        max_length=20, verbose_name=_(
            "pin"
        ), blank=True, null=True
    )
    balance = models.FloatField(default=0, verbose_name=_("balance"))
    wallet_address = models.UUIDField(
        verbose_name=_("wallet address"), default=uuid.uuid4, unique=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )

    def __str__(self):
        return f"{self.user} :: wallet"

    class Meta:
        verbose_name = _("Wallet")
        verbose_name_plural = _("Wallets")
        ordering = ["-id"]
        unique_together = ("user", "restaurant")


class UnregisteredGiftCard(models.Model):
  # test comment
  
    email = models.EmailField()
    amount = models.FloatField(
        verbose_name=_("Amount"), default=0
    )

    currency = models.CharField(max_length=10, default="CAD")
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, choices=(
        ('PENDING', 'Pending'), ('CLAIMED', 'Claimed')), default='PENDING')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Gift card for {self.email} - {self.amount} {self.currency} ({self.status})"


class Transactions(BaseModel):
    class UsedFor(models.TextChoices):
        GIFT = "gift", _("GIFT")
        PLACED_ORDER = "placed_order"

    class TransactionType(models.TextChoices):
        IN = "in", _("IN")
        OUT = "out", _("OUT")

    class TransactionStatus(models.TextChoices):
        PENDING = "pending", _("PENDING")
        SUCCESS = "success", _("SUCCESS")
        FAILED = "failed", _("FAILED")

    class PaymentGateway(models.TextChoices):
        UNKNOWN = "unknown", _("UNKNOWN")
        STRIPE = "stripe", _("STRIPE")
        PAYPAL = "paypal", _("PAYPAL")
        WALLET = "wallet", _("WALLET")

    wallet = models.ForeignKey(
        Wallet,
        on_delete=models.SET_NULL,
        verbose_name=_("wallet"),
        blank=True,
        null=True,
    )
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, verbose_name=_("user")
    )
    transaction_id = models.UUIDField(
        verbose_name=_("transaction id"), default=uuid.uuid4, unique=True
    )
    amount = models.FloatField(verbose_name=_("balance"))
    charges = models.FloatField(verbose_name=_("charges"), default=0)
    currency = models.CharField(
        max_length=50,
        verbose_name=_("currency"),
        blank=True,
        null=True,
    )
    type = models.CharField(
        max_length=50,
        verbose_name=_("transaction type"),
        choices=TransactionType.choices,
        blank=True,
        null=True,
        default=TransactionType.IN,
    )
    status = models.CharField(
        max_length=50,
        verbose_name=_("transaction status"),
        choices=TransactionStatus.choices,
        blank=True,
        null=True,
        default=TransactionStatus.PENDING,
    )
    gateway = models.CharField(
        max_length=50,
        verbose_name=_("gateway"),
        choices=PaymentGateway.choices,
        blank=True,
        null=True,
        default=PaymentGateway.UNKNOWN,
    )
    used_for = models.CharField(
        max_length=50,
        verbose_name="used in",
        choices=UsedFor.choices,
        default=UsedFor.PLACED_ORDER
    )
    gift_user = models.ForeignKey(
        RestaurantUser, on_delete=models.CASCADE, blank=True, null=True, related_name="gifted_to")
    gift_by = models.ForeignKey(
        RestaurantUser, on_delete=models.CASCADE, blank=True, null=True, related_name="gifted_by")
    sender_name = models.CharField(max_length=255, blank=True, null=True)
    
    gateway_transaction_id = models.CharField(
        max_length=250, verbose_name=_("gateway transaction id"), blank=True, null=True
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )
    is_seen = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.user} :: {self.transaction_id}"

    class Meta:
        verbose_name = _("Transaction")
        verbose_name_plural = _("Transactions")
        ordering = ["-id"]


class PaymentMethods(models.Model):
    # card details
    card_number = models.CharField(
        max_length=500, verbose_name=_("card number")
    )
    expire_date = models.CharField(
        max_length=500, verbose_name=_("expire_date")
    )
    cvv = models.CharField(max_length=500, verbose_name=_("cvv"))

    # card holder details
    name_on_card = models.CharField(
        max_length=500, verbose_name=_("card holder")
    )
    address_line_1 = models.CharField(
        max_length=500, verbose_name=_("address line 1")
    )
    address_line_2 = models.CharField(
        max_length=500, verbose_name=_("address line 2")
    )
    country = models.CharField(max_length=500, verbose_name=_("country"))
    city = models.CharField(max_length=500, verbose_name=_("city"))
    postal_code = models.CharField(
        max_length=500, verbose_name=_("postal code")
    )
    state = models.CharField(max_length=50, verbose_name=_("state"))
    uid = models.UUIDField(
        verbose_name=_(
            "uid"
        ), default=uuid.uuid4, unique=True
    )
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, verbose_name=_("user")
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_("Restaurant"), on_delete=models.SET_NULL, null=True
    )
    location = models.ForeignKey(
        Location, verbose_name=_("Location"), on_delete=models.SET_NULL, null=True
    )

    def __str__(self):
        return f"{self.uid}"

    class Meta:
        verbose_name = _("Payment Methods")
        verbose_name_plural = _("Payment Methods")
        ordering = ["-id"]


class RestaurantFee(BaseModel):
    company = models.ForeignKey(
        Company, verbose_name=_(
            'Company'
        ), on_delete=models.CASCADE
    )
    restaurant = models.ForeignKey(
        Restaurant, verbose_name=_(
            'Restaurant'
        ), on_delete=models.CASCADE
    )
    max_distance = models.FloatField(verbose_name=_('Max Distance'), default=5)
    delivery_fee = models.FloatField(verbose_name=_('Delivery Fee'), default=1)
    service_fee = models.FloatField(verbose_name=_('Service Fee'), default=0)

    class Meta:
        verbose_name = _('Restaurant Fee')
        verbose_name_plural = _('Restaurant Fees')

    def __str__(self):
        return f'{self.restaurant.name} :: {self.max_distance}'


class UberAuthModel(BaseModel):
    token = models.TextField()

    def __str__(self) -> str:
        return self.token


class RaiderAppAuth(BaseModel):
    token = models.TextField()

    def __str__(self) -> str:
        return self.token


class PayoutHistory(BaseModel):
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    location = models.ForeignKey(Location, on_delete=models.CASCADE)
    orders = models.ManyToManyField(Order)
    statement_start_date = models.DateTimeField(blank=True, null=True)
    statement_end_date = models.DateTimeField(blank=True, null=True)
    payout_amount = models.FloatField(default=0)
    net_revenue = models.FloatField(default=0)
    gross_revenue = models.FloatField(default=0)
    delivery_fees = models.FloatField(default=0)
    bag_fees = models.FloatField(default=0)
    tips = models.FloatField(default=0)
    utensil_fees = models.FloatField(default=0)
    promotional_expenses = models.FloatField(default=0)
    stripe_fees = models.FloatField(default=0)
    service_fees_paid_to_chatchef = models.FloatField(default=0)
    service_fees_paid_by_customer_to_restaurant = models.FloatField(default=0)
    delivery_fees_bare_by_restaurant = models.FloatField(default=0)
    tax_paid_by_customer = models.FloatField(default=0)
    tax_paid_by_restaurant = models.FloatField(default=0)
    invoice = models.FileField(upload_to='invoice/xls/%Y/%m/%d/')
    pdf = models.FileField(upload_to='invoice/pdf/%Y/%m/%d/', blank=False)
    is_paid = models.BooleanField(default=False)
    is_mailed = models.BooleanField(default=False)
    adjustments = models.FloatField(default=0)
    adjustments_note = models.TextField(blank=True, null=True)
    uid = models.CharField(max_length=255, null=True, blank=True)
    original_delivery_fees: models.FloatField = models.FloatField(default=0)

    def __str__(self) -> str:
        return f"{self.restaurant.name} --> {self.location.name} --> {self.id}"

    class Meta:
        ordering = ['-id']



class PayoutHistoryForHungry(BaseModel):
    restaurant = models.ForeignKey(Restaurant, on_delete=models.CASCADE)
    location = models.ForeignKey(Location, on_delete=models.CASCADE)
    orders = models.ManyToManyField(Order)
    statement_start_date = models.DateTimeField(blank=True, null=True)
    statement_end_date = models.DateTimeField(blank=True, null=True)
    payout_amount = models.FloatField(default=0)
    net_revenue = models.FloatField(default=0)
    gross_revenue = models.FloatField(default=0)
    delivery_fees = models.FloatField(default=0)
    commission_percentage = models.FloatField(default=0)
    commission_amount = models.FloatField(default=0)
    service_fee_to_restaurant = models.FloatField(default=0)
    service_fee_to_hungrytiger = models.FloatField(default=0)
    delivery_fees_expense = models.FloatField(default=0)
    tax = models.FloatField(default=0)
    discount = models.FloatField(default=0)
    bogo_discount = models.FloatField(default=0)
    restaurant_discount = models.FloatField(default=0)
    customer_absorbed_delivery_fees = models.FloatField(default=0)
    amount_to_restaurant = models.FloatField(default=0)
    ht_profit = models.FloatField(default=0)
    selling_price_inclusive_of_tax = models.FloatField(default=0)
    container_fees = models.FloatField(default=0)
    bag_fees = models.FloatField(default=0)
    tips_for_restaurant = models.FloatField(default=0)
    original_delivery_fees = models.FloatField(default=0)
    invoice = models.FileField(upload_to='invoice/xls/%Y/%m/%d/')
    pdf = models.FileField(upload_to='invoice/pdf/%Y/%m/%d/', blank=False)
    is_paid = models.BooleanField(default=False)
    is_mailed = models.BooleanField(default=False)
    adjustments = models.FloatField(default=0)
    adjustments_note = models.TextField(blank=True, null=True)
    uid = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        ordering = ['-id']

    def __str__(self) -> str:
        return f"{self.restaurant.name} --> {self.location.name} --> {self.id}"








class RestaurantContract(models.Model):
    restaurant = models.ForeignKey(
        Restaurant,  
        on_delete=models.CASCADE,
        related_name="restaurantContact",
        null=True,  
        blank=True,  
        verbose_name="Restaurant",
    )
    # Essential Fields
    commission_percentage = models.DecimalField(max_digits=5, decimal_places=2, help_text="HungryTiger commission percentage")
    bogo_bear_by_restaurant = models.DecimalField(max_digits=5, decimal_places=2, help_text="BOGO discount percentage borne by restaurant")
    restaurant_discount_percentage = models.DecimalField(max_digits=5, decimal_places=2, help_text="Discount percentage offered by the restaurant")
    ht_voucher_percentage_borne_by_restaurant = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0.00,
        help_text="HT Voucher percentage borne by Restaurant"
    )

    restaurant_accepted_discount = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0.00,
        help_text="Discount percentage accepted by the restaurant"
    )

    restaurant_voucher_codes = JSONField(
        blank=True,
        null=True,
        help_text="List of voucher codes accepted by the restaurant"
    )

    def __str__(self):
        return f"Contract #{self.id}"
    





class RiderRating(models.Model):
    """One rating per (order, customer) for RAIDER_APP deliveries."""
    TAGS = [
        ("on_time", "On-time"),
        ("polite", "Polite"),
        ("careful", "Handled Food Carefully"),
        ("late", "Late Delivery"),
        ("rude", "Rude Behavior"),
        ("spillage", "Food Spillage"),
        ("wrong_address", "Wrong Address"),
    ]

    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name="rider_ratings")
    customer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="rider_ratings")

    raider_id = models.CharField(max_length=255, db_index=True)  # from Order.raider_id
    rider_name = models.CharField(max_length=255, blank=True, null=True)

    stars = models.PositiveSmallIntegerField()  # 1..5
    comment = models.TextField(blank=True)
    tags = models.JSONField(default=list, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    edited_at = models.DateTimeField(blank=True, null=True)
    is_edited = models.BooleanField(default=False)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["order", "customer"], name="unique_rider_rating_per_order_customer"),
        ]
        indexes = [
            models.Index(fields=["raider_id", "-created_at"]),
        ]

    def __str__(self):
        return f"{self.stars}★ for {self.raider_id} (order #{self.order_id})"


class RiderScore(models.Model):
    """Denormalized aggregates for instant display, keyed by raider_id."""
    raider_id = models.CharField(max_length=255, unique=True)
    rider_name = models.CharField(max_length=255, blank=True, null=True)

    ratings_count = models.PositiveIntegerField(default=0)
    ratings_sum = models.PositiveIntegerField(default=0)
    rating_avg = models.DecimalField(max_digits=3, decimal_places=2, default=0.00)
    last_rated_at = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.rider_name or self.raider_id}: {self.rating_avg}★ ({self.ratings_count})"
