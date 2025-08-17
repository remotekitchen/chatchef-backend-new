from django.utils import timezone
from drf_writable_nested import WritableNestedModelSerializer
from rest_framework import serializers

from accounts.api.v1.serializers import UserSerializer
from billing.models import (BillingProfile, DeliveryFeeAssociation, Invoice,
                            InvoiceItem, Order, OrderedModifiers, OrderItem,
                            OrderModifiersItems, OrderReminder, PaymentDetails,
                            PaymentMethods, PayoutHistory,PayoutHistoryForHungry, RestaurantFee,
                            StripeConnectAccount, Transactions, Wallet)
from billing.utilities.check_bogo import check_bogo
from billing.utilities.check_bogo import check_bxgy
from hungrytiger.settings.defaults import ENV_TYPE
from core.api.serializers import BaseSerializer
from core.models import Address
from core.utils import get_logger
from food.api.base.serializers import (BaseMenuItemSerializer,
                                       BaseModifierGroupSerializer)
from food.models import Location, Restaurant
from marketing.api.v1.serializers import BogoSerializer, BxGySerializer, VoucherGetSerializer
from marketing.models import Voucher
from reward.api.v1.serializers import UserRewardSerializer
from reward.models import UserReward, LocalDeal
from accounts.models import Otp, RestaurantUser
from billing.models import RestaurantContract
from billing.models import Bogo
from decimal import Decimal
import pytz
from datetime import datetime

logger = get_logger()
from billing.models import CANCELLED_BY_CHOICES

class OrderItemCancelEntrySerializer(serializers.Serializer):
    order_item_id = serializers.IntegerField()
    qty = serializers.IntegerField(required=False, allow_null=True, min_value=1)
    reason = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)

class PartialCancelRequestSerializer(serializers.Serializer):
    items = OrderItemCancelEntrySerializer(many=True)
    cancelled_by = serializers.ChoiceField(choices=[c[0] for c in CANCELLED_BY_CHOICES], default="restaurant")
    reason = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)

class BaseOrderModifiersItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = OrderModifiersItems
        fields = "__all__"


class BaseOrderedModifierSerializer(WritableNestedModelSerializer):
    modifiersItems = BaseOrderModifiersItemSerializer(
        many=True, allow_empty=True
    )

    class Meta:
        model = OrderedModifiers
        fields = "__all__"


class BaseOrderItemSerializer(WritableNestedModelSerializer):
    item_name = serializers.SerializerMethodField()
    item_price = serializers.SerializerMethodField(read_only=True)
    modifiers = BaseOrderedModifierSerializer(many=True, allow_empty=True)
    active_quantity = serializers.SerializerMethodField()

    class Meta:
        model = OrderItem
        fields = "__all__"
        extra_kwargs = {
            "order": {"required": False},
            "modifiers": {"required": False},
        }

    def get_item_name(self, obj):
        # Works for both OrderItem instances and dicts
        if isinstance(obj, OrderItem):
            return obj.menu_item.name if obj.menu_item else ""
        if isinstance(obj, dict):
            mi = obj.get("menu_item")
            if isinstance(mi, dict):
                return mi.get("name", "")
        return ""

    def get_item_price(self, obj):
        # Works for both OrderItem instances and dicts
        if isinstance(obj, OrderItem):
            return obj.menu_item.base_price if obj.menu_item else None
        if isinstance(obj, dict):
            mi = obj.get("menu_item")
            if isinstance(mi, dict):
                return mi.get("base_price")
        return None

    def get_active_quantity(self, obj):
        # If it’s a real model instance, use the property you added
        if isinstance(obj, OrderItem):
            return obj.active_quantity

        # During request->representation, DRF may pass an OrderedDict
        if isinstance(obj, dict):
            try:
                qty = int(obj.get("quantity") or 0)
                canceled = int(obj.get("canceled_quantity") or 0)
                is_canceled = bool(obj.get("is_canceled") or False)
                active = max(qty - canceled, 0)
                return 0 if is_canceled else active
            except Exception:
                return 0

        return 0



class BaseOrderItemDetailSerializer(BaseOrderItemSerializer):
    item_detail = BaseMenuItemSerializer(read_only=True)


class OrderReminderSerializer(serializers.ModelSerializer):
    class Meta:
        model = OrderReminder
        fields = '__all__'


# Uber Serializer
class BaseAddressSerializer(serializers.ModelSerializer):
    class Meta:
        model = Address
        fields = '__all__'


class BaseOrderSerializer(WritableNestedModelSerializer):
    orderitem_set = BaseOrderItemSerializer(many=True, allow_empty=False)
    voucher = serializers.CharField(required=False, allow_blank=True)
    set_reminder = OrderReminderSerializer(many=False, required=False)
    pickup_address_details = BaseAddressSerializer(required=False)
    dropoff_address_details = BaseAddressSerializer(required=False)
    customer_email = serializers.SerializerMethodField()
    email = serializers.EmailField(required=True)
    local_deal = serializers.PrimaryKeyRelatedField(
        many=True,
        queryset=LocalDeal.objects.all(),
        required=False,
    )
    local_deal_validity = serializers.SerializerMethodField(
        read_only=True
    )
    class Meta:
        model = Order
        fields = "__all__"
        extra_kwargs = {
            "customer": {"required": False},
            "email": {"required": True},
            "tips": {"required": False},
            "dropoff_contact_first_name": {"required": False},
            "dropoff_contact_last_name": {"required": False},
            "status": {"required": False},
            "pickup_time": {"required": False},
            "dropoff_location": {"required": False},
            "tracking_url": {"read_only": True},
            "cancellation_reason": {"read_only": True},
            "delivery_time": {"read_only": True},
            # "is_paid": {"read_only": True},
            "subtotal": {"required": False},
            "quantity": {"required": False},
            "receive_date": {"read_only": True},
            "order_id": {"read_only": True},
            "extra": {"read_only": True},
            "user": {"required": False},
            "purchase": {"read_only": True},
            "support_reference": {"read_only": True},
            "company": {"read_only": True},
        }

    # def validate(self, data):
    #     bogo = data.get('bogo', None)
    #     if bogo is not None:
    #         check_bogo(
    #             order_list=data.get('orderitem_set'), bogo_id=bogo, location_id=data.get('location')
    #         
    # )
    
    # get the validity date from local deal end_time
    def get_local_deal_validity(self, obj: Order):
        if type(obj).__name__ != "Order":
            return None
        local_deal = obj.local_deal.first() if obj.local_deal.exists() else None
        if local_deal:
            return local_deal.end_time
        return None
      
    def get_customer_email(self, obj: Order):
        if type(obj).__name__ != "Order":
            return ""
        return obj.user.email if obj.user is not None else ''

    def create(self, validated_data, user=None ):
        local_deal_data = validated_data.pop('local_deal', None)
        try:
            print('------------------------------------>1500001')
            user = self.context["request"].user
            if user.is_authenticated:
                validated_data["user"] = user  # ✅ Only assign real users
            else:
                print("⚠️ Anonymous user, skipping user assignment")
            order_items = validated_data.get("orderitem_set")
            fee = validated_data.get("delivery_fee")
            voucher = validated_data.pop("voucher", None)
            bogo = validated_data.get("bogo", None)
            bxgy = validated_data.get("bxgy", None)
            print(user, '------------------------------------>150000')
            if bogo is not None:
                check_bogo(
                    order_list=validated_data.get("orderitem_set"),
                    bogo_id=bogo,
                    location_id=validated_data.get("location"),
                )
            if bxgy is not None:
                check_bxgy(
                    order_list=validated_data.get("orderitem_set"),
                    bxgy_id=bxgy,
                    location_id=validated_data.get("location"),
                )
            order = super().create(validated_data)
            print(f"Order saved with PK: {order.pk}")
            
            #Now assign M2M relation if present
            if local_deal_data:
                order.local_deal.set(local_deal_data)


            # contract = RestaurantContract.objects.filter(restaurant=order.restaurant).first()
            # # bogo = Bogo.objects.filter(items_id=item_id).first()

            # print('contract found---', contract.commission_percentage)    
            # # # Set all new cost breakdown fields with default initial values (example: 100)


            # base_price = 0
            # total_item_price = 0

            # print("Order data----99")

            # for item in order.order_item_meta_data:
            #     base_price = item["menu_item"]["base_price"]
            #     quantity = item["quantity"]

            #     if item.get("is_bogo"):
            #         paid_quantity = quantity // 2
            #     else:
            #         paid_quantity = quantity

            #     item_total = base_price * paid_quantity

            #     print(f"[DEBUG] Item: Base Price = {base_price}, Quantity = {quantity}, Paid Quantity = {paid_quantity}, Total = {item_total}")

            #     total_item_price += item_total

            # if (order.bogo_discount > 0 ):
            #     actual_selling_price =  total_item_price 
            # else:
            #     actual_selling_price = order.total   

            #     # actual_selling_price = (
            #     #     total_item_price if order_bogo_discount > 0
            #     #     else order.total
            #     # )

            # def calculate_actual_item_price(item):
            #         print("inner-loop", bogo_inflation_percent)
            #         base_price = item["menu_item"]["base_price"]
            #         quantity = item["quantity"]

            #         # Default to 40% inflation if not provided
            #         bogo_inflation_percent = item.get("inflate_percent", 40)

            #         if item.get("is_bogo"):
            #             # Remove BOGO inflation properly
            #             actual_unit_price = base_price / (1 + (bogo_inflation_percent / 100))
            #             actual_price = actual_unit_price * quantity
            #             return round(actual_price, 2)

            #         # Normal (non-BOGO) item
            #         return round(base_price * quantity, 2)




            # # Loop through order items and calculate total actual price
            # total_actual_price = sum(calculate_actual_item_price(item) for item in order.order_item_meta_data)

            # def calculate_commission_amount():
            #     return round(total_item_price * (contract.commission_percentage / 100), 2)

            # commission_amount = calculate_commission_amount()


            # total_actual_price = Decimal(str(total_actual_price))
            # total_item_price = Decimal(str(total_item_price))
            # order_discount = Decimal(str(order.discount or 0))
            # order_bogo_discount = Decimal(str(order.bogo_discount or 0))
            # commission_amount = Decimal(str(commission_amount))
            # # Now calculate safely
            # bogo_loss = total_actual_price - total_item_price
            # voucher_discount = order_discount - order_bogo_discount
            # main_discount = voucher_discount + bogo_loss

            # restaurant_discount = main_discount / Decimal("2")
            # ht_discount = main_discount / Decimal("2")

            # amount_to_restaurant = total_actual_price - commission_amount - ht_discount
            # amount_to_restaurant = amount_to_restaurant.quantize(Decimal("1.00"))  # Optional rounding


            # print("order value update checking---", order.bogo_discount, total_actual_price, order.total)

            # order.commission_amount = Decimal(contract.commission_percentage or 0)
            # order.selling_price_inclusive_tax = Decimal(actual_selling_price or 0)
            # order.amount_to_restaurant = Decimal(amount_to_restaurant or 0)
            # order.restaurant_discount = Decimal(restaurant_discount or 0)
            # order.hungry_tiger_discount = Decimal(ht_discount or 0)
            # order.actual_discount = Decimal(main_discount or 0)
            # order.bogo_loss = Decimal(bogo_loss or 0)
            # order.actual_item_price = Decimal(actual_selling_price or 0)


            # calculator = CostCalculation()
            # costs = calculator.get_updated_cost(
            #     order_list=BaseOrderItemSerializer(validated_data.get('orderitem_set'), many=True).data,
            #     delivery=fee, voucher=voucher,
            #     location=order.location,
            #     order_method=order.order_method
            # )
            # subtotal, quantity = self.get_subtotal(order_items, order.id)
            # order.subtotal = costs.get('order_value')
            # order.quantity = costs.get('quantity')
            # order.delivery_fee = costs.get('delivery_fee')
            # order.tax = costs.get('tax')
            # order.convenience_fee = costs.get('convenience_fee')
            # order.total = costs.get('total')
            # order.discount = costs.get('discount')
            order.company = order.restaurant.company
            order.receive_date = timezone.now()
            bdt = pytz.timezone('Asia/Dhaka')
            bdt_time = timezone.now().astimezone(bdt)
            order.receive_date_ht = bdt_time
           
            print("formatted_time final2", bdt_time)
          
            print('------------------------------------>162')
            # if self.context.get("request") is not None:
            #     order.user = self.context.get("request").user
            voucher_obj = Voucher.objects.filter(
                voucher_code=voucher, location=order.location
            ).first()
            
            if voucher_obj is not None:
                order.voucher = voucher_obj
            elif user is not None and user.is_authenticated:
                user_reward = UserReward.objects.filter(code=voucher, user=user).first()
                print(user_reward, '------------------------------------>170')
                if user_reward is not None and not user_reward.is_claimed:
                    order.reward_coupon = user_reward
                    """ For cash orders, make the reward coupons claimed now """
                    if order.payment_method == Order.PaymentMethod.CASH:
                        user_reward.is_claimed = True
                        user_reward.save(update_fields=["is_claimed"])
            order.save(
                update_fields=[
                    # 'subtotal', 'quantity', 'delivery_fee', 'tax', 'convenience_fee', 'total','discount',
                    "receive_date",
                    # "user",
                    "company",
                    "voucher",
                    "reward_coupon",
                    # "actual_item_price",
                    # "bogo_loss",
                    # "bogo_inflation_percentage",
                    # "actual_discount",
                    # "hungry_tiger_discount",
                    # "restaurant_discount",
                    # "commission_amount",
                    # "selling_price_inclusive_tax",
                    # "amount_to_restaurant",
                ]
            )
            
            print('------------------------------------>189')

            return order
        except Exception as e:
            logger.error(f"order create error {e}")
            raise e


    def get_subtotal(self, order_items, order_id):
        obj = []
        subtotal, quantity = 0, 0
        for item in order_items:
            order_item = OrderItem(
                order_id=order_id,
                menu_item=item.get("menu_item"),
                quantity=item.get("quantity"),
            )
            subtotal += order_item.total_cost
            quantity += item.get("quantity")
            obj.append(order_item)

        OrderItem.objects.bulk_create(obj)
        return subtotal, quantity


class BasePeriodicOrderSerializer(BaseSerializer):
    total_sale = serializers.FloatField()
    total_volume = serializers.IntegerField()


class BaseDailyOrderSerializer(BasePeriodicOrderSerializer):
    date = serializers.DateField()


class BaseHourlyOrderSerializer(BasePeriodicOrderSerializer):
    hour = serializers.IntegerField()


class BaseCreateQuoteSerializer(BaseSerializer):
    dropoff_address = serializers.CharField()
    dropoff_phone_number = serializers.CharField(required=False)
    pickup_address = serializers.CharField()
    pickup_business_name = serializers.CharField()
    pickup_phone_number = serializers.CharField()
    pickup_external_store_id = serializers.CharField(
        required=False, allow_blank=True)
    pickup_time = serializers.CharField(required=False)
    first_name = serializers.CharField(required=False)
    last_name = serializers.CharField(required=False)
    tips = serializers.IntegerField(required=False)
    order_list = BaseOrderItemSerializer(many=True)
    voucher = serializers.CharField(required=False, allow_blank=True)
    location = serializers.IntegerField(required=False)
    bogo = serializers.IntegerField(required=False,  allow_null=True)
    bxgy = serializers.IntegerField(required=False,  allow_null=True)
    spend_x_save_y = serializers.IntegerField(required=False)
    email = serializers.EmailField(required=True)

    def validate(self, data):
        bogo = data.get("bogo", None)
        bxgy = data.get("bxgy", None)
        if bogo is not None:
            check_bogo(
                order_list=data.get("order_list"),
                bogo_id=data.get("bogo"),
                location_id=data.get("location"),
            )
        if bxgy is not None:
            check_bxgy(
                order_list=data.get("order_list"),
                bxgy_id=data.get("bxgy"),
                location_id=data.get("location"),
            )
        return data


class BaseAcceptQuoteSerializer(BaseSerializer):
    external_delivery_id = serializers.CharField()
    restaurant = serializers.IntegerField()
    location = serializers.IntegerField()
    tips = serializers.FloatField(required=False)
    order_id = serializers.IntegerField()


class BasePaypalCreateOrderSerializer(BaseSerializer):
    items = BaseOrderItemSerializer(many=True)
    restaurant = serializers.IntegerField()


class BasePaymentDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = PaymentDetails
        fields = "__all__"
        extra_kwargs = {"company": {"read_only": True}}


class AddressSerializer(BaseSerializer):
    street_address = serializers.ListField(
        child=serializers.CharField(required=False, allow_blank=True)
    )
    state = serializers.CharField(max_length=50)
    city = serializers.CharField(max_length=100)
    zip_code = serializers.CharField(max_length=10)
    country = serializers.CharField(max_length=50)


class BaseUberCreateQuoteSerializer(BaseSerializer):
    pickup_address = AddressSerializer()
    dropoff_address = AddressSerializer()
    pickup_phone_number = serializers.CharField()
    dropoff_phone_number = serializers.CharField()

    restaurant_id = serializers.CharField()


# Uber create Delivery
class DimensionsSerializer(BaseSerializer):
    length = serializers.IntegerField()
    height = serializers.IntegerField()
    depth = serializers.IntegerField()


class ManifestItemSerializer(serializers.Serializer):
    name = serializers.CharField()
    quantity = serializers.IntegerField()
    # weight = serializers.IntegerField()
    # dimensions = DimensionsSerializer()


class RoboCourierSpecificationSerializer(serializers.Serializer):
    mode = serializers.CharField()


class TestSpecificationsSerializer(serializers.Serializer):
    robo_courier_specification = RoboCourierSpecificationSerializer()


class DeliveryRequestSerializer(BaseSerializer):
    quote_id = serializers.CharField()
    pickup_address = AddressSerializer()
    pickup_name = serializers.CharField()
    pickup_phone_number = serializers.CharField()
    # pickup_latitude = serializers.FloatField()
    # pickup_longitude = serializers.FloatField()
    dropoff_address = AddressSerializer()
    dropoff_name = serializers.CharField()
    dropoff_phone_number = serializers.CharField()
    # dropoff_latitude = serializers.FloatField()
    # dropoff_longitude = serializers.FloatField()
    manifest_items = ManifestItemSerializer(many=True)
    tip = serializers.IntegerField()
    restaurant_id = serializers.CharField()
    test_specifications = TestSpecificationsSerializer()


class BaseStripePaymentSerializer(BaseOrderSerializer, BaseCreateQuoteSerializer):
    currency = serializers.CharField(required=False)
    # voucher = serializers.CharField(write_only=True, required=False)


class BaseInvoiceItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = InvoiceItem
        fields = "__all__"


class BaseInvoiceSerializer(serializers.ModelSerializer):
    invoiceitem_set = BaseInvoiceItemSerializer(many=True)

    class Meta:
        model = Invoice
        fields = "__all__"


class BaseDeliveryFeeAssociationSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeliveryFeeAssociation
        fields = "__all__"
        extra_kwargs = {"company": {"required": False}}


class BaseStripeConnectAccountBriefSerializer(serializers.ModelSerializer):
    class Meta:
        model = StripeConnectAccount
        exclude = ["account_details"]


class BaseBillingProfileSerializer(serializers.ModelSerializer):
    stripe_connect_account = BaseStripeConnectAccountBriefSerializer(
        read_only=True
    )

    class Meta:
        model = BillingProfile
        fields = "__all__"


class BaseStripeConnectAccountSerializer(serializers.ModelSerializer):
    class Meta:
        model = StripeConnectAccount
        fields = "__all__"


class BaseCostCalculationSerializer(BaseSerializer):
    items = BaseOrderItemSerializer(many=True)
    voucher = serializers.CharField(required=False, allow_blank=True)
    spend_x_save_y = serializers.CharField(required=False, allow_blank=True)
    location = serializers.IntegerField(required=False)
    order_method = serializers.CharField(required=False)
    delivery_fee = serializers.FloatField(required=False)
    bogo = serializers.IntegerField(required=False, allow_null=True)
    bxgy = serializers.IntegerField(required=False, allow_null=True)
    on_time_guarantee_opted_in = serializers.BooleanField(required=False)
    on_time_guarantee_fee = serializers.FloatField(required=False)


class CostCalculationDeliverySerializer(BaseCreateQuoteSerializer, BaseCostCalculationSerializer):
    orderitem_set = BaseOrderItemSerializer(
        many=True, allow_empty=False, required=True
    )


class BaseOrderReportSerializer(BaseSerializer):
    from_date = serializers.DateTimeField()
    to_date = serializers.DateTimeField()
    restaurant = serializers.PrimaryKeyRelatedField(
        queryset=Restaurant.objects.all(), required=False, allow_null=True
    )
    location = serializers.PrimaryKeyRelatedField(
        queryset=Location.objects.all(), required=False, allow_null=True
    )


class BasePaymentInitiationReportSerializer(BaseOrderReportSerializer):
    pass


class BaseTopUpSerializer(BaseSerializer):
    CURRENCY_CHOICES = [
        ('CAD', 'CAD'),
    ]

    GATEWAY_CHOICES = [
        ('stripe', 'stripe'),
        ('paypal', 'paypal'),
    ]

    amount = serializers.FloatField()
    currency = serializers.ChoiceField(choices=CURRENCY_CHOICES)
    restaurant = serializers.CharField()
    gateway = serializers.ChoiceField(choices=GATEWAY_CHOICES)


class BasePaymentMethodSerializer(serializers.ModelSerializer):
    class Meta:
        model = PaymentMethods
        fields = '__all__'


class MenuItemSerializerWithModifiersDetails(BaseMenuItemSerializer):
    base_price = serializers.SerializerMethodField()
    original_price = serializers.SerializerMethodField()
    class Meta(BaseMenuItemSerializer.Meta):
        fields = ['id', 'name', 'base_price','original_price', 'virtual_price']
    
    def get_base_price(self, obj):
        return float(obj.base_price) if obj.base_price is not None else 0.0

    def get_original_price(self, obj):
        return float(obj.original_price) if obj.original_price is not None else 0.0



class ModifierGroupSerializerForOrderDetails(BaseModifierGroupSerializer):
    class Meta(BaseModifierGroupSerializer.Meta):
        fields = ['id', 'name']


class BaseGetOrderModifiersItemSerializer(serializers.ModelSerializer):
    modifiersOrderItems = MenuItemSerializerWithModifiersDetails(many=False)

    class Meta:
        model = OrderModifiersItems
        fields = "__all__"


class BaseGetOrderedModifierSerializer(serializers.ModelSerializer):
    modifiersItems = BaseGetOrderModifiersItemSerializer(many=True)
    modifiers = ModifierGroupSerializerForOrderDetails(many=False)

    class Meta:
        model = OrderedModifiers
        fields = "__all__"


class BaseOrderItemSerializerWithModifiersDetails(serializers.ModelSerializer):
    modifiers = BaseGetOrderedModifierSerializer(many=True, allow_empty=True)
    menu_item = MenuItemSerializerWithModifiersDetails(many=False)

    class Meta:
        model = OrderItem
        fields = "__all__"


class BaseOrderGetSerializerWithModifiersDetails(serializers.ModelSerializer):
    orderitem_set = BaseOrderItemSerializerWithModifiersDetails(
        many=True, allow_empty=False
    )
    bogo = BogoSerializer()
    bxgy = BxGySerializer()
    voucher = VoucherGetSerializer()
    reward_coupon = UserRewardSerializer()

    class Meta:
        model = Order
        fields = '__all__'


class BaseRecreateDeliverySerializer(BaseSerializer):
    order_id = serializers.IntegerField()


class BaseRestaurantFeeSerializer(serializers.ModelSerializer):
    class Meta:
        model = RestaurantFee
        fields = '__all__'
        extra_kwargs = {
            'company': {'required': False}
        }


class BasePhoneVerifySerializer(serializers.Serializer):
    phone = serializers.CharField(max_length=15)


class BaseVerifyOTPSerializer(BasePhoneVerifySerializer):
    otp = serializers.IntegerField()


class BaseCancelDeliverySerializer(BaseSerializer):
    order_id = serializers.IntegerField()
    status = serializers.ChoiceField(choices=Order.StatusChoices)


class BasePayoutHistorySerializer(serializers.ModelSerializer):
    restaurant_name = serializers.SerializerMethodField()
    location_name = serializers.SerializerMethodField()
    customer_tax_pst = serializers.SerializerMethodField()
    customer_tax_gst = serializers.SerializerMethodField()
    restaurant_tax_gst = serializers.SerializerMethodField()
    restaurant_tax_pst = serializers.SerializerMethodField()

    class Meta:
        model = PayoutHistory
        fields = '__all__'

    def get_restaurant_name(self, obj):
        return f"{obj.restaurant.name}"

    def get_location_name(self, obj):
        return f"{obj.location.name}"

    def get_customer_tax_gst(self, obj: PayoutHistory):
        gst, pst = self.calculate_gst_pst(obj.tax_paid_by_customer, 0.05, 0.07)
        return gst

    def get_customer_tax_pst(self, obj: PayoutHistory):
        gst, pst = self.calculate_gst_pst(obj.tax_paid_by_customer, 0.05, 0.07)
        return pst

    def get_restaurant_tax_gst(self, obj: PayoutHistory):
        gst, pst = self.calculate_gst_pst(
            obj.tax_paid_by_restaurant, 0.05, 0.07)
        return pst

    def get_restaurant_tax_pst(self, obj: PayoutHistory):
        gst, pst = self.calculate_gst_pst(
            obj.tax_paid_by_restaurant, 0.05, 0.07)
        return pst

    def calculate_gst_pst(self, tax_amount, gst_rate, pst_rate):
        total_tax_rate = gst_rate + pst_rate
        taxable_amount = tax_amount / total_tax_rate
        gst = gst_rate * taxable_amount
        pst = pst_rate * taxable_amount
        return gst, pst
    



class BasePayoutHistoryForHungrySerializer(serializers.ModelSerializer):
    restaurant_name = serializers.SerializerMethodField()
    location_name = serializers.SerializerMethodField()
    customer_tax_pst = serializers.SerializerMethodField()
    customer_tax_gst = serializers.SerializerMethodField()
    restaurant_tax_gst = serializers.SerializerMethodField()
    restaurant_tax_pst = serializers.SerializerMethodField()

    class Meta:
        model = PayoutHistoryForHungry
        fields = '__all__'

    def get_restaurant_name(self, obj):
        return f"{obj.restaurant.name}"

    def get_location_name(self, obj):
        return f"{obj.location.name}"

    def get_customer_tax_gst(self, obj):
        gst, pst = self.calculate_gst_pst(obj.tax, 0.05, 0.07)  # updated
        return gst

    def get_customer_tax_pst(self, obj):
        gst, pst = self.calculate_gst_pst(obj.tax, 0.05, 0.07)  # updated
        return pst

    def get_restaurant_tax_gst(self, obj):
        gst, pst = self.calculate_gst_pst(0, 0.05, 0.07)  # no restaurant tax field in model
        return gst

    def get_restaurant_tax_pst(self, obj):
        gst, pst = self.calculate_gst_pst(0, 0.05, 0.07)  # no restaurant tax field in model
        return pst

    def calculate_gst_pst(self, tax_amount, gst_rate, pst_rate):
        total_tax_rate = gst_rate + pst_rate
        if total_tax_rate == 0:
            return 0, 0
        taxable_amount = tax_amount / total_tax_rate
        gst = gst_rate * taxable_amount
        pst = pst_rate * taxable_amount
        return gst, pst


class BasePayoutHistoryUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = PayoutHistory
        fields = ['adjustments', 'adjustments_note']


class BaseTransactionsSerializer(serializers.ModelSerializer):
    restaurant_name = serializers.SerializerMethodField()
    

    class Meta:
        model = Transactions
        fields = '__all__'

    def get_restaurant_name(self, obj: Transactions):
        print(obj.restaurant)
        return obj.restaurant.name 
    


class BaseWalletSerializer(serializers.ModelSerializer):
    class Meta:
        model = Wallet
        fields = "__all__"


class WalletGETSerializer(BaseWalletSerializer):
    user = UserSerializer


class BaseGiftCardWalletSerializer(serializers.Serializer):
    amount = serializers.IntegerField()
    receiver = serializers.EmailField()
    restaurant = serializers.CharField()
    gateway = serializers.ChoiceField(
        choices=[
            ("wallet", "Wallet"),
            ("stripe", "Stripe")
        ]
    )


class AddressSerializer(serializers.Serializer):
    street_address = serializers.CharField(max_length=255)
    city = serializers.CharField(max_length=100)
    state = serializers.CharField(max_length=100)
    postal_code = serializers.CharField(max_length=20)
    country = serializers.CharField(max_length=100)


class BaseRaiderAppCheckAddressSerializer(serializers.Serializer):
    pickup_address = AddressSerializer()
    pickup_customer_name = serializers.CharField(max_length=255)
    pickup_phone = serializers.CharField(max_length=20)
    pickup_ready_at = serializers.DateTimeField()
    pickup_last_time = serializers.DateTimeField()
    drop_off_address = AddressSerializer()
    drop_off_customer_name = serializers.CharField(max_length=255)
    drop_off_phone = serializers.CharField(max_length=20)
    drop_off_last_time = serializers.DateTimeField()
    currency = serializers.CharField(max_length=3)
    tips = serializers.DecimalField(max_digits=10, decimal_places=2)
    amount = serializers.DecimalField(max_digits=10, decimal_places=2)
    payment_type = serializers.CharField(max_length=50)
    pickup_latitude = serializers.FloatField()
    pickup_longitude = serializers.FloatField()



class BaseOrderDeliveryExpenseSerializer(serializers.ModelSerializer):
    user_email = serializers.EmailField(source='user.email', read_only=True)
    user_id = serializers.IntegerField(source='user.id', read_only=True)
    restaurant_name = serializers.CharField(source='restaurant.name', read_only=True)

    class Meta:
        model = Order
        fields = '__all__' 






class BaseCartItemSerializer(serializers.Serializer):
    menu_item_id = serializers.IntegerField()
    quantity = serializers.IntegerField(min_value=1)