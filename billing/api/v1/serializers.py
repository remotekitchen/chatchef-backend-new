from billing.api.base.serializers import BaseOrderSerializer, BaseDailyOrderSerializer, BaseHourlyOrderSerializer, \
    BasePaymentDetailsSerializer, BaseStripePaymentSerializer, BaseBillingProfileSerializer, \
    BaseStripeConnectAccountSerializer, BaseOrderItemSerializer, BaseCostCalculationSerializer, \
    BaseRestaurantFeeSerializer


class OrderSerializer(BaseOrderSerializer):
    pass


class OrderItemSerializer(BaseOrderItemSerializer):
    pass


class DailyOrderSerializer(BaseDailyOrderSerializer):
    pass


class HourlyOrderSerializer(BaseHourlyOrderSerializer):
    pass


class PaymentDetailsSerializer(BasePaymentDetailsSerializer):
    pass


class StripePaymentSerializer(BaseStripePaymentSerializer):
    pass


class BillingProfileSerializer(BaseBillingProfileSerializer):
    pass


class StripeConnectAccountSerializer(BaseStripeConnectAccountSerializer):
    pass


class CostCalculationSerializer(BaseCostCalculationSerializer):
    pass


class RestaurantFeeSerializer(BaseRestaurantFeeSerializer):
    pass

