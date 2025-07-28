from rest_framework import serializers

from billing.api.base.serializers import BaseUberCreateQuoteSerializer, BaseCreateQuoteSerializer, \
    BaseAddressSerializer, BaseOrderSerializer, BaseStripePaymentSerializer, BasePhoneVerifySerializer, BaseVerifyOTPSerializer


class AddressSerializer(BaseAddressSerializer):
    pass


class CreateQuoteSerializer(BaseUberCreateQuoteSerializer, BaseCreateQuoteSerializer):
    pickup_address_details = AddressSerializer()
    dropoff_address_details = AddressSerializer()
    restaurant = serializers.IntegerField()
    pickup_address = None
    dropoff_address = None
    restaurant_id = None


class OrderSerializer(BaseOrderSerializer):
    # pickup_address_details = AddressSerializer()
    # dropoff_address_details = AddressSerializer()
    pass


class StripePaymentSerializer(OrderSerializer, BaseStripePaymentSerializer):
    pass


class PhoneVerifySerializer(BasePhoneVerifySerializer):
    pass


class VerifyOTPSerializer(BaseVerifyOTPSerializer):
    pass