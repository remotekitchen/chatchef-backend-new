from rest_framework import serializers

from accounts.models import Company
from billing.api.base.serializers import BaseOrderSerializer
from billing.models import ExternalPaymentInformation, Order
from food.models import (Category, Image, Menu, MenuItem, ModifierGroup,
                         OpeningHour, Restaurant, TimeTable)
from integration.models import Platform


class IntegrationTokenSerializer(serializers.Serializer):
    client_id = serializers.CharField(required=True)
    client_secret = serializers.CharField(required=True)
    scope = serializers.CharField(required=True)


class ExternalCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = ['id', 'name', 'description']


class ExternalTimeTableSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeTable
        fields = ['start_time', 'end_time']


class ExternalOpeningHourSerializer(serializers.ModelSerializer):
    opening_hour = ExternalTimeTableSerializer(read_only=True, many=True)

    class Meta:
        model = OpeningHour
        fields = ['day_index', 'opening_hour']


class ExternalImgSerializer(serializers.ModelSerializer):
    class Meta:
        model = Image
        fields = ['id', 'remote_url']


class BaseExternalItemSerializer(serializers.ModelSerializer):
    images = ExternalImgSerializer(read_only=True, many=True)

    class Meta:
        model = MenuItem
        fields = [
            'id', 'name', 'description',
                  'menu', 'base_price', 'virtual_price', 'currency', 'category', 'is_available', 'is_vegan', 'is_vegetarian', 'is_glutenfree', 'have_nuts', 'images'
        ]


class BaseExternalModifierSerializer(serializers.ModelSerializer):
    class Meta:
        model = ModifierGroup
        fields = ['id', 'name', 'description',
                  'requirement_status', 'modifier_items', 'used_by', 'modifier_limit', 'item_limit']


# class BaseExternalMenuSerializer(serializers.ModelSerializer):
#     categories = ExternalCategorySerializer(read_only=True, many=True)
#     opening_hours = ExternalOpeningHourSerializer(read_only=True, many=True)
#     items = BaseExternalItemSerializer(read_only=True, many=True)
#     modifiers = BaseExternalModifierSerializer(read_only=True, many=True)

#     class Meta:
#         model = Menu
#         fields = ['id', 'title', 'description',
#                   'categories', 'opening_hours', 'items', 'modifiers']

class BaseExternalMenuSerializer(serializers.ModelSerializer):
    category_set = ExternalCategorySerializer(read_only=True, many=True)
    opening_hours = ExternalOpeningHourSerializer(read_only=True, many=True)
    menuitem_set = BaseExternalItemSerializer(read_only=True, many=True)
    modifiergroup_set = BaseExternalModifierSerializer(
        read_only=True, many=True)

    class Meta:
        model = Menu
        fields = ['id', 'title', 'description', 'opening_hours', 'category_set',
                  'menuitem_set', 'modifiergroup_set']


class BasePlatformSerializer(serializers.ModelSerializer):
    class Meta:
        model = Platform
        fields = ['id', 'name', 'logo', 'client_id', 'client_secret']


class OnboardingSerializer(serializers.Serializer):
    client_id = serializers.CharField(max_length=255, required=True)
    restaurant_id = serializers.CharField(max_length=255, required=True)
    location_id = serializers.CharField(max_length=255, required=True)
    status = serializers.CharField(max_length=255, required=True)


class ExternalOrderSerializer(BaseOrderSerializer):
    # restaurant = serializers.CharField(max_length=255, required=True)
    class Meta:
        model = Order
        exclude = ['restaurant', 'user', 'company',
                   'location', 'purchase', 'voucher', 'pos_data', 'extra', 'delivery_platform']


class ExternalOrderPaymentDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExternalPaymentInformation
        fields = '__all__'
