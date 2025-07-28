from django.utils.translation import gettext_lazy as _
from drf_writable_nested import WritableNestedModelSerializer
from rest_framework import serializers
from decimal import Decimal
from reward.models import (AdditionalCondition, Reward, RewardGroup,
                           RewardLevel, RewardManage, UserReward, LocalDeal,Campaign,RewardRedemption,Task,Cut)
from food.models import MenuItem, Restaurant, Location


class BaseAdditionalConditionSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdditionalCondition
        fields = '__all__'
        extra_kwargs = {
            'reward_group': {'required': False}
        }


class BaseRewardSerializer(serializers.ModelSerializer):
    item_names = serializers.SerializerMethodField()
    reward_group_name = serializers.SerializerMethodField()

    item_details = serializers.SerializerMethodField()

    class Meta:
        model = Reward
        fields = '__all__'

    def get_item_names(self, obj: Reward):
        return list(obj.items.values_list('name', flat=True))

    def get_reward_group_name(self, obj: Reward):
        return obj.reward_group.name

    # def get_item_details(self, obj: Reward):
    #     return obj.items.all().values('id', 'name', 'menu', 'menu__title', '')
    def get_item_details(self, obj: Reward):
        from food.api.v2.serializers import MenuItemPreviewSerializer
        return MenuItemPreviewSerializer(obj.items.all(), many=True).data


class BaseRewardGroupSerializer(WritableNestedModelSerializer):
    reward_set = BaseRewardSerializer(many=True)
    additionalcondition_set = BaseAdditionalConditionSerializer(many=True)

    class Meta:
        model = RewardGroup
        fields = '__all__'


class BaseRewardGroupBriefSerializer(BaseRewardGroupSerializer):
    reward_set = None


class BaseExtendedRewardSerializer(BaseRewardSerializer):
    reward_group = BaseRewardGroupBriefSerializer()


class BaseUserRewardCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserReward
        fields = '__all__'
        extra_kwargs = {
            'user': {'required': False},
            'amount': {'required': True},
            'is_claimed': {'read_only': True},
        }


class BaseUserRewardSerializer(BaseUserRewardCreateSerializer):
    reward = BaseExtendedRewardSerializer()


class BaseRewardManageSerializer(WritableNestedModelSerializer):
    reward_group_details = BaseRewardGroupSerializer(
        read_only=True, source='reward_group')

    class Meta:
        model = RewardManage
        fields = '__all__'
        extra_fields = {
            'company': {'required': False}
        }


class BaseRewardLevelSerializer(WritableNestedModelSerializer):
    reward_manages = BaseRewardManageSerializer(many=True)

    class Meta:
        model = RewardLevel
        fields = '__all__'
        extra_fields = {
            'company': {'required': False}
        }

class BaseLocalDealSerializer(serializers.ModelSerializer):
    restaurant = serializers.PrimaryKeyRelatedField(queryset=Restaurant.objects.all())
    menu_item = serializers.PrimaryKeyRelatedField(queryset=MenuItem.objects.all())
    location = serializers.PrimaryKeyRelatedField(queryset=Location.objects.all(), required=False)
    item_details = serializers.SerializerMethodField()

    distance = serializers.SerializerMethodField()
    discount_percent = serializers.SerializerMethodField()
    restaurant_rating = serializers.SerializerMethodField()

    class Meta:
        model = LocalDeal
        fields = '__all__'

    def get_distance(self, obj):
        return getattr(obj, 'distance', None)
      
    def get_item_details(self, obj):
        from food.api.v2.serializers import MenuItemPreviewSerializer
        return MenuItemPreviewSerializer(obj.menu_item).data

    def get_discount_percent(self, obj):
        try:
            base_price = Decimal(obj.main_price or obj.menu_item.base_price)
            deal_price = Decimal(obj.deal_price)

            if base_price <= 0:
                return 0

            discount = (base_price - deal_price) / base_price * 100
            return round(discount, 2)
        except:
            return 0

    def get_restaurant_rating(self, obj):
        # Use `average_rating` from the Restaurant model
        if hasattr(obj, 'restaurant_rating'):
            return obj.restaurant_rating
        return getattr(obj.restaurant, 'average_rating', 0)
    



class BaseCampaignSerializer(serializers.ModelSerializer):
    class Meta:
        model = Campaign
        fields = '__all__'


class BaseTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task
        fields = '__all__'

class BaseCutSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cut
        fields = '__all__'

class BaseRewardRedemptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = RewardRedemption
        fields = '__all__'

