from django.contrib.contenttypes.models import ContentType
from drf_writable_nested import WritableNestedModelSerializer
from rest_framework import serializers

from accounts.api.v1.serializers import UserSerializer
from marketing.models import (BirthdayGift, Bogo, BxGy, BxGyBuyItem, BxGyFreeItem, ContactUsData, DemoData,
                              Duration, EmailConfiguration, EmailHistory,
                              FissionCampaign, FissionPrize, GiftCard,
                              GroupPromotion, GroupPromotionOption,
                              LoyaltyProgram, MembershipCard, Rating, Review,
                              SpendXSaveY, SpendXSaveYManager,
                              SpendXSaveYPromoOption, Voucher, Comment, MenuItem, AutoReplyToComments, Restaurant)
from reward.api.base.serializers import BaseRewardSerializer


class BaseDurationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Duration
        fields = "__all__"


class BaseSpendXSaveYPromoOptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = SpendXSaveYPromoOption
        fields = "__all__"


class BaseActivationCampaignSerializer(WritableNestedModelSerializer):
    durations = BaseDurationSerializer(many=True)
    company = serializers.PrimaryKeyRelatedField(read_only=True)
    restaurant_name = serializers.SerializerMethodField()
    location_name = serializers.SerializerMethodField()

    def get_restaurant_name(self, obj):
        try:
            return obj.restaurant.name
        except:
            return ""

    def get_location_name(self, obj):
        try:
            return obj.location.name
        except:
            return ""


class BaseSpendXSaveYSerializer(BaseActivationCampaignSerializer):
    class Meta:
        model = SpendXSaveY
        fields = "__all__"

    def create(self, validated_data):
        validated_data.update({
            'company': validated_data.get('manager').company
        })
        return super().create(validated_data)


class BaseSpendXSaveYManagerSerializer(BaseActivationCampaignSerializer):
    spendxsavey_set = BaseSpendXSaveYSerializer(many=True)

    class Meta:
        model = SpendXSaveYManager
        fields = "__all__"


class BaseVoucherSerializer(BaseActivationCampaignSerializer):
    reward_details = BaseRewardSerializer(read_only=True)
    applies_for = serializers.SerializerMethodField()
    # reward = BaseRewardSerializer(read_only=True)

    class Meta:
        model = Voucher
        fields = "__all__"
    
    def get_applies_for(self, obj):
        if obj.reward and obj.reward.reward_group:
            return obj.reward.reward_group.applies_for
        return []
     


class BaseBogoSerializer(BaseActivationCampaignSerializer):
    item_names = serializers.SerializerMethodField()

    class Meta:
        model = Bogo
        fields = "__all__"

    def get_item_names(self, obj: Bogo):
        try:
            return ",".join(list(obj.items.all().values_list("name", flat=True)))
        except:
            return ""

class BxGyFreeItemSerializer(serializers.ModelSerializer):
    free_items = serializers.ListField(child=serializers.IntegerField())

    class Meta:
        model = BxGyFreeItem
        fields = ['free_items', 'quantity']

class BxGyBuyItemSerializer(serializers.ModelSerializer):
    buy_items = serializers.ListField(child=serializers.IntegerField())
    free_items = BxGyFreeItemSerializer(many=True)

    class Meta:
        model = BxGyBuyItem
        fields = ['buy_items', 'quantity', 'free_items']

class BaseBxGySerializer(BaseActivationCampaignSerializer):
    buy_items = BxGyBuyItemSerializer(many=True)  # related_name on BxGyBuyItem FK
    item_names = serializers.SerializerMethodField()
    class Meta:
        model = BxGy
        fields = "__all__"
        
    def get_item_names(self, obj):
        return ",".join(obj.items.all().values_list("name", flat=True))
      
    def create(self, validated_data):
        durations_data = validated_data.pop('durations', [])
        buy_items_data = validated_data.pop('buy_items', [])
        items_ids = validated_data.pop('items', [])

        campaign = BxGy.objects.create(**validated_data)

        # Handle durations nested objects
        duration_instances = []
        for duration_data in durations_data:
            duration = Duration.objects.create(**duration_data)
            duration_instances.append(duration)
        campaign.durations.set(duration_instances)

        campaign.items.set(items_ids)

        for buy_item_data in buy_items_data:
            free_items_data = buy_item_data.pop('free_items', [])
            buy_item_instance = BxGyBuyItem.objects.create(campaign=campaign, **buy_item_data)
            for free_item_data in free_items_data:
                BxGyFreeItem.objects.create(buy_item_relation=buy_item_instance, **free_item_data)

        return campaign


    def update(self, instance, validated_data):
        durations_data = validated_data.pop('durations', [])
        buy_items_data = validated_data.pop('buy_items', [])
        items_ids = validated_data.pop('items', [])

        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()

        if durations_data:
            duration_instances = []
            for duration_data in durations_data:
                duration = Duration.objects.create(**duration_data)
                duration_instances.append(duration)
            instance.durations.set(duration_instances)

        if items_ids:
            instance.items.set(items_ids)

        if buy_items_data:
            instance.buy_items.all().delete()
            for buy_item_data in buy_items_data:
                free_items_data = buy_item_data.pop('free_items', [])
                buy_item_instance = BxGyBuyItem.objects.create(campaign=instance, **buy_item_data)
                for free_item_data in free_items_data:
                    BxGyFreeItem.objects.create(buy_item_relation=buy_item_instance, **free_item_data)

        return instance
      
class BaseGroupPromotionOptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = GroupPromotionOption
        fields = "__all__"


class BaseGroupPromotionSerializer(BaseActivationCampaignSerializer):
    promo_options = BaseGroupPromotionOptionSerializer(many=True)

    class Meta:
        model = GroupPromotion
        fields = "__all__"


class BaseFissionPrizeSerializer(serializers.ModelSerializer):
    from reward.api.base.serializers import BaseRewardGroupSerializer
    reward_details = BaseRewardGroupSerializer(
        read_only=True, source="reward_group")

    class Meta:
        model = FissionPrize
        fields = "__all__"


class BaseFissionCampaignSerializer(WritableNestedModelSerializer):
    company = serializers.PrimaryKeyRelatedField(read_only=True)
    prize_details = BaseFissionPrizeSerializer(many=True, source="prizes")
    durations = BaseDurationSerializer(many=True, required=False)
    restaurant_name = serializers.SerializerMethodField()

    class Meta:
        model = FissionCampaign
        fields = "__all__"
        extra_kwargs = {
            'last_week_users': {'required': False}
        }

    def get_restaurant_name(self, obj: FissionCampaign):
        return obj.restaurant.name if obj.restaurant is not None else ''


class BaseRetentionSerializer(WritableNestedModelSerializer):
    company = serializers.PrimaryKeyRelatedField(read_only=True)
    restaurant_name = serializers.SerializerMethodField()

    def get_restaurant_name(self, obj: BirthdayGift):
        return obj.restaurant.name if obj.restaurant is not None else ""


class BaseLoyaltyProgramSerializer(BaseRetentionSerializer):
    class Meta:
        model = LoyaltyProgram
        fields = "__all__"


class BaseBirthdayGiftSerializer(BaseRetentionSerializer):
    class Meta:
        model = BirthdayGift
        exclude = ["content_type"]
        extra_kwargs = {"received_by": {"read_only": True}}

    def create(self, validated_data):
        validated_data = self.get_updated_validated_data(validated_data)
        instance = super().create(validated_data)
        return instance

    def update(self, instance, validated_data):
        validated_data = self.get_updated_validated_data(validated_data)
        instance = super().update(instance, validated_data)
        return instance

    def get_updated_validated_data(self, validated_data):
        gift_option_type = validated_data.get("gift_option_type", None)
        if gift_option_type is None:
            return validated_data

        validated_data.update(
            {"content_type": ContentType.objects.get(
                model=gift_option_type.lower())}
        )
        return validated_data


class BaseGiftCardSerializer(BaseRetentionSerializer):
    class Meta:
        model = GiftCard
        fields = "__all__"


class BaseMembershipCardSerializer(BaseRetentionSerializer):
    # usage_time = BaseDurationSerializer()
    dish_names = serializers.SerializerMethodField()

    class Meta:
        model = MembershipCard
        fields = "__all__"

    def get_dish_names(self, obj: MembershipCard):
        return obj.dishes.all().values_list("name", flat=True)

class CommentSerializer(serializers.ModelSerializer):
    parent = serializers.PrimaryKeyRelatedField(allow_null=True, queryset=Comment.objects.all(), required=False)
    replies = serializers.SerializerMethodField()  # Dynamically load replies based on parent
    

    class Meta:
        model = Comment
        fields = ['id', 'user', 'review', 'text', 'parent', 'replies', 'created_at']

    def get_replies(self, obj):
        """
        Filter replies by parent, showing only comments where parent is the current comment.
        """
        # Filter comments that have the current comment (obj) as their parent
        child_comments = Comment.objects.filter(parent=obj)
        return CommentSerializer(child_comments, many=True).data 
    
# class MenuItemSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = MenuItem
#         fields = ['id', 'name', 'description', 'images','original_image', 'category', 'restaurant']
class ReviewSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)
    comments = serializers.SerializerMethodField()
    likes_count = serializers.SerializerMethodField()
    dislikes_count = serializers.SerializerMethodField()
    liked_users = serializers.SerializerMethodField()
    disliked_users = serializers.SerializerMethodField()
    # order_id = serializers.SerializerMethodField()
    # menuItem_details = BaseMenuItemSerializer(source='menuItem', read_only=True)  # For output
    
    # Use PrimaryKeyRelatedField for menuItem
    menuItem = serializers.PrimaryKeyRelatedField(queryset=MenuItem.objects.all(), required=False, allow_null=True)

    class Meta:
        model = Review
        fields = ['id', 'review', 'user', 'menuItem','menuItem_details', 'is_approved', 'likes_count', 'dislikes_count', 'liked_users', 'disliked_users', 'comments', 'rating','created_date', 'modified_date', "restaurant", "is_pinned","order_id"]

    def validate(self, data):
        """
        Custom validation logic, if needed, for other fields.
        No need to validate menuItem since PrimaryKeyRelatedField handles it.
        """
        if not data.get('review'):
            raise serializers.ValidationError("Review text is required.")
        return data

    def create(self, validated_data):
        # Get the current user from the request context
        print("hello bangladesh final")
        user = self.context['request'].user
        menu_item = validated_data.get('menuItem')
        order_id = validated_data.get('order_id')
        restaurant = validated_data.get('restaurant')
        # Create the Review instance directly with user and menuItem ID
        review = Review.objects.create(
            review=validated_data['review'],
            user=user,
            menuItem=menu_item,
            order_id=order_id,
            rating=validated_data['rating'],
            restaurant=restaurant
        )
        return review

    def get_likes_count(self, obj):
        return obj.likes.count()

    def get_dislikes_count(self, obj):
        return obj.dislikes.count()

    def get_liked_users(self, obj):
        return UserSerializer(obj.likes.all(), many=True).data

    def get_disliked_users(self, obj):
        return UserSerializer(obj.dislikes.all(), many=True).data

    def get_comments(self, obj):
        top_level_comments = obj.comments.filter(parent=None)
        return CommentSerializer(top_level_comments, many=True).data


class RatingSerializer(serializers.ModelSerializer):
    review = ReviewSerializer(many=True, read_only=True)
    user = UserSerializer(many=True, read_only=True)

    class Meta:
        model = Rating
        fields = "__all__"


class BaseReviewRatingSerializer(serializers.Serializer):
    review = serializers.CharField(required=True)
    menuItem = serializers.IntegerField(required=False, allow_null=True)  # Use IntegerField for menuItem ID

    def validate_review(self, value):
        """
        Ensure that the review is not empty and has meaningful content.
        """
        if not value.strip():  # Check if the review is an empty string or just spaces
            raise serializers.ValidationError("Review text must be provided and cannot be empty.")
        return value

    def validate(self, data):
        """
        Ensure that the review is provided and validate menuItem if provided.
        """
        # Validate that if a menuItem is provided, it exists in the database
        if 'menuItem' in data and data['menuItem'] is not None:
            try:
                MenuItem.objects.get(id=data['menuItem'])
            except MenuItem.DoesNotExist:
                raise serializers.ValidationError({"menuItem": "Menu item does not exist."})

        return data

    def create(self, validated_data):
        menu_item = None

        # Check if menuItem is provided and fetch it from the database
        if 'menuItem' in validated_data and validated_data['menuItem'] is not None:
            menu_item = MenuItem.objects.get(id=validated_data['menuItem'])  

        # Create the review with the associated menuItem (if any)
        review = Review.objects.create(
            review=validated_data['review'],
            user=self.context['request'].user,
            menuItem=menu_item  # Associate with menu item if provided
        )
        return review



class BaseBogoDetailSerializer(BaseBogoSerializer):
    # from food.api.base.serializers import BaseMenuItemSerializer
    item_details = serializers.SerializerMethodField()

    def get_item_details(self, obj: Bogo):
        from food.api.v2.serializers import MenuItemPreviewSerializer
        return MenuItemPreviewSerializer(obj.items.all(), many=True).data
      
class BaseBxGyDetailSerializer(BaseBxGySerializer):
    # from food.api.base.serializers import BaseMenuItemSerializer
    
    buy_item_details = serializers.SerializerMethodField()
    free_item_details = serializers.SerializerMethodField()

    def get_buy_item_details(self, obj: BxGy):
        from food.api.v2.serializers import MenuItemPreviewSerializer
        from food.models import MenuItem  # adjust import as needed

        buy_item_ids = set()

        for buy_item in obj.buy_items.all():
            if isinstance(buy_item.buy_items, list):
                buy_item_ids.update(buy_item.buy_items)
            elif hasattr(buy_item, "menu_item_id"):
                buy_item_ids.add(buy_item.menu_item_id)

        buy_menu_items = MenuItem.objects.filter(id__in=buy_item_ids)
        return MenuItemPreviewSerializer(buy_menu_items, many=True).data
      
    def get_free_item_details(self, obj: BxGy):
        from food.api.v2.serializers import MenuItemPreviewSerializer
        from food.models import MenuItem  # or wherever your model is

        free_item_ids = set()

        # Traverse each buy item, then its free_items list
        for buy_item in obj.buy_items.all():
            for free_item in buy_item.free_items.all():
                if isinstance(free_item.free_items, list):
                    free_item_ids.update(free_item.free_items)
                elif hasattr(free_item, "menu_item_id"):
                    free_item_ids.add(free_item.menu_item_id)

        # Fetch distinct MenuItems
        free_menu_items = MenuItem.objects.filter(id__in=free_item_ids)
        return MenuItemPreviewSerializer(free_menu_items, many=True).data


class BaseEmailConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = EmailConfiguration
        fields = "__all__"


class BaseEmailSendSerializers(serializers.Serializer):
    Program = serializers.CharField(allow_null=True, allow_blank=True)
    audience = serializers.CharField(allow_null=True, allow_blank=True)
    subject = serializers.CharField(allow_null=True, allow_blank=True)
    html_path = serializers.CharField()
    context = serializers.JSONField(allow_null=True)
    to_emails = serializers.JSONField()
    restaurant = serializers.IntegerField()
    schedule_time = serializers.JSONField(allow_null=True)


class BaseStaffSendEmailSerializer(serializers.Serializer):
    location = serializers.IntegerField()
    restaurant = serializers.IntegerField()
    subject = serializers.CharField()
    to_emails = serializers.JSONField()


class BaseEmailHistorySerializers(serializers.Serializer):
    class Meta:
        model = EmailHistory
        fields = "__all__"


class BaseContactUsDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = ContactUsData
        fields = "__all__"


class BaseDemoDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = DemoData
        fields = "__all__"

class AutoReplyToCommentsSerializer(serializers.ModelSerializer):
    class Meta:
        model = AutoReplyToComments
        fields = [
            "id",
            "restaurant",
            "location",
            "auto_reply_to_good_comments",
            "auto_reply_to_bad_comments",
            "voucher_amount",
        ]

    def validate(self, data):
        # Check if settings exist for the same restaurant and location
        restaurant = data.get("restaurant", None)
        location = data.get("location", None)

        # Skip validation if updating the current instance
        if self.instance:
            if (
                self.instance.restaurant == restaurant
                and self.instance.location == location
            ):
                return data

        if AutoReplyToComments.objects.filter(
            restaurant=restaurant, location=location
        ).exclude(id=self.instance.id if self.instance else None).exists():
            raise serializers.ValidationError("Settings already exist for this restaurant and location.")

        return data