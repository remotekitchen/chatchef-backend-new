from rest_framework import serializers

from food.api.base.serializers import (BaseCategorySerializer,
                                       BaseMenuItemSerializer,
                                       BaseMenuItemWithoutModifierSerializer,
                                       BaseMenuSerializer)
from food.models import MenuItem, ModifierGroup
from marketing.models import Rating


# Category serializers
class CategoryListSerializer(BaseCategorySerializer):
    class Meta(BaseCategorySerializer.Meta):
        fields = [
            "id",
            "name",
        ]


# Menu Item serializers
class MenuItemListSerializer(BaseMenuItemWithoutModifierSerializer):
    class Meta(BaseMenuItemSerializer.Meta):
        fields = [
            "id",
            "name",
            "description",
            "base_price",
            "virtual_price",
            "is_available",
            "is_available_today",
            "original_image",
            "like_count",
            "is_current_user_liked",
            "category",
            "has_modifier",
            "images",
            "rating",
            "disabled"
            # "category_names",
        ]

    def get_is_current_user_liked(self, obj: MenuItem):
        request = self.context.get("request")

        if request.user.is_authenticated:
            return Rating.objects.filter(menuItem=obj.id, user=request.user).exists()


class MenuItemPreviewSerializer(BaseMenuItemSerializer):
    has_required_modifier = serializers.SerializerMethodField()

    class Meta(BaseMenuItemSerializer.Meta):
        fields = [
            "id",
            "name",
            "images",
            "original_image",
            "menu_name",
            "menu",
            "has_required_modifier",
            "base_price",
            "virtual_price",
            "modifiergrouporder_set"
        ]

    def get_has_required_modifier(self, obj: MenuItem):
        return obj.modifiergroup_set.filter(
            requirement_status=ModifierGroup.RequirementType.REQUIRED
        ).exists()


# Menu Serializers
class MenuListSerializer(BaseMenuSerializer):
    class Meta(BaseMenuSerializer.Meta):
        fields = ["id", "title", "is_closed","restaurant_name",
            "location_names",
            "opening_hours",]


class MenuDetailSerializer(BaseMenuSerializer):
    class Meta(BaseMenuSerializer.Meta):
        fields = [
            "id",
            "title",
            "description",
            "showing",
            "is_closed",
            "menuitem_set",
            "category_set",
            "slug",
            "opening_hours",
            "categories_list",
            "inflation_percent"
        ]

    menuitem_set = MenuItemListSerializer(many=True)
    category_set = CategoryListSerializer(many=True)
    categories_list = CategoryListSerializer(many=True)


class MenuItemGETSerializer(BaseMenuItemSerializer):
    class Meta(BaseMenuItemSerializer.Meta):
        fields = [
            "id",
            "images",
            "original_image",
            "menu_name",
            "category_names",
            "name",
            "description",
            "base_price",
            "virtual_price",
            "is_alcoholic",
            "disabled",
            "showing",
            "menu",
            "category"
        ]
