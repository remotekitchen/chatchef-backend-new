from food.api.base.serializers import (BaseCategoryDetailSerializer,
                                       BaseCategorySerializer,
                                       BaseExcelMenuUploadSerializer,
                                       BaseLocationDetailSerializer,
                                       BaseLocationSerializer,
                                       BaseMenuDetailSerializer,
                                       BaseMenuItemSerializer,
                                       BaseMenuSerializer,
                                       BaseModifierGroupOrderGETSerializer,
                                       BaseModifierGroupSerializer,
                                       BaseRestaurantDetailSerializer,
                                       BaseRestaurantSerializer)


class RestaurantSerializer(BaseRestaurantSerializer):
    pass


class LocationSerializer(BaseLocationSerializer):
    pass


class MenuSerializer(BaseMenuSerializer):
    pass


class CategorySerializer(BaseCategorySerializer):
    pass


class MenuItemSerializer(BaseMenuItemSerializer):
    pass


class ModifierGroupSerializer(BaseModifierGroupSerializer):
    # modifier_group_order_set = BaseModifierGroupOrderGETSerializer()
    pass


class ModifierGETGroupSerializer(BaseModifierGroupSerializer):
    modifiergrouporder_set = BaseModifierGroupOrderGETSerializer(many=True)


class MenuDetailSerializer(BaseMenuDetailSerializer):
    pass


class CategoryDetailSerializer(BaseCategoryDetailSerializer):
    pass


class ExcelMenuUploadSerializer(BaseExcelMenuUploadSerializer):
    pass


class RestaurantDetailSerializer(BaseRestaurantDetailSerializer):
    pass


class LocationDetailSerializer(BaseLocationDetailSerializer):
    pass
