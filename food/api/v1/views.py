from rest_framework.permissions import IsAuthenticatedOrReadOnly

from food.api.base.serializers import BaseExcelMenuUploadSerializer
from food.api.base.views import (BaseAllLocationAPIView,
                                 BaseCategoryListCreateAPIView,
                                 BaseCategoryRetrieveUpdateDestroyAPIView,
                                 BaseEmployRegisterGetAPIView,
                                 BaseExcelMenuUploadAPIView,
                                 BaseExcelModifierGroupUploadAPIView,
                                 BaseExportModifiersToExcelAPIView,
                                 BaseListOfRestaurantsForOrderCallAPIview,
                                 BaseLocationAvailibilityApiView,
                                 BaseLocationListCreateAPIView,
                                 BaseLocationRetrieveUpdateDestroyAPIView,
                                 BaseMenuExportToExcelAPIView,
                                 BaseMenuItemAvailibilityApiView,
                                 BaseMenuItemListCreateAPIView,
                                 BaseMenuItemRetrieveUpdateDestroyAPIView,
                                 BaseMenuListCreateAPIView,
                                 BaseMenuPriceInflationApiView,
                                 BaseMenuRetrieveUpdateDestroyAPIView,
                                 BaseModifierGroupListCreateAPIView,
                                 BaseModifierGroupRetrieveUpdateDestroyAPIView,
                                 BaseModifiersAvailabilityAPIView,
                                 BaseModifiersGroupOrderModelView,
                                 BaseModifiersItemOrderModelView,
                                 BaseRestaurantBannerImageAddAPIView,
                                 BaseRestaurantBannerImageRemoveAPIView,
                                 BaseRestaurantListAPIView,
                                 BaseRestaurantListCreateAPIView,
                                 BaseRestaurantOMSUsagesTrackerAPIView,
                                 BaseRestaurantRetrieveUpdateDestroyAPIView,

                                 BaseUpdateMenuOpeningTime, BaseRecommendedDishView, BaseRemoteKitchenCuisineView,
                                BaseLocationDropdownListAPIView,BaseRestaurantDropdownListAPIView,
                                BaseUpdateMenuOpeningTime, BaseRecommendedDishView, BaseRemoteKitchenCuisineView, BaseMenuItemAvailabilityUpdateAPIView)


                                 
from food.api.v1.serializers import (CategorySerializer,
                                     LocationDetailSerializer,
                                     LocationSerializer, MenuDetailSerializer,
                                     MenuItemSerializer, MenuSerializer,
                                     ModifierGETGroupSerializer,
                                     ModifierGroupSerializer,
                                     RestaurantDetailSerializer,
                                     RestaurantSerializer)


class RestaurantListCreateAPIView(BaseRestaurantListCreateAPIView):
    serializer_class = RestaurantSerializer


class RestaurantListAPIView(BaseRestaurantListAPIView):
    permission_classes = [IsAuthenticatedOrReadOnly]


class RestaurantRetrieveUpdateDestroyAPIView(BaseRestaurantRetrieveUpdateDestroyAPIView):
    serializer_class = RestaurantDetailSerializer


class RestaurantBannerImageAddAPIView(BaseRestaurantBannerImageAddAPIView):
    pass


class RestaurantBannerImageRemoveAPIView(BaseRestaurantBannerImageRemoveAPIView):
    pass


class LocationListCreateAPIView(BaseLocationListCreateAPIView):
    serializer_class = LocationSerializer


class LocationRetrieveUpdateDestroyAPIView(BaseLocationRetrieveUpdateDestroyAPIView):
    serializer_class = LocationDetailSerializer


class LocationAvailibilityApiView(BaseLocationAvailibilityApiView):
    pass


class AllLocationAPIView(BaseAllLocationAPIView):
    pass


class MenuListCreateAPIView(BaseMenuListCreateAPIView):
    serializer_class = MenuSerializer


class MenuRetrieveUpdateDestroyAPIView(BaseMenuRetrieveUpdateDestroyAPIView):
    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = MenuDetailSerializer


class CategoryListCreateAPIView(BaseCategoryListCreateAPIView):
    serializer_class = CategorySerializer


class CategoryRetrieveUpdateDestroyAPIView(BaseCategoryRetrieveUpdateDestroyAPIView):
    serializer_class = CategorySerializer


class MenuItemListCreateAPIView(BaseMenuItemListCreateAPIView):
    serializer_class = MenuItemSerializer


class MenuItemRetrieveUpdateDestroyAPIView(BaseMenuItemRetrieveUpdateDestroyAPIView):
    serializer_class = MenuItemSerializer


class MenuItemAvailibityApiView(BaseMenuItemAvailibilityApiView):
    pass


class ModifierGroupListCreateAPIView(BaseModifierGroupListCreateAPIView):
    serializer_class = ModifierGroupSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]


class ModifierGroupRetrieveUpdateDestroyAPIView(BaseModifierGroupRetrieveUpdateDestroyAPIView):
    serializer_class = ModifierGroupSerializer

    def get_serializer_class(self):
        if self.request.method == "PATCH":
            return ModifierGroupSerializer
        else:
            return ModifierGETGroupSerializer


class ModifiersGroupOrderModelView(BaseModifiersGroupOrderModelView):
    pass


class ModifiersItemOrderModelView(BaseModifiersItemOrderModelView):
    pass


class ModifiersAvailabilityAPIView(BaseModifiersAvailabilityAPIView):
    pass


class ExcelMenuUploadAPIView(BaseExcelMenuUploadAPIView):
    serializer_class = BaseExcelMenuUploadSerializer


class UpdateMenuOpeningTime(BaseUpdateMenuOpeningTime):
    pass


class MenuPriceInflationApiView(BaseMenuPriceInflationApiView):
    pass


class ExcelModifierGroupUploadAPIView(BaseExcelModifierGroupUploadAPIView):
    pass


class MenuExportToExcelAPIView(BaseMenuExportToExcelAPIView):
    pass


class ExportModifiersToExcelAPIView(BaseExportModifiersToExcelAPIView):
    pass


class RestaurantOMSUsagesTrackerAPIView(BaseRestaurantOMSUsagesTrackerAPIView):
    pass


class EmployRegisterGetAPIView(BaseEmployRegisterGetAPIView):
    pass


class ListOfRestaurantsForOrderCallAPIview(BaseListOfRestaurantsForOrderCallAPIview):
    pass

class RecommendedDishView(BaseRecommendedDishView):
    pass


class RemoteKitchenCuisineView(BaseRemoteKitchenCuisineView):
    pass



class LocationDropdownListAPIView(BaseLocationDropdownListAPIView):
    pass

class RestaurantDropdownListAPIView(BaseRestaurantDropdownListAPIView):
    pass

class MenuItemAvailabilityUpdateAPIView(BaseMenuItemAvailabilityUpdateAPIView):
    pass

