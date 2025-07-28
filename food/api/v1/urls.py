from django.urls import include, path
from rest_framework.routers import DefaultRouter

from food.api.v1.views import (AllLocationAPIView, CategoryListCreateAPIView,
                               CategoryRetrieveUpdateDestroyAPIView,
                               EmployRegisterGetAPIView,
                               ExcelMenuUploadAPIView,
                               ExcelModifierGroupUploadAPIView,
                               ExportModifiersToExcelAPIView,
                               ListOfRestaurantsForOrderCallAPIview,
                               LocationAvailibilityApiView,
                               LocationListCreateAPIView,
                               LocationRetrieveUpdateDestroyAPIView,
                               MenuExportToExcelAPIView,
                               MenuItemAvailibityApiView,
                               MenuItemListCreateAPIView,
                               MenuItemRetrieveUpdateDestroyAPIView,
                               MenuListCreateAPIView,
                               MenuPriceInflationApiView,
                               MenuRetrieveUpdateDestroyAPIView,
                               ModifierGroupListCreateAPIView,
                               ModifierGroupRetrieveUpdateDestroyAPIView,
                               ModifiersAvailabilityAPIView,
                               ModifiersGroupOrderModelView,
                               ModifiersItemOrderModelView,
                               RestaurantBannerImageAddAPIView,
                               RestaurantBannerImageRemoveAPIView,
                               RestaurantListAPIView,
                               RestaurantListCreateAPIView,
                               RestaurantOMSUsagesTrackerAPIView,
                               RestaurantRetrieveUpdateDestroyAPIView,

                               UpdateMenuOpeningTime, RecommendedDishView, 
                               RemoteKitchenCuisineView,RestaurantDropdownListAPIView,
                               LocationDropdownListAPIView,UpdateMenuOpeningTime, RecommendedDishView, RemoteKitchenCuisineView, MenuItemAvailabilityUpdateAPIView
                               )

                               


router = DefaultRouter()
router.register('modifiers-groups-order', ModifiersGroupOrderModelView,
                basename="modifiers-group-ordering")
router.register('modifiers-items-order', ModifiersItemOrderModelView,
                basename="modifiers-items-ordering")


# superadmin access
# router.register('superadmin/restaurants', SuperadminRestaurantViewSet, basename='superadmin-restaurants')

urlpatterns = [
    path('', include(router.urls)),
    path('restaurant/', RestaurantListCreateAPIView.as_view(), name='restaurant'),
    path('restaurants/', RestaurantListAPIView.as_view(), name='restaurants'),
    path('restaurant/item/', RestaurantRetrieveUpdateDestroyAPIView.as_view(),
         name='restaurant-item'),
    path('restaurant/banner-images/add/<str:pk>/', RestaurantBannerImageAddAPIView.as_view(),
         name='restaurant-add-banner-image'),
    path('restaurant/banner-images/remove/<str:pk>/', RestaurantBannerImageRemoveAPIView.as_view(),
         name='restaurant-remove-banner-image'),
    path('location/', LocationListCreateAPIView.as_view(), name='location'),
    path('location/path/', LocationRetrieveUpdateDestroyAPIView.as_view(),
         name='location-item-1'),
    path('location/item/', LocationRetrieveUpdateDestroyAPIView.as_view(),
         name='location-item'),
    path('location/all/', AllLocationAPIView.as_view(),
         name='location-all'),
    path('location/availability/', LocationAvailibilityApiView.as_view(),
         name='location-availability'),
    path('menu/', MenuListCreateAPIView.as_view(), name='menu'),
    path('menu/item/', MenuRetrieveUpdateDestroyAPIView.as_view(),
         name='individual-menu'),
    path('category/', CategoryListCreateAPIView.as_view(), name='category'),
    path('category/item/', CategoryRetrieveUpdateDestroyAPIView.as_view(),
         name='category-item'),
    path('menu-item/', MenuItemListCreateAPIView.as_view(), name='menu-item'),
    path('menu-item/item/', MenuItemRetrieveUpdateDestroyAPIView.as_view(),
         name='individual-menu-item'),
    path('menu-item/availability/', MenuItemAvailibityApiView.as_view(),
         name='Avilibity-menu-item'),
    path('modifier-group/', ModifierGroupListCreateAPIView.as_view(),
         name='modifier-group'),
    path('modifier-group/item/', ModifierGroupRetrieveUpdateDestroyAPIView.as_view(),
         name='modifier-group-item'),
    path('modifier-group-availability/<str:pk>/', ModifiersAvailabilityAPIView.as_view(),
         name='modifier-group-availability'),
    path('menu/excel/', ExcelMenuUploadAPIView.as_view(), name='menu-excel'),
    path('modifier/excel/', ExcelModifierGroupUploadAPIView.as_view(),
         name='menu-excel'),
    path('menu-opening-time-update/<str:pk>/', UpdateMenuOpeningTime.as_view()),
    path('menu-inflation/<str:pk>/', MenuPriceInflationApiView.as_view()),
    path('menu-export/<str:pk>/', MenuExportToExcelAPIView.as_view()),
    path('modifier-export/', ExportModifiersToExcelAPIView.as_view()),
    path('oms-tracker/', RestaurantOMSUsagesTrackerAPIView.as_view()),
    path('get-employ-register-code/<str:pk>/',
         EmployRegisterGetAPIView.as_view()),
    path('get-employ-register-code/',
         EmployRegisterGetAPIView.as_view()),
    path("List-Of-Restaurants-For-Order-Call",
         ListOfRestaurantsForOrderCallAPIview.as_view()),
    path("recommended-dish",
         RecommendedDishView.as_view()),
    path('cuisine/', RemoteKitchenCuisineView.as_view(), name='cuisine'),
    path('<int:pk>/availability/', MenuItemAvailabilityUpdateAPIView.as_view(), name='cuisine'),



# new add
    path('restaurant-dropdown/', RestaurantDropdownListAPIView.as_view(), name='restaurant-search'),
    path('location-dropdown/', LocationDropdownListAPIView.as_view(), name='location-search'),

]
