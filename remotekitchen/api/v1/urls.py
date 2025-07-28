from django.urls import include, path
from rest_framework.routers import DefaultRouter
from remotekitchen.api.base.views import BaseSaveFavoriteView, BaseRemoveFavoriteView, BaseFavoriteListView , BaseRestaurantFavoriteView

from remotekitchen.api.v1.views import (RemoteKitchenCuisineModelView,
                                        RemoteKitchenRestaurantList, BaseSearchSuggestionApiView,RemoteKitchenAllCuisinesAPIView, BaseTopSearchKeywordsAPIView, ChatchefRestaurantList, VisitHistoryView,ItemVisitHistoryView, FavoriteStatusView, DefaultAddressView, AddressManagementView,CountdownView, GoogleAutocompleteView, GLocateMeView, GReverseGeocodeView, GPlaceDetailsView)

router = DefaultRouter()
router.register("cuisines", RemoteKitchenCuisineModelView, basename="cuisines")

urlpatterns = [
    path("", include(router.urls)),
    path("restaurant/lists", RemoteKitchenRestaurantList.as_view()),
    path("restaurant/lists/chatchef", ChatchefRestaurantList.as_view()),
    path('search-suggestions/', BaseSearchSuggestionApiView.as_view(), name='search-suggestions'),
    path('top-search-keywords/', BaseTopSearchKeywordsAPIView.as_view(), name='top-search-keywords'),
    path('restaurant/save-favorites', BaseSaveFavoriteView.as_view(), name='save-favorite'),
    path('restaurant/remove-favorites', BaseRemoveFavoriteView.as_view(), name='remove-favorite'),
    path('restaurant/favorites', BaseFavoriteListView.as_view(), name='favorite'),
    path('restaurant/favorite-status/<int:item_id>/', FavoriteStatusView.as_view(), name='favorite-status'),
    path("cuisines-list/", RemoteKitchenAllCuisinesAPIView.as_view()),
    

    path('restaurant/visit-history', VisitHistoryView.as_view(), name='visit-history'),
    path('restaurant/item-visit-history', ItemVisitHistoryView.as_view(), name='item-visit-history'),

     path("restaurant/addresses/", AddressManagementView.as_view(), name="address-list"),  #
    path("restaurant/addresses/<int:pk>/", AddressManagementView.as_view(), name="address-detail"),  
    path("restaurant/addresses/<int:pk>/default/", AddressManagementView.as_view(), name="address-change-default"),  
    path("restaurant/addresses/default/", DefaultAddressView.as_view(), name="address-default"),  

    path('restaurant/res-favorite/', BaseRestaurantFavoriteView.as_view(), name='favorite-list'),  
    path('restaurant/res-favorite/<int:pk>/', BaseRestaurantFavoriteView.as_view(), name='favorite-detail'), 
    path('restaurant/countdown/', CountdownView.as_view(), name='countdown'), 
    # google maps
    path('autocomplete/', GoogleAutocompleteView.as_view(), name='google-autocomplete'),
    path('locate-me/', GLocateMeView.as_view(), name='locate-me'),
    path('reverse-geocode/', GReverseGeocodeView.as_view(), name='reverse-geocode'),
    path('place-details/', GPlaceDetailsView.as_view(), name='place-details'),
]
