from rest_framework.permissions import IsAuthenticatedOrReadOnly

from remotekitchen.api.base.views import (BaseRemoteKitchenCuisineModelView,
                                          BaseRemoteKitchenRestaurantList, BaseSearchSuggestionApiView, BaseTopSearchKeywordsAPIView, 
                                          BaseChatchefRestaurantList, BaseVisitHistoryView, BaseItemVisitHistoryView, BaseFavoriteStatusView, 
                                          BaseAddressManagementView, BaseDefaultAddressView, BaseCountdownView, BaseGoogleAutocompleteView, 
                                          BaseGLocateMeView, BaseGPlaceDetailsView, BaseGReverseGeocodeView,BaseRemoteKitchenAllCuisinesAPIView)


class RemoteKitchenCuisineModelView(BaseRemoteKitchenCuisineModelView):
    pass


class RemoteKitchenAllCuisinesAPIView(BaseRemoteKitchenAllCuisinesAPIView):
    pass



class RemoteKitchenRestaurantList(BaseRemoteKitchenRestaurantList):
    permission_classes = [IsAuthenticatedOrReadOnly]

class ChatchefRestaurantList(BaseChatchefRestaurantList):
    permission_classes = [IsAuthenticatedOrReadOnly]
class SearchSuggestionAPIView(BaseSearchSuggestionApiView):
    pass 
  
class TopSearchKeywordsAPIView(BaseTopSearchKeywordsAPIView):
    pass


class VisitHistoryView(BaseVisitHistoryView):
    pass
class ItemVisitHistoryView(BaseItemVisitHistoryView):
    pass

class FavoriteStatusView(BaseFavoriteStatusView):
    pass
class AddressManagementView(BaseAddressManagementView):
    pass


class DefaultAddressView(BaseDefaultAddressView):
    pass
class CountdownView(BaseCountdownView):
    pass
class GoogleAutocompleteView(BaseGoogleAutocompleteView):
    pass

class GLocateMeView(BaseGLocateMeView):
    pass
class GReverseGeocodeView(BaseGReverseGeocodeView):
    pass


class GPlaceDetailsView(BaseGPlaceDetailsView):
    pass