from django.urls import path, include
from rest_framework.routers import DefaultRouter
from hotel.api.v1.views  import (HotelViewSet,RoomTypeViewSet,BookingViewSet,SimilarHotelViewSet,BookingPayViewSet,HotelOwnerViewSet,
                    RoomTypeOwnerViewSet,AvailabilityOwnerViewSet,BookingOwnerViewSet,PaymentOwnerViewSet,
                    HotelImageViewSet,RoomImageViewSet,HotelReviewViewSet,SimilarHotelViewSet, DashboardViewSet, SearchSuggestionView,
                    SaveSearchHistoryView, HotelTagAssignmentViewSet,SavedHotelViewSet,RecentSearchesView,
                    HotelPolicyOwnerViewSet,HourlyAvailabilityOwnerViewSet,HotelOwnerUserViewSet)

router = DefaultRouter()
router.register(r'hotels', HotelViewSet, basename='hotel')
router.register(r'room-types', RoomTypeViewSet, basename='room-type')
router.register(r'bookings', BookingViewSet, basename='booking')
router.register(r'booking-pay', BookingPayViewSet, basename='booking-pay')
router.register(r'similar-hotels', SimilarHotelViewSet, basename='similar-hotels')
router.register(r'saved-hotels', SavedHotelViewSet, basename='saved-hotels')



# OMS
router.register(r'manage-hotels', HotelOwnerViewSet, basename='manage-hotels')
router.register(r"oms/room-types", RoomTypeOwnerViewSet, basename="oms-room-type")
router.register(r"oms/availability", AvailabilityOwnerViewSet, basename="oms-availability")
router.register(r'oms/hourly-availability',HourlyAvailabilityOwnerViewSet, basename='hourly-availability')
router.register(r"oms/bookings", BookingOwnerViewSet, basename="oms-bookings")
router.register(r"oms/payments", PaymentOwnerViewSet, basename="oms-payments")
router.register(r"oms/hotel-images", HotelImageViewSet, basename="oms-hotel-images")
router.register(r"oms/room-images", RoomImageViewSet, basename="oms-room-images")
router.register(r"reviews", HotelReviewViewSet, basename="hotel-reviews")
router.register(r"oms/dashboard", DashboardViewSet, basename="dashboard")
router.register(r'oms/assign-tags', HotelTagAssignmentViewSet, basename='assign-tags')
router.register(r'oms/hotel-policies', HotelPolicyOwnerViewSet, basename='hotel-policy')

router.register(
    r'admin/hotel-owners',
    HotelOwnerUserViewSet,
    basename='hotel-owner'
)
urlpatterns = [
    path('', include(router.urls)),
    path('search-suggestions/', SearchSuggestionView.as_view(), name='search-suggestions'),
    path('save-search-history/', SaveSearchHistoryView.as_view(), name='save-search-history'),
    path('recent-searches/', RecentSearchesView.as_view(), name='recent-searches'),
]