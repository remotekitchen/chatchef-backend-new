from hotel.api.base.views import (
    # Public User Views
    BaseHotelViewSet,
    BaseRoomTypeViewSet,
    BaseBookingViewSet,
    BaseBookingPayViewSet,
    BaseSimilarHotelViewSet,
    BaseSearchSuggestionView,
    BaseSaveSearchHistoryView,
    BaseSavedHotelViewSet,
    BaseRecentSearchesView,

    # Hotel Owner (OMS) Views
    BaseHotelOwnerViewSet,
    BaseRoomTypeOwnerViewSet,
    BaseAvailabilityOwnerViewSet,
    BaseBookingOwnerViewSet,
    BasePaymentOwnerViewSet,
    BaseHotelImageViewSet,
    BaseRoomImageViewSet,
    BaseHotelReviewViewSet,
    BaseHotelTagAssignmentViewSet,
     BaseHotelPolicyOwnerViewSet,
    BaseDashboardViewSet,
    BaseHourlyAvailabilityOwnerViewSet,

    BaseHotelOwnerUserViewSet
)

# -------------------------------
# Public API Views
# -------------------------------

class HotelViewSet(BaseHotelViewSet):
    """Public: List and retrieve hotels."""
    pass

class RoomTypeViewSet(BaseRoomTypeViewSet):
    """Public: View room types of a hotel."""
    pass

class BookingViewSet(BaseBookingViewSet):
    """Public: Booking-related operations for users."""
    pass

class BookingPayViewSet(BaseBookingPayViewSet):
    """Public: Booking payment handler."""
    pass

class SimilarHotelViewSet(BaseSimilarHotelViewSet):
    """Public: Retrieve similar hotels for a given hotel."""
    pass

class SearchSuggestionView(BaseSearchSuggestionView):
    """Public: Auto-suggest for hotel search."""
    pass

class SaveSearchHistoryView(BaseSaveSearchHistoryView):
    """Public: Save a user's hotel search history."""
    pass

class SavedHotelViewSet(BaseSavedHotelViewSet):
    """Public: Manage user's saved hotels."""
    pass

class RecentSearchesView(BaseRecentSearchesView):
    """Public: View user's recent hotel search history."""
    pass

# -------------------------------
# Owner (OMS) API Views
# -------------------------------

class HotelOwnerViewSet(BaseHotelOwnerViewSet):
    """Owner: Manage own hotels."""
    pass

class RoomTypeOwnerViewSet(BaseRoomTypeOwnerViewSet):
    """Owner: Manage room types for own hotels."""
    pass

class AvailabilityOwnerViewSet(BaseAvailabilityOwnerViewSet):
    """Owner: Manage room availability."""
    pass

class HourlyAvailabilityOwnerViewSet(BaseHourlyAvailabilityOwnerViewSet):
    """Owner: Manage room availability."""
    pass

class BookingOwnerViewSet( BaseBookingOwnerViewSet):
    """Owner: View and manage bookings."""
    pass

class PaymentOwnerViewSet(BasePaymentOwnerViewSet):
    """Owner: View and manage payments."""
    pass

class HotelImageViewSet(BaseHotelImageViewSet):
    """Owner: Upload and manage hotel images."""
    pass

class RoomImageViewSet(BaseRoomImageViewSet):
    """Owner: Upload and manage room images."""
    pass

class HotelReviewViewSet(BaseHotelReviewViewSet):
    """Owner: View reviews for their hotels."""
    pass

class HotelTagAssignmentViewSet(BaseHotelTagAssignmentViewSet):
    """Owner: Assign tags to hotels (e.g. featured, deal)."""
    pass

class DashboardViewSet(BaseDashboardViewSet):
    """Owner: Hotel dashboard with analytics and summary."""
    pass

class HotelPolicyOwnerViewSet(BaseHotelPolicyOwnerViewSet):
    """Owner: Hotel dashboard with analytics and summary."""
    pass


# hotel admin

class HotelOwnerUserViewSet(BaseHotelOwnerUserViewSet):
    """Owner: Hotel dashboard with analytics and summary."""
    pass
