from hotel.api.base.serializers import (
    HotelBaseSerializer,
    RoomTypeBaseSerializer,
    RoomAvailabilitySerializer,
    GuestSerializer,
    BookingDetailSerializer,
    BookingPaymentSerializer,
    BookingSerializer,
    HotelManageSerializer,
    HotelImageSerializer,
    RoomImageSerializer,
    HotelReviewSerializer,
    SimilarHotelSerializer,
    BookingCouponSerializer,
    SavedHotelSerializer,
    HotelViewHistorySerializer,
    HotelTagAssignmentSerializer,
    NearbyPlaceSerializer,
    HotelSearchHistorySerializer,
    SearchSuggestionSerializer,

    

    

)

# -------------------------------
# Base Inherited Serializers
# -------------------------------

class HotelSerializer(HotelBaseSerializer):
    pass

class RoomTypeSerializer(RoomTypeBaseSerializer):
    pass

class RoomAvailabilityDataSerializer(RoomAvailabilitySerializer):
    pass

class GuestDataSerializer(GuestSerializer):
    pass

class BookingSerializerV1(BookingSerializer):
    pass

class BookingDetailSerializerV1(BookingDetailSerializer):
    pass

class BookingPaymentSerializerV1(BookingPaymentSerializer):
    pass

class HotelManageSerializerV1(HotelManageSerializer):
    pass

class HotelImageSerializerV1(HotelImageSerializer):
    pass

class RoomImageSerializerV1(RoomImageSerializer):
    pass

class HotelReviewSerializerV1(HotelReviewSerializer):
    pass

# -------------------------------
# Additional Custom Serializers
# -------------------------------

class SavedHotelSerializerV1(SavedHotelSerializer):
    pass

class HotelViewHistorySerializerV1(HotelViewHistorySerializer):
    pass

class HotelTagAssignmentSerializerV1(HotelTagAssignmentSerializer):
    pass

class NearbyPlaceSerializerV1(NearbyPlaceSerializer):
    pass

class HotelSearchHistorySerializerV1(HotelSearchHistorySerializer):
    pass

class SimilarHotelSerializerV1(SimilarHotelSerializer):
    pass

class BookingCouponSerializerV1(BookingCouponSerializer):
    pass

class SearchSuggestionSerializerV1(SearchSuggestionSerializer):
    pass

