from rest_framework import serializers
from hotel.models import (Hotel, RoomType, RoomAvailability,Guest,Booking,BookingPayment,
                          HotelImage,RoomImage, HotelPolicy,HotelReview,HotelTag,HotelTagAssignment,
                          HotelViewHistory,Coupon,NearbyPlace,SavedHotel,SearchSuggestion,BookingCoupon,
                          HotelSearchHistory,RoomHourlyAvailability,BookingRoomItem)
from hotel.utils.location import get_lat_lon_from_city
from hotel.utils.helpers import get_date_range, haversine
from rest_framework.pagination import PageNumberPagination
import logging
from decimal import Decimal
from django.core.cache import cache
from hungrytiger.settings.defaults import mapbox_api_key
from collections import defaultdict
from django.db.models import Max
import hashlib
import json
from datetime import datetime
from decimal import Decimal
from collections import defaultdict


logger = logging.getLogger(__name__)

class HotelImageSerializer(serializers.ModelSerializer):
    class Meta:
        model = HotelImage
        fields = "__all__"
        read_only_fields = ["hotel", "created_at", "updated_at"]



class RoomImageSerializer(serializers.ModelSerializer):
    class Meta:
        model = RoomImage
        fields = "__all__"
        read_only_fields = ["room_type", "created_at", "updated_at"]


class HotelTagSerializer(serializers.ModelSerializer):
    class Meta:
        model = HotelTag
        fields = ['id', 'name', 'icon', 'is_flash_deal', 'display_order']


class HotelTagAssignmentSerializer(serializers.ModelSerializer):
    tag = HotelTagSerializer(read_only=True)

    class Meta:
        model = HotelTagAssignment
        fields = ['id', 'hotel', 'tag']

    def validate_hotel(self, hotel):
        # Ensure the hotel belongs to the current authenticated user (Hotel Owner)
        if hotel.owner != self.context['request'].user:
            raise serializers.ValidationError("You do not own this hotel.")
        return hotel


class NearbyPlaceSerializer(serializers.ModelSerializer):
    distance_m = serializers.SerializerMethodField()
    distance_min = serializers.SerializerMethodField()

    class Meta:
        model = NearbyPlace
        fields = ['name', 'category', 'distance_m', 'distance_min']

    def get_distance_m(self, obj):
        hotel_lat = self.context.get('hotel_lat')
        hotel_lon = self.context.get('hotel_lon')
        if hotel_lat is None or hotel_lon is None:
            return None
        km = haversine(float(hotel_lat), float(hotel_lon),float(obj.latitude), float(obj.longitude))
        return int(km * 1000)  # convert km to meters

    def get_distance_min(self, obj):
        dist_m = self.get_distance_m(obj)
        if dist_m is None:
            return None
        # Approximate walking speed: 80 meters per minute
        return max(1, int(dist_m / 80))

class HotelPolicySerializer(serializers.ModelSerializer):
    class Meta:
        model = HotelPolicy
        fields = "__all__"
        # read_only_fields = ["hotel"]

class HotelReviewSerializer(serializers.ModelSerializer):
    hotel_name = serializers.CharField(source="hotel.name", read_only=True)
    class Meta:
        model = HotelReview
        fields = [
            "id", "rating", "comment", "staff", "cleanliness", "location",
            "created_at", "hotel", "hotel_name","booking"
        ]
        read_only_fields = ["id", "created_at", "hotel"]

class RoomAvailabilitySerializer(serializers.ModelSerializer):
    class Meta:
        model = RoomAvailability
        fields = [
            "date", "room_type_name", "available_rooms", "price_override",
            "effective_price", "is_special_offer", "special_offer_name", "min_nights_stay"
        ]

    room_type_name = serializers.SerializerMethodField()
    effective_price = serializers.SerializerMethodField()

    def get_room_type_name(self, obj):
        return getattr(obj.room_type, "name", "")

    def get_effective_price(self, obj):
        return obj.get_effective_price()


class RoomTypeBaseSerializer(serializers.ModelSerializer):
    current_price = serializers.SerializerMethodField()
    room_features = serializers.SerializerMethodField()
    availability_summary = serializers.SerializerMethodField()
    availability = serializers.SerializerMethodField()
    room_images = RoomImageSerializer(many=True, read_only=True)

    total_nights = serializers.SerializerMethodField()
    total_price = serializers.SerializerMethodField()
    rooms_left = serializers.SerializerMethodField()

    total_hours = serializers.SerializerMethodField()
    total_hourly_price = serializers.SerializerMethodField()


    class Meta:
        model = RoomType
        fields = [
               "id", "hotel", "name", "description", "max_adults", "max_children",
            "max_occupancy", "number_of_beds", "bed_type", "room_size",
            "price_per_night", "discount_price", "current_price",
            "is_hourly", "hourly_price", "total_rooms", "room_code",
            "extra_bed_fee", "early_checkin_fee", "late_checkout_fee", "view_type",
            "room_features", "min_age_checkin",
            "availability_summary", "availability", "room_images", 
            "total_nights","total_hours", "total_price","total_hourly_price", "rooms_left"
        ]


    def get_room_features(self, obj):
        # List of room-related amenities with 'has_' prefix
        feature_fields = [
            "has_air_conditioning", "has_private_bathroom", "has_balcony", "has_tv",
            "has_refrigerator", "has_toiletries", "has_towels", "has_slippers", "has_clothes_rack",
            "has_safe", "has_desk", "has_minibar", "has_coffee_maker", "has_bathtub",
            "has_hairdryer", "has_iron", "has_seating_area"
        ]

        features = []
        # Loop over the feature fields and append to the list if the feature is True
        for field in feature_fields:
            if getattr(obj, field, False):  # Only add if the feature is True
                features.append(field[4:])  # Remove the 'has_' prefix

        # Add additional non-boolean fields like 'is_refundable' if True
        if obj.is_refundable:
            features.append("refundable")
        if obj.smoking_allowed:
            features.append("smoking_allowed")
        
        return features
    
    def get_current_price(self, obj):
        return obj.get_current_price()
    
    def get_total_nights(self, obj):
        checkin = self.context.get("checkin")
        checkout = self.context.get("checkout")

        if not checkin or not checkout:
            return 0

        if isinstance(checkin, str):
            checkin_date = datetime.strptime(checkin, "%Y-%m-%d").date()
        else:
            checkin_date = checkin  # already a date object

        if isinstance(checkout, str):
            checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()
        else:
            checkout_date = checkout  # already a date object

        return (checkout_date - checkin_date).days


    def get_rooms_left(self, obj):
        checkin = self.context.get("checkin")
        checkout = self.context.get("checkout")
        if not checkin or not checkout:
            return None
        date_range = get_date_range(checkin, checkout)
        availability_counts = obj.availability.filter(date__in=date_range).values_list("available_rooms", flat=True)
        return min(availability_counts) if availability_counts else 0

    def get_total_price(self, obj):
        booking_type = self.context.get("booking_type", "daily")
        number_of_rooms = self.context.get("number_of_rooms", 1)

        if booking_type == "daily":
            total_nights = self.get_total_nights(obj)
            price_per_night = Decimal(obj.get_current_price() or 0)
            tax_rate = Decimal(getattr(obj.hotel, 'tax_rate', 0) or 0)
            tax_amount = (price_per_night * tax_rate) / Decimal(100)
            total_price = (price_per_night + tax_amount) * total_nights * Decimal(number_of_rooms)

            return round(total_price, 2)

        elif booking_type == "hourly":
            checkin_time_str = self.context.get("checkin_time")
            checkout_time_str = self.context.get("checkout_time")
            if not checkin_time_str or not checkout_time_str:
                return None

            try:
                checkin_time = datetime.strptime(checkin_time_str, "%H:%M")
                checkout_time = datetime.strptime(checkout_time_str, "%H:%M")
            except ValueError:
                return None

            total_hours = (checkout_time - checkin_time).seconds / 3600
            hourly_price = Decimal(obj.hourly_price or obj.get_current_price() or 0)
            tax_rate = Decimal(getattr(obj.hotel, 'tax_rate', 0) or 0)
            tax_amount = (hourly_price * tax_rate) / Decimal(100)
            total_price = (hourly_price + tax_amount) * Decimal(str(total_hours)) * number_of_rooms
            return round(total_price, 2)

        else:
            return None
        
    def get_total_hours(self, obj):
        booking_type = self.context.get("booking_type", "daily")
        if booking_type != "hourly":
            return None

        checkin_time_str = self.context.get("checkin_time")
        checkout_time_str = self.context.get("checkout_time")
        if not checkin_time_str or not checkout_time_str:
            return None

        try:
            checkin_time = datetime.strptime(checkin_time_str, "%H:%M")
            checkout_time = datetime.strptime(checkout_time_str, "%H:%M")
        except ValueError:
            return None

        total_hours = (checkout_time - checkin_time).seconds // 3600
        return total_hours

    def get_total_hourly_price(self, obj):
        booking_type = self.context.get("booking_type", "daily")
        number_of_rooms = self.context.get("number_of_rooms", 1)

        if booking_type != "hourly":
            return None

        total_hours = self.get_total_hours(obj)
        if total_hours is None:
            return None

        hourly_price = Decimal(obj.hourly_price or 0)
        tax_rate = Decimal(getattr(obj.hotel, 'tax_rate', 0) or 0)
        tax_amount = (hourly_price * tax_rate) / Decimal(100)

        total_price = (hourly_price + tax_amount) * total_hours * number_of_rooms
        return round(total_price, 2)

    def get_availability_summary(self, obj):
        checkin = self.context.get("checkin")
        checkout = self.context.get("checkout")
        booking_type = self.context.get("booking_type", "daily")

        if not checkin:
            return {
                "available_nights": 0,
                "min_price": None,
                "max_price": None,
                "fully_available": False,
            }

        cache_key = f"availability_summary:{obj.id}:{checkin}:{checkout}:{booking_type}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        if booking_type == "hourly":
            date = checkin
            checkin_time = self.context.get("checkin_time")
            checkout_time = self.context.get("checkout_time")
            rooms_requested = self.context.get("number_of_rooms", 1)

            if not checkin_time or not checkout_time:
                return {
                    "available_hour_slots": 0,
                    "fully_available": False,
                }

            try:
                checkin_time_dt = datetime.strptime(checkin_time, "%H:%M").time()
                checkout_time_dt = datetime.strptime(checkout_time, "%H:%M").time()
            except ValueError:
                return {
                    "available_hour_slots": 0,
                    "fully_available": False,
                }

            overlapping_slots = RoomHourlyAvailability.objects.filter(
                room_type=obj,
                date=checkin,
                start_time__lt=checkout_time_dt,
                end_time__gt=checkin_time_dt,
                available_rooms__gte=rooms_requested
            )

            summary = {
                "available_hour_slots": overlapping_slots.count(),
                "fully_available": overlapping_slots.exists()
            }

            cache.set(cache_key, summary, timeout=300)
            return summary

        else:
            date_range = get_date_range(checkin, checkout)
            avail_qs = obj.availability.filter(date__in=date_range, available_rooms__gte=1)

            available_dates = set(avail_qs.values_list("date", flat=True))
            fully_available = all(day in available_dates for day in date_range)

            prices = [a.get_effective_price() for a in avail_qs]
            summary = {
                "available_nights": len(available_dates),
                "min_price": float(min(prices)) if prices else None,
                "max_price": float(max(prices)) if prices else None,
                "fully_available": fully_available
            }

            cache.set(cache_key, summary, timeout=300)
            return summary

    def get_availability(self, obj):
        checkin = self.context.get("checkin")
        checkout = self.context.get("checkout")
        booking_type = self.context.get("booking_type", "daily")

        if not checkin:
            return []

        if booking_type == "hourly":
            checkin_time = self.context.get("checkin_time")
            checkout_time = self.context.get("checkout_time")

            try:
                checkin_time_dt = datetime.strptime(checkin_time, "%H:%M").time()
                checkout_time_dt = datetime.strptime(checkout_time, "%H:%M").time()
            except (TypeError, ValueError):
                return []

            avail_qs = RoomHourlyAvailability.objects.filter(
                room_type=obj,
                date=checkin,
                start_time__lt=checkout_time_dt,
                end_time__gt=checkin_time_dt
            ).order_by("start_time")

            return [
                {
                    "date": a.date.strftime("%Y-%m-%d"),
                    "start_time": a.start_time.strftime("%H:%M"),
                    "end_time": a.end_time.strftime("%H:%M"),
                    "available_rooms": a.available_rooms
                }
                for a in avail_qs
            ]

        else:
            if not checkout:
                return []

            date_range = get_date_range(checkin, checkout)
            avail_qs = obj.availability.filter(date__in=date_range).order_by("date")

            return [
                {"date": a.date.strftime("%Y-%m-%d"), "available_rooms": a.available_rooms}
                for a in avail_qs
            ]



class HotelBaseSerializer(serializers.ModelSerializer):
    room_types = serializers.SerializerMethodField()
    primary_image = serializers.SerializerMethodField()
    amenities = serializers.SerializerMethodField()
    distance_km = serializers.SerializerMethodField()
    nearby_places = serializers.SerializerMethodField()
    
    is_saved = serializers.SerializerMethodField()
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()
    images = HotelImageSerializer(many=True, read_only=True)
    policy = HotelPolicySerializer(read_only=True)
    tags = HotelTagAssignmentSerializer(many=True, read_only=True, source='tag_assignments')

    reviews = HotelReviewSerializer(many=True, read_only=True)

    class Meta:
        model = Hotel
        fields = [
            "id", "name", "city", "base_price_per_night","review_score", "review_count",
            "star_rating", "primary_image", "room_types", "amenities", "distance_km","latitude", 
            "longitude","images", "policy" ,"reviews","nearby_places","tags", "is_saved"
        ]
    read_only_fields = ['owner']

    # This method checks if the current user saved this hotel
    def get_is_saved(self, obj):
        request = self.context.get("request")
        user = request.user if request else None

        if user and user.is_authenticated:
            return obj.savedhotel_set.filter(user=user).exists()
        return False
    def get_primary_image(self, obj):
        image = getattr(obj, "images", []).filter(is_primary=True).first()
        return image.image.url if image and image.image else None

    def get_amenities(self, obj):
        amenity_fields = [f.name for f in obj._meta.fields if f.name.startswith("has_")]
        return [
            {
                "key": field.replace("has_", ""),
                "label": field.replace("has_", "").replace("_", " ").title()
            }
            for field in amenity_fields if getattr(obj, field)
        ]

    def get_distance_km(self, obj):
        city_lat = self.context.get("city_lat")
        city_lon = self.context.get("city_lon")
        if city_lat and city_lon and obj.latitude and obj.longitude:
            distance = haversine(city_lat, city_lon, float(obj.latitude), float(obj.longitude))
            return f"{round(distance, 1)}"
        return None

    def get_room_types(self, obj):
        booking_type = self.context.get("booking_type", "daily")
        checkin_time = self.context.get("checkin_time")  # format: "HH:MM"
        checkout_time = self.context.get("checkout_time")  # format: "HH:MM"

        checkin = self.context.get("checkin")
        checkout = self.context.get("checkout")
        min_price = self.context.get("min_price")
        max_price = self.context.get("max_price")
        limit = self.context.get("limit_room_types", 2)
        total_guests = self.context.get("total_guests", 1)
        number_of_rooms = self.context.get("number_of_rooms", 1)  # added for rooms left and total price

        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = None

        try:
            min_price = Decimal(min_price) if min_price is not None else None
        except:
            min_price = None
        try:
            max_price = Decimal(max_price) if max_price is not None else None
        except:
            max_price = None

        room_amenity_filters = self.context.get("room_amenity_filters", {})
        filters_hash = hashlib.md5(json.dumps(room_amenity_filters, sort_keys=True).encode()).hexdigest()

        cache_key = (
            f"hotel:{obj.id}:roomtypes:{booking_type}:{checkin}:{checkout}:"
            f"{checkin_time}:{checkout_time}:{min_price}:{max_price}:{total_guests}:{filters_hash}:{number_of_rooms}"
        )
        cached = cache.get(cache_key)
        if cached:
            return cached

        room_types_qs = getattr(obj, "room_types", None)
        if not room_types_qs:
            return []

        if room_amenity_filters:
            room_types_qs = room_types_qs.filter(**room_amenity_filters)

        room_types = list(room_types_qs.all())
        room_types = sorted(room_types, key=lambda r: r.get_current_price())

        # Add hotel and number_of_rooms to context for serializer usage
        self.context['number_of_rooms'] = number_of_rooms
        self.context['hotel'] = obj

        # No date filter provided — return limited
        if not checkin or not checkout:
            limited_room_types = room_types if limit is None else room_types[:limit]
            result = RoomTypeBaseSerializer(limited_room_types, many=True, context=self.context).data
            cache.set(cache_key, result, timeout=300)
            return result

        filtered = []

        if booking_type == "hourly" and checkin and checkin_time and checkout_time:
            try:
                checkin_time_obj = datetime.strptime(checkin_time, "%H:%M").time()
                checkout_time_obj = datetime.strptime(checkout_time, "%H:%M").time()
            except ValueError:
                return []  # skip if invalid time format

            for room in room_types:
                if booking_type == "hourly" and not room.is_hourly:
                    continue
                if room.max_occupancy < total_guests:
                    continue

                available = room.availability.filter(date=checkin, available_rooms__gte=number_of_rooms).exists()
                if not available:
                    continue

                price = room.hourly_price if room.hourly_price is not None else room.get_current_price()
                if min_price is not None and price < min_price:
                    continue
                if max_price is not None and price > max_price:
                    continue

                filtered.append(room)
                if limit and len(filtered) >= limit:
                    break

            result = RoomTypeBaseSerializer(filtered, many=True, context=self.context).data
            cache.set(cache_key, result, timeout=300)
            return result

        # Daily booking logic
        date_range = get_date_range(checkin, checkout)
        for room in room_types:
            if room.max_occupancy < total_guests:
                continue

            avail_qs = room.availability.filter(date__in=date_range, available_rooms__gte=number_of_rooms)
            available_dates = set(avail_qs.values_list("date", flat=True))
            if not all(day in available_dates for day in date_range):
                continue

            price = room.get_current_price()
            if min_price is not None and price < min_price:
                continue
            if max_price is not None and price > max_price:
                continue

            filtered.append(room)
            if limit and len(filtered) >= limit:
                break

        result = RoomTypeBaseSerializer(filtered, many=True, context=self.context).data

        if result and booking_type == "daily":
            total_nights = len(date_range)
            for room in result:
                room['total_nights'] = total_nights

        cache.set(cache_key, result, timeout=300)
        return result




    def get_nearby_places(self, obj):
            max_distance_km = 5  # example 5 km max distance
            hotel_lat = obj.latitude
            hotel_lon = obj.longitude

            places_qs = obj.nearby_places.all()
            nearby_filtered = []

            # Filter places dynamically in Python
            for place in places_qs:
                dist_km = haversine(float(hotel_lat),float(hotel_lon),float(place.latitude),float(place.longitude))
                if dist_km <= max_distance_km:
                    place._distance_km = dist_km  # attach for info/debug if needed
                    nearby_filtered.append(place)

            serializer = NearbyPlaceSerializer(
                nearby_filtered,
                many=True,
                context={"hotel_lat": hotel_lat, "hotel_lon": hotel_lon}
            )
            places_data = serializer.data

            # Group by category
            grouped = defaultdict(list)
            for place in places_data:
                grouped[place['category']].append(place)

            return grouped
    def get_tags(self, obj):
        tags = [ta.tag for ta in obj.tag_assignments.all()]
        tags_sorted = sorted(tags, key=lambda tag: tag.display_order)
        return HotelTagSerializer(tags_sorted, many=True).data

    
    # def get_max_adults(self, obj):
    # # Calculate max_adults across all room types for this hotel
    #     return obj.room_types.aggregate(max_adults=Max('max_adults'))['max_adults'] or 0

    # def get_max_children(self, obj):
    # # Calculate max_children across all room types for this hotel
    #     return obj.room_types.aggregate(max_children=Max('max_children'))['max_children'] or 0





class SimilarHotelSerializer(serializers.ModelSerializer):
    primary_image = serializers.SerializerMethodField()
    distance_km = serializers.SerializerMethodField()
    star_rating = serializers.DecimalField(max_digits=2, decimal_places=1, allow_null=True)
    review_score = serializers.DecimalField(max_digits=3, decimal_places=1, allow_null=True)
    lowest_price = serializers.SerializerMethodField()
    is_hot_deal = serializers.SerializerMethodField()
    free_breakfast = serializers.BooleanField(source='has_free_breakfast', default=False)
    free_cancellation = serializers.BooleanField(source='has_free_cancellation', default=False)

    class Meta:
        model = Hotel
        fields = [
            'id', 'name', 'primary_image', 'city', 'star_rating', 'review_score', 
            'distance_km', 'lowest_price', 'is_hot_deal', 'free_breakfast', 'free_cancellation'
        ]

    def get_primary_image(self, obj):
        image = obj.images.filter(is_primary=True).first()
        return image.image.url if image else None

    def get_distance_km(self, obj):
        if hasattr(obj, 'distance_km') and obj.distance_km is not None:

            return f"{round(obj.distance_km, 1)}"
        ref_lat = self.context.get('latitude')
        ref_lon = self.context.get('longitude')
        if obj.latitude and obj.longitude and ref_lat and ref_lon:
            return round(haversine(float(ref_lat), float(ref_lon), float(obj.latitude), float(obj.longitude)), 1)
        return None

    def get_lowest_price(self, obj):
        # Compute lowest price from room types
        prices = []
        for room in obj.room_types.all():
            price = room.get_current_price()
            if price is not None:
                prices.append(price)
        return min(prices) if prices else None
    def get_is_hot_deal(self, obj):
        # obj is a Hotel instance
        # Check if any room_type's availability has a special offer
        return RoomAvailability.objects.filter(
            room_type__hotel=obj,
            is_special_offer=True,
            available_rooms__gt=0
        ).exists()




class HotelManageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Hotel
        exclude = ["created_at", "updated_at"]
        # read_only_fields = ["owner"]


# serializers.py
class GuestSerializer(serializers.ModelSerializer):
    user_id = serializers.IntegerField(source='user.id', read_only=True)
    class Meta:
        model = Guest
        fields = '__all__'
    def get_user_id(self, obj):
        return obj.user.id if obj.user else None



class SimpleRoomTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = RoomType
        fields = ['id', 'name', 'price_per_night', 'hourly_price', 'bed_type']

# for showing minimul room info after booking

class BookingRoomItemSimpleSerializer(serializers.ModelSerializer):
    room_type = SimpleRoomTypeSerializer(read_only=True)

    class Meta:
        model = BookingRoomItem
        fields = [
            "id",
            "room_type",
            "number_of_rooms",
            "extra_bed_rooms_count",
            "extra_bed_count_per_room",
            "base_price",
            "total_price",
            "extra_bed_fee_total",
        ]


class BookingRoomItemSerializer(serializers.ModelSerializer):
    room_type = RoomTypeBaseSerializer(read_only=True)  # or your existing room type serializer
    base_price=serializers.DecimalField(max_digits=10, decimal_places=2)
    total_price = serializers.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        model = BookingRoomItem
        fields = [
            "id",
            "room_type",
            "number_of_rooms",
            "extra_bed_rooms_count",
            "extra_bed_count_per_room",
            "base_price",
            "total_price",
            "extra_bed_fee_total",
        ]

class BookingSerializer(serializers.ModelSerializer):
    # Include related room items
    room_items = BookingRoomItemSerializer(many=True, read_only=True, source='room_items')
    class Meta:
        model = Booking
        fields = '__all__'

class BookingDetailSerializer(serializers.ModelSerializer):
    hotel_name = serializers.CharField(source='hotel.name', read_only=True)
    room_type_name = serializers.CharField(source='room_type.name', read_only=True)
    guest = GuestSerializer(read_only=True) 

    class Meta:
        model = Booking
        fields = [
            'id', 'booking_number', 'status', 'payment_status',
            'check_in_date', 'check_out_date', 'number_of_guests', 'number_of_adults', 'number_of_children', 'number_of_rooms',
            'total_price', 'hotel_name', 'room_type_name','created_at',
            'guest'
        ]


class BookingPaymentSerializer(serializers.ModelSerializer):
    
    class Meta:
        model = BookingPayment
        fields = [
            'id',
            'booking',
            'amount',
            'payment_type',
            'payment_method',
            'status',
            'transaction_id',
            'payment_date',
            'notes',
            # 'created_at'
        ]
        read_only_fields = ['id', 'payment_date', 'booking', 'transaction_id']

    def validate_amount(self, value):
        if value <= 0:
            raise serializers.ValidationError("Amount must be greater than zero.")
        return value
    






class CouponSerializer(serializers.ModelSerializer):
    class Meta:
        model = Coupon
        fields = ['id', 'code', 'description', 'discount_amount', 'valid_from', 'valid_until', 'is_active']


class BookingCouponSerializer(serializers.ModelSerializer):
    coupon = CouponSerializer(read_only=True, default=None)
    coupon_id = serializers.PrimaryKeyRelatedField(queryset=Coupon.objects.all(), source='coupon', write_only=True)

    class Meta:
        model = BookingCoupon
        fields = ['id', 'booking', 'coupon', 'coupon_id', 'discount_amount']


class SavedHotelSerializer(serializers.ModelSerializer):
    hotel_name = serializers.CharField(source="hotel.name", read_only=True)
    hotel_image = serializers.SerializerMethodField()
    hotel_city = serializers.CharField(source="hotel.city", read_only=True)
    hotel_star_rating = serializers.DecimalField(
        source='hotel.star_rating', read_only=True,
        max_digits=2, decimal_places=1
    )

    class Meta:
        model = SavedHotel
        fields = [
            'id', 'hotel', 'hotel_name', 'hotel_image', 'hotel_city',
            'hotel_star_rating', 'created_at'
        ]
        read_only_fields = ['created_at']

    def get_hotel_image(self, obj):
        image = obj.hotel.images.filter(is_primary=True).first()
        return image.image.url if image and image.image else None



class HotelViewHistorySerializer(serializers.ModelSerializer):
    hotel_name = serializers.CharField(source='hotel.name', read_only=True)
    hotel_image = serializers.SerializerMethodField()
    hotel_city = serializers.CharField(source='hotel.city', read_only=True)
    hotel_star_rating = serializers.DecimalField(source='hotel.star_rating', read_only=True, max_digits=2, decimal_places=1)

    class Meta:
        model = HotelViewHistory
        fields = ['id', 'user', 'session_id', 'hotel', 'hotel_name', 'hotel_image', 'hotel_city', 'hotel_star_rating', 'viewed_at']
        read_only_fields = ['viewed_at']

    def get_hotel_image(self, obj):
        image = obj.hotel.images.filter(is_primary=True).first()
        return image.image.url if image and image.image else None


class SearchSuggestionSerializer(serializers.ModelSerializer):
    class Meta:
        model = SearchSuggestion
        fields = ['id', 'keyword']

class HotelSearchHistorySerializer(serializers.ModelSerializer):
    guest_summary = serializers.SerializerMethodField()
    date_range = serializers.SerializerMethodField()

    class Meta:
        model = HotelSearchHistory
        fields = ['id', 'city', 'checkin', 'checkout', 'adults', 'children', 'rooms', 'created_at', 'guest_summary', 'date_range']

    def get_guest_summary(self, obj):
        parts = []
        if obj.adults:
            parts.append(f"{obj.adults} adult{'s' if obj.adults > 1 else ''}")
        if obj.children:
            parts.append(f"{obj.children} child{'ren' if obj.children > 1 else ''}")
        return " • ".join(parts) if parts else "1 guest"

    def get_date_range(self, obj):
        return f"{obj.checkin.strftime('%b %d')} - {obj.checkout.strftime('%b %d')}"



class RoomHourlyAvailabilitySerializer(serializers.ModelSerializer):
    class Meta:
        model = RoomHourlyAvailability
        fields = '__all__'