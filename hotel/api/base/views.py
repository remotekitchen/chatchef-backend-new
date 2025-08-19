from rest_framework import viewsets, filters,permissions
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny,IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from datetime import datetime,timedelta
from decimal import Decimal
from django.core.cache import cache
from urllib.parse import urlencode
from hotel.utils.location import get_lat_lon_from_city
from hotel.utils.booking import get_or_create_guest, calculate_booking_cost
from hotel.utils.helpers import get_date_range,haversine,bounding_box,check_hourly_availability, block_hourly_availability,validate_bulk_inventory_csv, get_cache_version
from django.db.models import Q, Count, Min
from hotel.models import (Hotel, RoomType, RoomAvailability,Guest,Booking,BookingPayment,HotelImage,RoomImage,HotelReview,
                          SavedHotel, HotelViewHistory, HotelTagAssignment, NearbyPlace, HotelSearchHistory,Coupon,
                          SearchSuggestion,HotelTag,HotelPolicy,RoomHourlyAvailability,BookingRoomItem)
from hotel.api.base.serializers import (HotelBaseSerializer, RoomTypeBaseSerializer, RoomAvailabilitySerializer,GuestSerializer,
                                        BookingDetailSerializer,BookingPaymentSerializer,BookingSerializer,
                                        HotelManageSerializer,HotelImageSerializer,RoomImageSerializer,
                                        HotelReviewSerializer, SavedHotelSerializer, HotelViewHistorySerializer,
                                        HotelTagAssignmentSerializer, NearbyPlaceSerializer,
                                        HotelSearchHistorySerializer,SimilarHotelSerializer, BookingCouponSerializer,
                                        SearchSuggestionSerializer,HotelPolicySerializer,RoomHourlyAvailabilitySerializer,
                                        BookingRoomItemSerializer,BookingRoomItemSimpleSerializer)
from hotel.services.search import search_available_hotels
from hungrytiger.settings.defaults import mapbox_api_key
from hotel.api.base.filters import HotelFilter
from django.utils import timezone
from datetime import datetime, date
from django.shortcuts import get_object_or_404
import uuid
from django.db.models import Avg
from core.api.permissions import IsHotelOwner, IsHotelOwnerOfBooking,IsStaffAndHotelAdmin, IsHotelOwnerOrHotelAdmin
from rest_framework.pagination import PageNumberPagination
from rest_framework.exceptions import ValidationError, PermissionDenied
from hotel.tasks import send_payment_confirmation_message
from rest_framework import status
from django.utils.timezone import now
from django.db.models import Sum, Count, Avg, Q
from datetime import datetime, timedelta
from django.utils.timezone import make_aware
from django.utils.timezone import localtime
import pytz
from django.utils.dateparse import parse_date
from rest_framework.parsers import MultiPartParser, FormParser,JSONParser
from hotel.models import Booking, Coupon, BookingCoupon
import os
from django.conf import settings
from hotel.tasks import process_bulk_inventory_csv_task,send_booking_confirmation_message
from django.core.files.storage import default_storage
import logging
from django.core.files.base import ContentFile
from django.db import IntegrityError
from hashlib import md5
from rest_framework.views import APIView
from core.api.paginations import StandardResultsSetPagination,HotelStandardResultsSetPagination
from django.db import transaction
from accounts.api.base.serializers import  HotelOwnerUserSerializer
from django.contrib.auth import get_user_model
from django.utils.decorators import method_decorator
from django.views.decorators.cache import never_cache, cache_control
User = get_user_model()
import json
from hotel.cache_utils import invalidate_hotel_search_cache
from django.db.models import F
logger = logging.getLogger(__name__)



class BaseHotelViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Hotel.objects.filter(is_active=True).prefetch_related(
        "room_types__availability",
        "room_types__room_images",
        "images",
        "reviews",
        "tag_assignments__tag"
    )
    serializer_class = HotelBaseSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter, DjangoFilterBackend]
    filterset_class = HotelFilter
    search_fields = ["name", "city"]
    ordering_fields = ["review_score", "base_price_per_night", "star_rating"]
    ordering = ['name']
    pagination_class = StandardResultsSetPagination
    
    permission_classes = []  # Add IsAuthenticated if needed

    def _parse_dates(self, checkin, checkout=None, booking_type="daily"):
        try:
            checkin_date = datetime.strptime(checkin, "%Y-%m-%d").date()

            if booking_type == "hourly":
                if not checkout:
                    # If checkout is missing for hourly, set to checkin_date
                    checkout_date = checkin_date
                else:
                    checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()
                    if checkout_date != checkin_date:
                        return None, None, Response(
                            {"error": "For hourly bookings, check-in and check-out dates must be the same."},
                            status=status.HTTP_400_BAD_REQUEST
                        )
            else:
                if not checkout:
                    return None, None, Response(
                        {"error": "Checkout date is required for daily bookings."},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()
                if checkout_date <= checkin_date:
                    return None, None, Response(
                        {"error": "Checkout must be after checkin."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            return checkin_date, checkout_date, None

        except ValueError:
            return None, None, Response(
                {"error": "Invalid date format. Use YYYY-MM-DD."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
    
    @action(detail=False, methods=["get"], url_path="search")
    def search_hotels(self, request):
        lat = request.query_params.get("lat")
        lon = request.query_params.get("lon")
        radius_km = request.query_params.get("radius_km", 5)  # default 5 km

        try:
            lat = float(lat)
            lon = float(lon)
            radius_km = float(radius_km)
        except (TypeError, ValueError):
            return Response({"error": "Invalid lat, lon or radius_km."}, status=400)

        checkin = request.query_params.get("checkin")
        checkout = request.query_params.get("checkout")

        checkin_time = request.query_params.get("checkin_time")  # format: HH:MM
        checkout_time = request.query_params.get("checkout_time")  # format: HH:MM
        booking_type = request.query_params.get("booking_type", "daily")

        min_price = request.query_params.get("min_price")
        max_price = request.query_params.get("max_price")
        tags = request.query_params.getlist("tags")
        flash_deal = request.query_params.get("flash_deal")

        # --- Hotel amenities filter from `amenities` param ---
        amenities_param = request.query_params.get("amenities", "")
        amenities_list = [a.strip() for a in amenities_param.split(",") if a.strip()]
        hotel_filters = {f"has_{amenity}": True for amenity in amenities_list}

        # --- Room features filter from `room_features` param ---
        room_features_param = request.query_params.get("room_features", "")
        room_features_list = [f.strip() for f in room_features_param.split(",") if f.strip()]
        room_amenity_filters = {f"has_{feature}": True for feature in room_features_list}

        try:
            number_of_adults = int(request.query_params.get("adults", 1))
            number_of_children = int(request.query_params.get("children", 0))
            number_of_rooms = int(request.query_params.get("rooms", 1))
        except ValueError:
            return Response({"error": "Invalid adults, children, or rooms parameter."}, status=400)

        if number_of_adults < 1:
            return Response({"error": "At least one adult is required."}, status=400)
        if number_of_rooms < 1:
            return Response({"error": "At least one room is required."}, status=400)

        if number_of_children > 0:
            children_ages_str = request.query_params.get("children_ages", "")
            children_ages = [age.strip() for age in children_ages_str.split(",") if age.strip()]
            if len(children_ages) != number_of_children:
                return Response({"error": "Ages for all children must be provided."}, status=400)
            for age in children_ages:
                if not age.isdigit() or int(age) < 0:
                    return Response({"error": f"Invalid child age: {age}"}, status=400)

        total_guests = number_of_adults + number_of_children
        if total_guests < 1:
            return Response({"error": "Total guests must be at least 1."}, status=400)

        if lat is None or lon is None or checkin is None:
            return Response({"error": "lat, lon and checkin are required."}, status=400)

        if booking_type == "daily" and not checkout:
            return Response({"error": "checkout is required for daily bookings."}, status=400)

        checkin_date, checkout_date, error_resp = self._parse_dates(checkin, checkout, booking_type)

        if booking_type == "hourly":
            if checkin_date != checkout_date:
                return Response({"error": "Hourly bookings must have the same check-in and check-out date."}, status=400)

            if not checkin_time or not checkout_time:
                return Response({"error": "checkin_time and checkout_time are required for hourly bookings."}, status=400)

            try:
                checkin_time_obj = datetime.strptime(checkin_time, "%H:%M").time()
                checkout_time_obj = datetime.strptime(checkout_time, "%H:%M").time()

                if checkout_time_obj <= checkin_time_obj:
                    return Response({"error": "Checkout time must be after check-in time."}, status=400)

            except ValueError:
                return Response({"error": "Invalid time format. Use HH:MM."}, status=400)

        if error_resp:
            return error_resp

        query_items = sorted(request.query_params.items())
        query_string = "&".join(f"{k}={v}" for k, v in query_items)
        version = get_cache_version()
        cache_key_hotels = f"hotel_search:{version}:{md5(query_string.encode()).hexdigest()}"

        cached_response = cache.get(cache_key_hotels)
        if cached_response:
            return Response({
                "count": len(cached_response),
                "next": None,
                "previous": None,
                "results": cached_response,
            })

        hotel_ids, error = search_available_hotels(
            request=request,
            lat=lat,
            lon=lon,
            radius_km=radius_km,
            checkin=checkin,
            checkout=checkout,
            booking_type=booking_type,
            checkin_time=checkin_time,
            checkout_time=checkout_time,
            mapbox_key=None,
            number_of_adults=number_of_adults,
            number_of_children=number_of_children,
            number_of_rooms=number_of_rooms,
            min_price=min_price,
            max_price=max_price,
            room_amenity_filters=room_amenity_filters,
        )
        if error:
            return Response(error, status=400)

        hotels = Hotel.objects.filter(
            id__in=[h.id for h in hotel_ids],
            **hotel_filters
        ).prefetch_related(
            "room_types__availability",
            "room_types__room_images",
            "images",
            "reviews",
            "tag_assignments__tag"
        )

        if flash_deal and flash_deal.lower() == "true":
            hotels = hotels.filter(tag_assignments__tag__is_flash_deal=True)

        if tags:
            hotels = hotels.filter(tag_assignments__tag__id__in=tags)

        deal_type = request.query_params.get("deal_type")
        if deal_type == "special":
            hotels = hotels.filter(tag_assignments__tag__is_special_deal=True)
        elif deal_type == "season":
            hotels = hotels.filter(tag_assignments__tag__is_seasonal_deal=True)

        hotels = hotels.distinct()
        hotel_list = list(hotels)

        for hotel in hotel_list:
            if hotel.latitude and hotel.longitude:
                hotel.distance_km = haversine(lat, lon, float(hotel.latitude), float(hotel.longitude))
            else:
                hotel.distance_km = float("inf")

        ordering_param = request.query_params.get("ordering", "most_relevant")
        ordering_list = [o.strip() for o in ordering_param.split(",") if o.strip()]


        for hotel in hotel_list:
            rating = float(hotel.review_score or 0)
            price = float(hotel.base_price_per_night or 0)
            distance = hotel.distance_km
            hotel.relevance_score = (
                (0.4 * rating) +
                (0.4 * (1 / (distance + 0.1))) +
                (0.2 * (1 / (price + 1)))
            )

       # Default: sort by relevance
        if not ordering_list:
            ordering_list = ["most_relevant"]

        for ordering in reversed(ordering_list):  # Reversed for correct priority
            if ordering == "most_relevant":
                hotel_list.sort(key=lambda h: h.relevance_score, reverse=True)
            elif ordering == "least_relevant":
                hotel_list.sort(key=lambda h: h.relevance_score)
            elif ordering == "newest":
                hotel_list.sort(key=lambda h: h.created_at, reverse=True)
            elif ordering == "oldest":
                hotel_list.sort(key=lambda h: h.created_at)
            elif ordering == "nearest":
                hotel_list.sort(key=lambda h: h.distance_km)
            elif ordering == "farthest":
                hotel_list.sort(key=lambda h: h.distance_km, reverse=True)
            elif ordering == "lowprice":
                hotel_list.sort(key=lambda h: h.base_price_per_night)
            elif ordering == "highprice":
                hotel_list.sort(key=lambda h: h.base_price_per_night, reverse=True)
            elif ordering == "top_rated":
                hotel_list.sort(key=lambda h: h.review_score or 0, reverse=True)

        paginator = StandardResultsSetPagination()
        page = paginator.paginate_queryset(hotel_list, request)
        serializer = self.get_serializer(page, many=True, context={
            "request": request,
            "checkin": checkin_date,
            "checkout": checkout_date,
            "checkin_time": checkin_time,
            "checkout_time": checkout_time,
            "booking_type": booking_type,
            "min_price": min_price,
            "max_price": max_price,
            "limit_room_types": 5,
            "city_lat": lat,
            "city_lon": lon,
            "include_nearby": False,
            "total_guests": total_guests,
        })

        cache.set(cache_key_hotels, serializer.data, timeout=300)
        return paginator.get_paginated_response(serializer.data)


    @action(detail=True, methods=["get"], url_path="detail")
    def hotel_detail(self, request, pk=None):
        checkin = request.query_params.get("checkin")
        checkout = request.query_params.get("checkout")

        if not checkin or not checkout:
            return Response({"error": "checkin and checkout are required."}, status=status.HTTP_400_BAD_REQUEST)

        checkin_date, checkout_date, error_resp = self._parse_dates(checkin, checkout)
        if error_resp:
            return error_resp

        hotel = self.get_object()

        city_coords = get_lat_lon_from_city(hotel.city, mapbox_api_key)
        if not city_coords:
            return Response({"error": "Could not find city location."}, status=status.HTTP_400_BAD_REQUEST)

        # Extract room amenity filters (like has_air_conditioning=true)
        room_amenity_filters = {
            k: v.lower() == "true"
            for k, v in request.query_params.items()
            if k.startswith("has_")
        }

        context = {
            "request": request,
            "checkin": checkin_date,
            "checkout": checkout_date,
            "city_lat": city_coords[0],
            "city_lon": city_coords[1],
            "min_price": request.query_params.get("min_price"),
            "max_price": request.query_params.get("max_price"),
            "limit_room_types": None,
            "include_nearby": True,
            "room_amenity_filters": room_amenity_filters,
            "total_guests": int(request.query_params.get("adults", 1)) + int(request.query_params.get("children", 0))
        }

        serializer = self.get_serializer(hotel, context=context)
        return Response(serializer.data)


# user can see available room by the date range

class BaseRoomTypeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = RoomType.objects.select_related("hotel").prefetch_related("availability").order_by("id")
    serializer_class = RoomTypeBaseSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [AllowAny]

    @action(detail=True, methods=['get'], url_path='availability')
    def availability(self, request, pk=None):
        try:
            room_type = self.get_object()
            checkin = request.query_params.get('checkin')
            checkout = request.query_params.get('checkout')

            if not checkin or not checkout:
                return Response({"error": "Please provide checkin and checkout dates."}, status=400)

            checkin_date = datetime.strptime(checkin, "%Y-%m-%d").date()
            checkout_date = datetime.strptime(checkout, "%Y-%m-%d").date()

            if checkin_date >= checkout_date:
                return Response({"error": "Checkout must be after checkin."}, status=400)

            availability = RoomAvailability.objects.filter(
                room_type=room_type,
                date__gte=checkin_date,
                date__lt=checkout_date
            ).order_by("date")

            serializer = RoomAvailabilitySerializer(availability, many=True)
            return Response(serializer.data)

        except Exception as e:
            return Response({"error": str(e)}, status=500)







class BaseSimilarHotelViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = SimilarHotelSerializer
    queryset = Hotel.objects.filter(is_active=True)
    # pagination_class = PageNumberPagination

    @action(detail=True, methods=['get'])
    def nearby(self, request, pk=None):
        hotel = self.get_object()
        radius_km = float(request.query_params.get('radius', 5))

        checkin_str = request.query_params.get("checkin")
        checkout_str = request.query_params.get("checkout")

        if not checkin_str or not checkout_str:
            return Response({"error": "checkin and checkout query params are required"}, status=400)

        try:
            checkin = datetime.strptime(checkin_str, "%Y-%m-%d").date()
            checkout = datetime.strptime(checkout_str, "%Y-%m-%d").date()
        except ValueError:
            return Response({"error": "Invalid date format for checkin or checkout"}, status=400)

        if checkin >= checkout:
            return Response({"error": "checkout must be after checkin"}, status=400)

        lat = float(hotel.latitude)
        lon = float(hotel.longitude)

        min_lat, max_lat, min_lon, max_lon = bounding_box(lat, lon, radius_km)

        # Candidate hotels inside bounding box (exclude current hotel)
        hotels_qs = Hotel.objects.filter(
            is_active=True,
            latitude__gte=min_lat,
            latitude__lte=max_lat,
            longitude__gte=min_lon,
            longitude__lte=max_lon,
        ).exclude(id=hotel.id)

        # Date range list
        days_count = (checkout - checkin).days
        date_range = [checkin + timedelta(days=i) for i in range(days_count)]

        # Get room_type ids related to candidate hotels
        room_types_qs = RoomType.objects.filter(hotel__in=hotels_qs)

        # Get availability for these room_types in the date_range
        avail_qs = RoomAvailability.objects.filter(
            room_type__in=room_types_qs,
            date__gte=checkin,
            date__lt=checkout,
            available_rooms__gte=1
        )

        # Group availability by room_type and count distinct dates available
        avail_counts = avail_qs.values('room_type').annotate(
            available_days=Count('date', distinct=True),
            hotel_id=Min('room_type__hotel')
        ).filter(available_days=days_count)

        # Get hotel ids that have at least one room_type available for all requested days
        available_hotel_ids = set(avail_counts.values_list('hotel_id', flat=True))

        # Filter hotels to only those with availability
        nearby_hotels = list(
            hotels_qs.filter(id__in=available_hotel_ids)
        )

        # Calculate distance and filter by radius
        filtered_hotels = []
        for h in nearby_hotels:
            try:
                h_lat = float(h.latitude)
                h_lon = float(h.longitude)
            except (TypeError, ValueError):
                continue
            dist = haversine(lat, lon, h_lat, h_lon)
            if dist <= radius_km:
                h._distance_km = dist
                filtered_hotels.append(h)

        filtered_hotels.sort(key=lambda h: h._distance_km)

        serializer = SimilarHotelSerializer(filtered_hotels, many=True, context={'latitude': lat, 'longitude': lon})

        return Response(serializer.data)
    


class BaseBookingViewSet(viewsets.ViewSet):
    permission_classes = [AllowAny]
    # pagination_class = PageNumberPagination




    @action(detail=False, methods=["post"], url_path="precheckout")
    def precheckout(self, request):
        rooms_data = request.data.get("rooms")  # New multi-room input

        booking_type = request.data.get("booking_type", "daily")
        hours = int(request.data.get("hours", 1))

        checkin = request.data.get("check_in_date")
        checkout = request.data.get("check_out_date")
        
        try:
            check_in = datetime.strptime(checkin, "%Y-%m-%d").date()
            check_out = datetime.strptime(checkout, "%Y-%m-%d").date() if booking_type == "daily" else check_in
        except Exception:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

        nights = (check_out - check_in).days if booking_type == "daily" else 0

        number_of_adults = int(request.data.get("number_of_adults", 1))
        number_of_children = int(request.data.get("number_of_children", 0))
        total_guests = number_of_adults + number_of_children

        checkin_time = request.data.get("checkin_time") if booking_type == "hourly" else None
        checkout_time = request.data.get("checkout_time") if booking_type == "hourly" else None

        coupon_code = request.data.get("coupon_code", None)

        if rooms_data:
            if not isinstance(rooms_data, list) or not rooms_data:
                return Response({"error": "rooms must be a non-empty list"}, status=400)

            room_type_ids = [room.get("room_type") for room in rooms_data]
            room_types = RoomType.objects.filter(id__in=room_type_ids).select_related("hotel").prefetch_related("room_images")
            room_type_map = {rt.id: rt for rt in room_types}

            if len(room_types) != len(room_type_ids):
                return Response({"error": "One or more room_type IDs are invalid."}, status=400)

            hotels = {rt.hotel_id for rt in room_types}
            if len(hotels) != 1:
                return Response({"error": "All room types must belong to the same hotel."}, status=400)

            try:
                with transaction.atomic():
                    coupon = None
                    if coupon_code:
                        coupon = Coupon.objects.select_for_update().filter(
                            code=coupon_code,
                            is_active=True,
                            valid_from__lte=now(),
                            valid_until__gte=now()
                        ).first()
                        if not coupon:
                            return Response({"error": "Invalid or expired coupon."}, status=400)

                    total_base_price = Decimal("0.00")
                    total_room_price = Decimal("0.00")
                    total_taxes = Decimal("0.00")
                    total_service_fee = Decimal("0.00")
                    total_discount = Decimal("0.00")
                    total_extra_bed_fee = Decimal("0.00")

                    rooms_response = []

                    for room_item in rooms_data:
                        rt_id = room_item.get("room_type")
                        rt = room_type_map.get(rt_id)

                        number_of_rooms = int(room_item.get("number_of_rooms", 1))
                        extra_bed_rooms_count = int(room_item.get("extra_bed_rooms_count", 0))
                        extra_bed_count_per_room = int(room_item.get("extra_bed_count_per_room", 0))
                        extra_bed_requested = extra_bed_rooms_count > 0 and extra_bed_count_per_room > 0

                        if extra_bed_rooms_count > number_of_rooms:
                            return Response({
                                "error": f"extra_bed_rooms_count cannot exceed number_of_rooms for room type {rt.name}."
                            }, status=400)

                        base_price, room_price, taxes, service_fee, discount_amount, total_price, extra_bed_fee = calculate_booking_cost(
                            room_type=rt,
                            nights=nights,
                            rooms=number_of_rooms,
                            coupon=coupon,
                            extra_bed_requested=extra_bed_requested,
                            extra_bed_rooms_count=extra_bed_rooms_count,
                            extra_bed_count_per_room=extra_bed_count_per_room,
                            booking_type=booking_type,
                            hours=hours
                        )

                        total_base_price += base_price
                        total_room_price += room_price
                        total_taxes += taxes
                        total_service_fee += service_fee
                        total_discount += discount_amount
                        total_extra_bed_fee += extra_bed_fee

                        image_url = None
                        if rt.room_images.exists():
                            image_url = rt.room_images.first().image.url

                        policy = getattr(rt.hotel, "policy", None)

                        rooms_response.append({
                            "room_type": RoomTypeBaseSerializer(rt, context={"checkin": check_in, "checkout": check_out}).data,
                            "hotel_name": rt.hotel.name,
                            "image_url": image_url,
                            "number_of_rooms": number_of_rooms,
                            "extra_bed_request": extra_bed_requested,
                            "extra_bed_rooms_count": extra_bed_rooms_count,
                            "extra_bed_count_per_room": extra_bed_count_per_room,
                            "cost_summary": {
                                "base_price": str(base_price),
                                "total_room_price": str(room_price),
                                "extra_bed_fee": str(extra_bed_fee),
                                "discount": str(discount_amount),
                                "taxes": str(taxes),
                                "service_fee": str(service_fee),
                                "total_price": str(total_price)
                            },
                            "policies": {
                                "cancellation_policy": getattr(policy, "cancellation_policy", None),
                                "is_refundable": getattr(rt, "is_refundable", False),
                                "min_age_checkin": getattr(rt, "min_age_checkin", None),
                                "smoking_allowed": getattr(rt, "smoking_allowed", False),
                                "children_allowed": getattr(policy, "children_allowed", True),
                                "children_policy": getattr(policy, "children_policy", ""),
                                "pets_allowed": getattr(policy, "pets_allowed", False),
                                "pets_fee": str(getattr(policy, "pets_fee", None)) if policy and getattr(policy, "pets_fee", None) else None,
                                "shuttle_service": getattr(policy, "has_shuttle_service", False),
                                "shuttle_service_fee": str(getattr(policy, "shuttle_service_fee", None)) if policy and getattr(policy, "shuttle_service_fee", None) else None,
                                "breakfast_included": getattr(policy, "breakfast_included", False),
                                "breakfast_fee": str(getattr(policy, "breakfast_fee", None)) if policy and getattr(policy, "breakfast_fee", None) else None,
                                "extra_person_fee": str(getattr(policy, "extra_person_fee", None)) if policy and getattr(policy, "extra_person_fee", None) else None,
                                "general_policy": getattr(policy, "general_policy", ""),
                                "payment_options": getattr(policy, "payment_options", ""),
                            },
                        })

                    total_price = total_room_price + total_taxes + total_service_fee + total_extra_bed_fee - total_discount

            except Exception as e:
                return Response({"error": str(e)}, status=400)

            response_data = {
                "rooms": rooms_response,
                "cost_summary": {
                    "base_price": str(total_base_price),
                    "total_room_price": str(total_room_price),
                    "extra_bed_fee": str(total_extra_bed_fee),
                    "discount": str(total_discount),
                    "taxes": str(total_taxes),
                    "service_fee": str(total_service_fee),
                    "total_price": str(total_price)
                },
                "check_in_date": checkin,
                "check_out_date": checkout if booking_type == "daily" else None,
                "checkin_time": checkin_time,
                "checkout_time": checkout_time,
                "nights": nights,
                "number_of_adults": number_of_adults,
                "number_of_children": number_of_children,
                "number_of_guests": total_guests,
                "booking_type": booking_type,
                "hours": hours if booking_type == "hourly" else None,
                "payment_methods": [
                    {"value": key, "label": label} for key, label in BookingPayment.PAYMENT_METHOD_CHOICES
                ],
                "promo_applied": {
                    "code": coupon.code,
                    "discount_amount": str(coupon.discount_amount)
                } if coupon else None,
            }

            cache_key = f'''precheckout_multi:{checkin}:{checkout}:{total_guests}:{coupon_code or ''}:{booking_type}:{hours}:{','.join(f"{r['room_type']}:{r['number_of_rooms']}" for r in rooms_data)}'''

            cache.set(cache_key, response_data, timeout=300)
            return Response(response_data)

        else:
            # Single-room precheckout (wrap coupon locking inside transaction)
            room_type_id = request.data.get("room_type")
            checkin = request.data.get("check_in_date")
            checkout = request.data.get("check_out_date")
            checkin_time = request.data.get("checkin_time")  # hourly
            checkout_time = request.data.get("checkout_time")  # hourly
            number_of_adults = int(request.data.get("number_of_adults", 1))
            number_of_children = int(request.data.get("number_of_children", 0))
            total_guests = number_of_adults + number_of_children

            rooms = int(request.data.get("number_of_rooms", 1))

            extra_bed_requested = request.data.get("extra_bed_request", False)
            if isinstance(extra_bed_requested, str):
                extra_bed_requested = extra_bed_requested.lower() in ("true", "1", "yes")

            extra_bed_rooms_count = int(request.data.get("extra_bed_rooms_count", 0))
            extra_bed_count_per_room = int(request.data.get("extra_bed_count_per_room", 0))

            booking_type = request.data.get("booking_type", "daily")
            hours = int(request.data.get("hours", 1))

            coupon_code = request.data.get("coupon_code", None)

            if booking_type == "hourly":
                if not all([room_type_id, checkin, checkin_time, checkout_time]):
                    return Response({
                        "error": "room_type, check_in_date, checkin_time, and checkout_time are required for hourly booking."
                    }, status=400)
            else:
                if not all([room_type_id, checkin, checkout]):
                    return Response({
                        "error": "room_type, check_in_date, and check_out_date are required for daily booking."
                    }, status=400)

            try:
                room_type = RoomType.objects.select_related("hotel").prefetch_related("room_images").get(id=room_type_id)
            except RoomType.DoesNotExist:
                return Response({"error": "Invalid room_type."}, status=400)

            try:
                check_in = datetime.strptime(checkin, "%Y-%m-%d").date()
                check_out = datetime.strptime(checkout, "%Y-%m-%d").date() if booking_type == "daily" else check_in
            except ValueError:
                return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

            if booking_type == "daily" and check_in >= check_out:
                return Response({"error": "Check-out must be after check-in."}, status=400)
            if check_in < date.today():
                return Response({"error": "Check-in date cannot be in the past."}, status=400)

            nights = (check_out - check_in).days if booking_type == "daily" else 0

            if extra_bed_requested:
                if extra_bed_rooms_count <= 0 or extra_bed_count_per_room <= 0:
                    extra_bed_rooms_count = max(extra_bed_rooms_count, 1)
                    extra_bed_count_per_room = max(extra_bed_count_per_room, 1)
                if extra_bed_rooms_count > rooms:
                    return Response({"error": "extra_bed_rooms_count cannot exceed number_of_rooms."}, status=400)

            try:
                with transaction.atomic():
                    coupon = None
                    if coupon_code:
                        coupon = Coupon.objects.select_for_update().filter(
                            code=coupon_code,
                            is_active=True,
                            valid_from__lte=now(),
                            valid_until__gte=now()
                        ).first()
                        if not coupon:
                            return Response({"error": "Invalid or expired coupon."}, status=400)

                    base_price, total_room_price, taxes, service_fee, discount_amount, total_price, extra_bed_fee_total = calculate_booking_cost(
                        room_type=room_type,
                        nights=nights,
                        rooms=rooms,
                        coupon=coupon,
                        extra_bed_requested=extra_bed_requested,
                        extra_bed_rooms_count=extra_bed_rooms_count,
                        extra_bed_count_per_room=extra_bed_count_per_room,
                        booking_type=booking_type,
                        hours=hours
                    )
            except Exception as e:
                return Response({"error": str(e)}, status=400)

            image_url = None
            if room_type.room_images.exists():
                image_url = room_type.room_images.first().image.url
            policy = getattr(room_type.hotel, "policy", None)

            response_data = {
                "room_type": RoomTypeBaseSerializer(room_type, context={"checkin": check_in, "checkout": check_out}).data,
                "hotel_name": room_type.hotel.name,
                "image_url": image_url,
                "check_in_date": checkin,
                "check_out_date": checkout if booking_type == "daily" else None,
                "checkin_time": checkin_time if booking_type == "hourly" else None,
                "checkout_time": checkout_time if booking_type == "hourly" else None,
                "nights": nights,
                "number_of_adults": number_of_adults,
                "number_of_children": number_of_children,
                "number_of_guests": total_guests,
                "number_of_rooms": rooms,
                "hours": hours if booking_type == "hourly" else None,
                "extra_bed_request": extra_bed_requested,
                "extra_bed_rooms_count": extra_bed_rooms_count,
                "extra_bed_count_per_room": extra_bed_count_per_room,
                "policies": {
                    "cancellation_policy": getattr(policy, "cancellation_policy", None),
                    "is_refundable": getattr(room_type, "is_refundable", False),
                    "min_age_checkin": getattr(room_type, "min_age_checkin", None),
                    "smoking_allowed": getattr(room_type, "smoking_allowed", False),
                    "children_allowed": getattr(policy, "children_allowed", True),
                    "children_policy": getattr(policy, "children_policy", ""),
                    "pets_allowed": getattr(policy, "pets_allowed", False),
                    "pets_fee": str(getattr(policy, "pets_fee", None)) if policy and getattr(policy, "pets_fee", None) else None,
                    "shuttle_service": getattr(policy, "has_shuttle_service", False),
                    "shuttle_service_fee": str(getattr(policy, "shuttle_service_fee", None)) if policy and getattr(policy, "shuttle_service_fee", None) else None,
                    "breakfast_included": getattr(policy, "breakfast_included", False),
                    "breakfast_fee": str(getattr(policy, "breakfast_fee", None)) if policy and getattr(policy, "breakfast_fee", None) else None,
                    "extra_person_fee": str(getattr(policy, "extra_person_fee", None)) if policy and getattr(policy, "extra_person_fee", None) else None,
                    "general_policy": getattr(policy, "general_policy", ""),
                    "payment_options": getattr(policy, "payment_options", ""),
                },
                "cost_summary": {
                    "base_price": str(base_price),
                    "total_room_price": str(total_room_price),
                    "extra_bed_fee": str(extra_bed_fee_total),
                    "discount": str(discount_amount),
                    "taxes": str(taxes),
                    "service_fee": str(service_fee),
                    "total_price": str(total_price)
                },
                "payment_methods": [
                    {"value": key, "label": label} for key, label in BookingPayment.PAYMENT_METHOD_CHOICES
                ],
                "promo_applied": {
                    "code": coupon.code,
                    "discount_amount": str(coupon.discount_amount)
                } if coupon else None,
            }

            date_range = get_date_range(check_in, check_out) if booking_type == "daily" else [check_in]
            availability_qs = RoomAvailability.objects.filter(
                room_type=room_type,
                date__in=date_range
            )
            min_available = None
            promo_active = False
            for day in availability_qs:
                if min_available is None or day.available_rooms < min_available:
                    min_available = day.available_rooms
                if day.is_special_offer:
                    promo_active = True

            availability_note = None
            if min_available is not None and min_available <= 5:
                availability_note = f"Only {min_available} room(s) left!"
                if promo_active:
                    availability_note += " Promo active!"

            if availability_note:
                response_data["availability_note"] = availability_note

            cache_key = f"precheckout:{room_type_id}:{checkin}:{checkout}:{total_guests}:{rooms}:{coupon_code or ''}:{booking_type}:{hours}:{extra_bed_rooms_count}:{extra_bed_count_per_room}"
            cache.set(cache_key, response_data, timeout=300)
            return Response(response_data)



    # create booking

    @action(detail=False, methods=["post"], url_path="create")
    def create_booking(self, request):
        guest_data = request.data.get("guest", {})
        booking_data = request.data.get("booking")
        if not booking_data:
            return Response({"error": "Booking data is required."}, status=400)

        # Extract guest counts
        num_adults = int(booking_data.get("number_of_adults", 1))
        num_children = int(booking_data.get("number_of_children", 0))
        num_guests = num_adults + num_children

        # Extra bed info for fallback single-room booking
        extra_bed_requested = booking_data.get("extra_bed_request", False)
        if isinstance(extra_bed_requested, str):
            extra_bed_requested = extra_bed_requested.lower() in ("true", "1", "yes")

        try:
            extra_bed_rooms_count = int(booking_data.get("extra_bed_rooms_count", 0))
            extra_bed_count_per_room = int(booking_data.get("extra_bed_count_per_room", 0))
        except (TypeError, ValueError):
            return Response({"error": "Invalid extra bed counts."}, status=400)

        # Coupon and booking type info
        coupon_code = booking_data.get("coupon_code")
        booking_type = booking_data.get("booking_type", "daily")
        hours = int(booking_data.get("hours", 1))

        # Multi-room booking data (optional)
        rooms_data = booking_data.get("rooms", None)

        # Get or create guest
        guest = get_or_create_guest(
            guest_data,
            user=request.user if request.user.is_authenticated else None
        )

        # Validate booking source and user permissions
        valid_sources = ['walk_in', 'online', 'phone', 'third_party']
        user = request.user
        if user.is_authenticated and IsHotelOwner().has_permission(request, self):
            booking_source = 'walk_in'
            status = 'confirmed'
        else:
            booking_source = booking_data.get("booking_source", "online")
            if booking_source not in valid_sources:
                booking_source = 'online'
            status = 'confirmed'

        # Parse dates (common for all bookings)
        try:
            check_in = datetime.strptime(booking_data["check_in_date"], "%Y-%m-%d").date()
            if booking_type == "daily":
                if "check_out_date" not in booking_data:
                    return Response({"error": "check_out_date is required for daily bookings."}, status=400)
                check_out = datetime.strptime(booking_data["check_out_date"], "%Y-%m-%d").date()
            else:
                check_out = check_in  # hourly bookings same day
        except ValueError:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

        if booking_type == "daily" and check_in >= check_out:
            return Response({"error": "Check-out must be after check-in."}, status=400)

        if check_in < date.today():
            return Response({"error": "Check-in date cannot be in the past."}, status=400)

        # Parse times for hourly booking
        if booking_type == "hourly":
            checkin_time_str = booking_data.get("checkin_time")
            checkout_time_str = booking_data.get("checkout_time")
            if not checkin_time_str or not checkout_time_str:
                return Response({"error": "checkin_time and checkout_time are required for hourly bookings."}, status=400)
            try:
                checkin_time = datetime.strptime(checkin_time_str, "%H:%M").time()
                checkout_time = datetime.strptime(checkout_time_str, "%H:%M").time()
                if checkout_time <= checkin_time:
                    return Response({"error": "checkout_time must be after checkin_time."}, status=400)
            except ValueError:
                return Response({"error": "Invalid time format. Use HH:MM."}, status=400)
        else:
            checkin_time = None
            checkout_time = None

        # Validate payment method
        payment_method = booking_data.get("payment_method", "cash")
        valid_methods = [k for k, _ in BookingPayment.PAYMENT_METHOD_CHOICES]
        if payment_method not in valid_methods:
            return Response({"error": "Invalid payment method."}, status=400)

        # ---------- MULTI-ROOM BOOKING FLOW ----------
        if rooms_data and isinstance(rooms_data, list):
            # Validate room_type ids and fetch RoomType objects
            room_type_ids = [room["room_type"] for room in rooms_data]
            room_types = RoomType.objects.filter(id__in=room_type_ids).select_related("hotel")

            if len(room_types) != len(room_type_ids):
                return Response({"error": "One or more room_type IDs are invalid."}, status=400)

            # Check all room types belong to the same hotel
            hotel_ids = {rt.hotel_id for rt in room_types}
            if len(hotel_ids) != 1:
                return Response({"error": "All rooms must belong to the same hotel."}, status=400)

            hotel = room_types[0].hotel
            date_range = get_date_range(check_in, check_out)

            try:
                with transaction.atomic():
                    # Lock availability and validate per room & date
                    for room in rooms_data:
                        rt = next((r for r in room_types if r.id == room["room_type"]), None)
                        number_of_rooms = int(room.get("number_of_rooms", 1))
                        extra_bed_rooms_count = int(room.get("extra_bed_rooms_count", 0))
                        extra_bed_count_per_room = int(room.get("extra_bed_count_per_room", 0))
                        extra_bed_requested = extra_bed_rooms_count > 0 and extra_bed_count_per_room > 0

                        if booking_type == "daily":
                            availability_qs = RoomAvailability.objects.select_for_update().filter(
                                room_type=rt,
                                date__in=date_range
                            )
                            availability_map = {a.date: a for a in availability_qs}
                            for day in date_range:
                                availability = availability_map.get(day)
                                if not availability:
                                    return Response({"error": f"No availability set for {day} and room {rt.name}."}, status=400)
                                if availability.available_rooms < number_of_rooms:
                                    return Response({"error": f"Only {availability.available_rooms} rooms available on {day} for room {rt.name}."}, status=400)

                        elif booking_type == "hourly":
                            is_available, err_msg = check_hourly_availability(
                                rt, check_in, checkin_time, checkout_time, number_of_rooms
                            )
                            if not is_available:
                                return Response({"error": err_msg}, status=400)

                    # Coupon validation
                    coupon = None
                    discount_amount = Decimal("0.00")
                    if coupon_code:
                        coupon = Coupon.objects.select_for_update().filter(
                            code=coupon_code,
                            is_active=True,
                            valid_from__lte=now(),
                            valid_until__gte=now()
                        ).first()

                        if not coupon:
                            return Response({"error": "Invalid or expired coupon code."}, status=400)

                        guest_email = guest.email.strip().lower()
                        already_used = BookingCoupon.objects.filter(
                            coupon=coupon,
                            booking__guest__email__iexact=guest_email,
                            booking__hotel=hotel
                        ).exists()

                        if already_used:
                            return Response({"error": "You have already used this coupon for this hotel."}, status=400)

                        discount_amount = coupon.discount_amount

                    # Calculate costs per room & accumulate totals
                    total_base_price = Decimal("0.00")
                    total_room_price = Decimal("0.00")
                    total_taxes = Decimal("0.00")
                    total_service_fee = Decimal("0.00")
                    total_extra_bed_fee = Decimal("0.00")

                    for room in rooms_data:
                        rt = next((r for r in room_types if r.id == room["room_type"]), None)
                        number_of_rooms = int(room.get("number_of_rooms", 1))
                        extra_bed_rooms_count = int(room.get("extra_bed_rooms_count", 0))
                        extra_bed_count_per_room = int(room.get("extra_bed_count_per_room", 0))
                        extra_bed_requested = extra_bed_rooms_count > 0 and extra_bed_count_per_room > 0

                        base_price, room_price, taxes, service_fee, _, _, extra_bed_fee = calculate_booking_cost(
                            room_type=rt,
                            nights=(check_out - check_in).days if booking_type == "daily" else 0,
                            rooms=number_of_rooms,
                            coupon=None,  # coupon discount applied globally later
                            extra_bed_requested=extra_bed_requested,
                            extra_bed_rooms_count=extra_bed_rooms_count,
                            extra_bed_count_per_room=extra_bed_count_per_room,
                            booking_type=booking_type,
                            hours=hours
                        )

                        total_base_price += base_price
                        total_room_price += room_price
                        total_taxes += taxes
                        total_service_fee += service_fee
                        total_extra_bed_fee += extra_bed_fee

                    total_discount = discount_amount
                    total_price = total_room_price + total_taxes + total_service_fee + total_extra_bed_fee - total_discount
                    total_nights = (check_out - check_in).days if booking_type == "daily" else 0

                    # Create main Booking record
                    booking = Booking.objects.create(
                        guest=guest,
                        
                        hotel=hotel,
                        check_in_date=check_in,
                        check_out_date=check_out,
                        number_of_adults=num_adults,
                        number_of_children=num_children,
                        number_of_guests=num_guests,
                        number_of_rooms=sum(int(r.get("number_of_rooms", 1)) for r in rooms_data),
                        base_price=total_base_price,
                        total_nights=total_nights,
                        total_room_price=total_room_price,
                        taxes=total_taxes,
                        additional_fees=total_service_fee,
                        discount_amount=total_discount,
                        total_price=total_price,
                        special_requests=booking_data.get("special_requests", ""),
                        estimated_arrival=booking_data.get("estimated_arrival"),
                        crib_request=booking_data.get("crib_request", False),
                        extra_bed_request=any(
                            int(r.get("extra_bed_rooms_count", 0)) > 0 and int(r.get("extra_bed_count_per_room", 0)) > 0
                            for r in rooms_data
                        ),
                        extra_bed_rooms_count=sum(int(r.get("extra_bed_rooms_count", 0)) for r in rooms_data),
                        extra_bed_count_per_room=max(int(r.get("extra_bed_count_per_room", 0)) for r in rooms_data),
                        status=status,
                        payment_status="pending",
                        payment_method=payment_method,
                        booking_type=booking_type,
                        hours=hours if booking_type == "hourly" else 0,
                        booking_source=booking_source,
                    )

                    # Create BookingRoomItem records per room
                    for room in rooms_data:
                        rt = next((r for r in room_types if r.id == room["room_type"]), None)
                        number_of_rooms = int(room.get("number_of_rooms", 1))
                        extra_bed_rooms_count = int(room.get("extra_bed_rooms_count", 0))
                        extra_bed_count_per_room = int(room.get("extra_bed_count_per_room", 0))
                        extra_bed_requested = extra_bed_rooms_count > 0 and extra_bed_count_per_room > 0

                        base_price, room_price, taxes, service_fee, _, _, extra_bed_fee = calculate_booking_cost(
                            room_type=rt,
                            nights=total_nights,
                            rooms=number_of_rooms,
                            coupon=None,  # coupon applied globally, not per room
                            extra_bed_requested=extra_bed_requested,
                            extra_bed_rooms_count=extra_bed_rooms_count,
                            extra_bed_count_per_room=extra_bed_count_per_room,
                            booking_type=booking_type,
                            hours=hours
                        )

                        BookingRoomItem.objects.create(
                            booking=booking,
                            room_type=rt,
                            number_of_rooms=number_of_rooms,
                            extra_bed_rooms_count=extra_bed_rooms_count,
                            extra_bed_count_per_room=extra_bed_count_per_room,
                            base_price=base_price,
                            total_price=room_price + extra_bed_fee,
                            extra_bed_fee_total=extra_bed_fee,
                        )

                    # Block availability after booking
                    if booking_type == "hourly":
                        for room in rooms_data:
                            rt = next((r for r in room_types if r.id == room["room_type"]), None)
                            number_of_rooms = int(room.get("number_of_rooms", 1))
                            block_hourly_availability(rt, check_in, checkin_time, checkout_time, number_of_rooms)
                    else:
                        for room in rooms_data:
                            rt = next((r for r in room_types if r.id == room["room_type"]), None)
                            availability_qs = RoomAvailability.objects.filter(
                                room_type=rt,
                                date__in=date_range
                            )
                            for availability in availability_qs:
                                availability.available_rooms = max(availability.available_rooms - int(room.get("number_of_rooms", 1)), 0)
                                availability.save()

                    # CREATE BOOKING COUPON WITH INTEGRITY HANDLING
                    if coupon:
                        try:
                            BookingCoupon.objects.create(
                                booking=booking,
                                coupon=coupon,
                                discount_amount=discount_amount
                            )
                        except IntegrityError:
                            return Response({
                                "error": "Coupon was already used concurrently. Please try another coupon or booking."
                            }, status=400)
                    transaction.on_commit(invalidate_hotel_search_cache)
                        

            except Exception as e:
                logger.exception("Booking failed")
                return Response({"error": "Something went wrong. Please try again."}, status=500)


            # Cache invalidation
            room_parts = ','.join(f"{r['room_type']}:{r['number_of_rooms']}" for r in rooms_data)
            cache_key = f"precheckout_multi:{check_in}:{check_out}:{num_guests}:{coupon_code or ''}:{booking_type}:{hours}:{room_parts}"
            cache.delete(cache_key)

            guest_serializer = GuestSerializer(guest)
            # Fetch all BookingRoomItem for the created booking
            booking_room_items = BookingRoomItem.objects.filter(booking=booking)

            # Serialize room items
            room_items_serialized = BookingRoomItemSimpleSerializer(booking_room_items, many=True).data
          
#  Ensure checkin_time_str and checkout_time_str defined for response (even if None)
            checkin_time_str = booking_data.get("checkin_time") if booking_type == "hourly" else None
            checkout_time_str = booking_data.get("checkout_time") if booking_type == "hourly" else None

            return Response({
                "success": True,
                "data": {
                    "booking_id": booking.id,
                    "booking_number": booking.booking_number,
                    "hotel_name": hotel.name,
                    "check_in_date": str(booking.check_in_date),
                    "check_out_date": str(booking.check_out_date),
                    "number_of_adults": booking.number_of_adults,
                    "number_of_children": booking.number_of_children,
                    "number_of_guests": booking.number_of_guests,
                    "number_of_rooms": booking.number_of_rooms,
                    "hours": booking.hours,
                    "discount_amount": str(discount_amount),
                    "extra_bed_request": booking.extra_bed_request,
                    "extra_bed_rooms_count": booking.extra_bed_rooms_count,
                    "extra_bed_count_per_room": booking.extra_bed_count_per_room,
                    "checkin_time": checkin_time_str if booking_type == "hourly" else None,
                    "checkout_time": checkout_time_str if booking_type == "hourly" else None,
                    "taxes": str(total_taxes),
                    "fees": str(total_service_fee),
                    "total_price": str(booking.total_price),
                    "status": booking.status,
                    "payment_status": booking.payment_status,
                    "payment_method": booking.payment_method,
                    "booking_type": booking.booking_type,
                    "booking_source": booking.booking_source,
                    "guest": guest_serializer.data,
                      "rooms": room_items_serialized,
                    # Optionally add booking room item details serialized here
                    "cost_summary": {
                        "base_price_total": str(total_base_price),
                        "taxes": str(total_taxes),
                        "fees": str(total_service_fee),
                        "discount": str(discount_amount),
                        "total_price": str(total_price),
                    }
                },
            })

        
        # ------------------- Single-room booking flow  -------------------
        else:
            # Extract data for single room booking
            room_type_id = booking_data.get("room_type")
            rooms = int(booking_data.get("number_of_rooms", 1))

            # Validate guests and rooms again for single room booking
            try:
                guests_from_booking = int(booking_data.get("number_of_guests", num_guests))
                if extra_bed_requested and extra_bed_rooms_count > rooms:
                    return Response({"error": "extra_bed_rooms_count cannot be greater than number_of_rooms."}, status=400)
            except (TypeError, ValueError):
                return Response({"error": "Invalid guests or rooms count."}, status=400)

            # Get room type
            try:
                room_type = RoomType.objects.select_related("hotel").get(id=room_type_id)
            except RoomType.DoesNotExist:
                return Response({"error": "Invalid room_type."}, status=400)

            date_range = get_date_range(check_in, check_out)

            # Initialize cost variables to default values (safe fallback)
            total_taxes = Decimal("0.00")
            total_service_fee = Decimal("0.00")
            extra_bed_fee_total = Decimal("0.00")
            discount_amount = Decimal("0.00")
            total_price = Decimal("0.00")
            base_price = Decimal("0.00")
            total_room_price = Decimal("0.00")

            try:
                with transaction.atomic():
                    if booking_type == "daily":
                        # Lock RoomAvailability rows to prevent concurrent overselling
                        availability_qs = RoomAvailability.objects.select_for_update().filter(
                            room_type=room_type,
                            date__in=date_range
                        )

                        availability_map = {a.date: a for a in availability_qs}
                        for day in date_range:
                            availability = availability_map.get(day)
                            if not availability:
                                return Response({"error": f"No availability set for {day}. Please contact support."}, status=400)
                            if availability.available_rooms < rooms:
                                return Response({"error": f"Only {availability.available_rooms} rooms available on {day}, but {rooms} requested."}, status=400)

                    elif booking_type == "hourly":
                        # Check hourly availability before booking
                        is_available, error_msg = check_hourly_availability(
                            room_type, check_in, checkin_time, checkout_time, rooms
                        )
                        if not is_available:
                            return Response({"error": error_msg}, status=400)

                    # Validate coupon with row locking
                    coupon = None
                    if coupon_code:
                        coupon = Coupon.objects.select_for_update().filter(
                            code=coupon_code,
                            is_active=True,
                            valid_from__lte=now(),
                            valid_until__gte=now()
                        ).first()
                        if not coupon:
                            return Response({"error": "Invalid or expired coupon."}, status=400)

                        guest_email = guest.email.strip().lower()
                        already_used = BookingCoupon.objects.filter(
                            coupon=coupon,
                            booking__guest__email__iexact=guest_email,
                            booking__hotel=room_type.hotel
                        ).exists()

                        if already_used:
                            return Response({"error": "You have already used this coupon for this hotel."}, status=400)

                        discount_amount = coupon.discount_amount

                    # Calculate cost and assign to all relevant variables
                    base_price, total_room_price, total_taxes, total_service_fee, discount_amount, total_price, extra_bed_fee_total = calculate_booking_cost(
                        room_type,
                        nights=(check_out - check_in).days if booking_type == "daily" else 0,
                        rooms=rooms,
                        coupon=coupon,
                        extra_bed_requested=extra_bed_requested,
                        extra_bed_rooms_count=extra_bed_rooms_count,
                        extra_bed_count_per_room=extra_bed_count_per_room,
                        booking_type=booking_type,
                        hours=hours
                    )
                    total_nights = (check_out - check_in).days if booking_type == "daily" else 0

                    # Create booking
                    booking = Booking.objects.create(
                        guest=guest,
                        hotel=room_type.hotel,
                        room_type=room_type,
                        check_in_date=check_in,
                        check_out_date=check_out,
                        number_of_adults=num_adults,
                        number_of_children=num_children,
                        number_of_guests=num_guests,
                        number_of_rooms=rooms,
                        base_price=base_price,
                        total_nights=total_nights,
                        total_room_price=total_room_price,
                        taxes=total_taxes,
                        additional_fees=total_service_fee,
                        discount_amount=discount_amount,
                        total_price=total_price,
                        special_requests=booking_data.get("special_requests", ""),
                        estimated_arrival=booking_data.get("estimated_arrival"),
                        crib_request=booking_data.get("crib_request", False),
                        extra_bed_request=extra_bed_requested,
                        extra_bed_rooms_count=extra_bed_rooms_count,
                        extra_bed_count_per_room=extra_bed_count_per_room,
                        status=status,
                        payment_status="pending",
                        payment_method=payment_method,
                        booking_type=booking_type,
                        hours=hours if booking_type == "hourly" else None,
                        booking_source=booking_source,
                    )

                    # Block inventory AFTER booking creation inside transaction
                    if booking_type == "hourly":
                        block_hourly_availability(room_type, check_in, checkin_time, checkout_time, rooms)
                    else:
                        for availability in availability_qs:
                            availability.available_rooms = max(availability.available_rooms - rooms, 0)
                            availability.save()

                    # Create BookingCoupon if coupon applied
                    if coupon:
                        try:
                            BookingCoupon.objects.create(
                                booking=booking,
                                coupon=coupon,
                                discount_amount=discount_amount
                            )
                        except IntegrityError:
                            return Response({
                                "error": "Coupon was already used concurrently. Please try another coupon or booking."
                            }, status=400)
                    transaction.on_commit(invalidate_hotel_search_cache) 

            except Exception as e:
                # Any error rolls back the transaction and returns error response
                return Response({"error": str(e)}, status=400)

            # Cache invalidation after transaction
            cache_key = f"precheckout:{room_type.id}:{check_in}:{check_out}:{num_guests}:{rooms}:{coupon_code or ''}:{booking_type}:{hours}:{extra_bed_rooms_count}:{extra_bed_count_per_room}"
            cache.delete(cache_key)

            hourly_price = room_type.hourly_price or Decimal("0.00")
            total_hourly_price = hourly_price * (booking.hours or 0) if booking.booking_type == "hourly" else Decimal("0.00")

            guest_serializer = GuestSerializer(guest)

            return Response({
                "success": True,
                "data": {
                    "booking_id": booking.id,
                    "booking_number": booking.booking_number,
                    "hotel_name": booking.hotel.name,
                    "room_type_name": booking.room_type.name,
                    "check_in_date": str(booking.check_in_date),
                    "check_out_date": str(booking.check_out_date),
                    "number_of_adults": booking.number_of_adults,
                    "number_of_children": booking.number_of_children,
                    "number_of_guests": booking.number_of_guests,
                    "number_of_rooms": booking.number_of_rooms,
                    "hours": booking.hours,
                    "hourly_price": str(hourly_price) if booking.booking_type == "hourly" else None,
                    "total_hourly_price": str(total_hourly_price) if booking.booking_type == "hourly" else None,
                    "discount_amount": str(discount_amount),
                    "extra_bed_request": extra_bed_requested,
                    "extra_bed_rooms_count": extra_bed_rooms_count,
                    "extra_bed_count_per_room": extra_bed_count_per_room,
                    "extra_bed_fee_total": str(extra_bed_fee_total),
                    "checkin_time": booking_data.get("checkin_time"),
                    "checkout_time": booking_data.get("checkout_time"),
                    "taxes": str(total_taxes),
                    "fees": str(total_service_fee),
                    "total_price": str(booking.total_price),
                    "status": booking.status,
                    "payment_status": booking.payment_status,
                    "payment_method": booking.payment_method,
                    "booking_type": booking.booking_type,
                    "booking_source": booking.booking_source,
                    "guest": guest_serializer.data,
                },
            })



    # apply coupon
    @action(detail=True, methods=["post"], url_path="apply-coupon")
    def apply_coupon(self, request, pk=None):
        booking = self.get_object()
        code = request.data.get("code")

        if not code:
            return Response({"error": "Coupon code is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            coupon = Coupon.objects.get(
                code=code,
                is_active=True,
                valid_from__lte=timezone.now(),
                valid_until__gte=timezone.now()
            )
        except Coupon.DoesNotExist:
            return Response({"error": "Invalid or expired coupon."}, status=status.HTTP_400_BAD_REQUEST)

        guest_email = (booking.guest.email or "").strip().lower()

        if not guest_email:
            return Response({"error": "Guest email is required to apply this coupon."}, status=400)

        # ✅ Check if user (by email) already used this coupon for this hotel
        already_used = BookingCoupon.objects.filter(
            coupon=coupon,
            booking__guest__email__iexact=guest_email,
            booking__hotel=booking.hotel
        ).exists()

        if already_used:
            return Response({
                "error": "You have already used this coupon for this hotel."
            }, status=status.HTTP_400_BAD_REQUEST)

        #  Prevent duplicate coupon on same booking
        if BookingCoupon.objects.filter(booking=booking, coupon=coupon).exists():
            return Response({
                "error": "Coupon already applied to this booking."
            }, status=status.HTTP_400_BAD_REQUEST)

        #  Apply coupon
        BookingCoupon.objects.create(
            booking=booking,
            coupon=coupon,
            discount_amount=coupon.discount_amount
        )

        booking.discount_amount += coupon.discount_amount
        booking.save(update_fields=["discount_amount"])

        return Response({
            "success": True,
            "discount": str(coupon.discount_amount),
            "code": code
        })



    # cancel booking
    @action(detail=True, methods=["post"], url_path="cancel", permission_classes=[IsAuthenticated])
    def cancel_booking(self, request, pk=None):
        booking = get_object_or_404(Booking.objects.select_related("guest"), pk=pk)

        # --- ownership checks (same as yours) ---
        if booking.guest.user:
            if booking.guest.user != request.user:
                return Response({"error": "You can only cancel your own bookings."}, status=403)
        else:
            if booking.guest.email.lower() != request.user.email.lower():
                return Response({"error": "You can only cancel your own bookings."}, status=403)

        # --- state checks (same as yours) ---
        if booking.payment_status == "paid":
            return Response({"error": "Paid bookings cannot be cancelled."}, status=400)
        if booking.status in ["completed", "cancelled", "no_show"]:
            return Response({"error": f"Booking is already {booking.status} and cannot be cancelled."}, status=400)
        if not booking.is_cancelable():
            return Response({"error": "Booking cannot be cancelled."}, status=400)

        # --- cancel + RESTORE STOCK atomically ---
        with transaction.atomic():
            if booking.booking_type == "hourly":
                # TODO: implement reverse of your block_hourly_availability()
                # unblock_hourly_availability(booking, ...)
                pass
            else:
                # DAILY: restore for every day in the stay
                dates = get_date_range(booking.check_in_date, booking.check_out_date)

                if booking.room_type_id:
                    # single-room booking
                    (RoomAvailability.objects
                        .select_for_update()
                        .filter(room_type=booking.room_type, date__in=dates)
                        .update(available_rooms=F("available_rooms") + booking.number_of_rooms))
                else:
                    # multi-room booking: restore per item
                    items = BookingRoomItem.objects.select_related("room_type").filter(booking=booking)
                    for item in items:
                        (RoomAvailability.objects
                            .select_for_update()
                            .filter(room_type=item.room_type, date__in=dates)
                            .update(available_rooms=F("available_rooms") + item.number_of_rooms))

            # mark booking cancelled
            booking.status = "cancelled"
            booking.cancellation_date = timezone.now()
            booking.cancellation_reason = request.data.get("reason", "")
            booking.save(update_fields=["status", "cancellation_date", "cancellation_reason"])

            # make search show updated availability on the very next call
            transaction.on_commit(invalidate_hotel_search_cache)

        return Response({"success": True, "message": "Booking cancelled and rooms restored."})


    # viewing own booking list
    @action(detail=False, methods=["get"], url_path="my", permission_classes=[IsAuthenticated])
    def my_bookings(self, request):
        from django.db.models import Q

        status_filter = request.query_params.get("status")
        bookings = Booking.objects.filter(
            Q(guest__user=request.user) | Q(guest__email__iexact=request.user.email)
        ).select_related("room_type", "hotel").order_by("-created_at")  # <- add ordering here

        if status_filter:
            bookings = bookings.filter(status=status_filter)

        paginator = StandardResultsSetPagination()
        page = paginator.paginate_queryset(bookings, request)
        serializer = BookingDetailSerializer(page, many=True, context={"request": request})

        return paginator.get_paginated_response(serializer.data)


    @action(detail=True, methods=["post"], url_path="mark-no-show", permission_classes=[IsAuthenticated, IsHotelOwner])
    def mark_no_show(self, request, pk=None):
        booking = get_object_or_404(Booking, pk=pk)

        if not IsHotelOwnerOfBooking().has_object_permission(request, self, booking):
            return Response({"error": "You can only mark your own hotel's bookings as no-show."}, status=403)

        if date.today() < booking.check_in_date:
            return Response({"error": "Cannot mark no-show before check-in date."}, status=400)

        if booking.status in ["cancelled", "completed", "no_show"]:
            return Response({"error": f"This booking is already marked as {booking.status}."}, status=400)

        booking.status = "no_show"
        booking.save()
        return Response({"success": True, "message": "Booking marked as no-show."})
    
    
    @action(detail=True, methods=["post"], url_path="confirm", permission_classes=[IsAuthenticated, IsHotelOwner])
    def confirm_booking(self, request, pk=None):
        booking = get_object_or_404(Booking, pk=pk)

        if not IsHotelOwnerOfBooking().has_object_permission(request, self, booking):
            return Response({"error": "You can only confirm bookings for your own hotel."}, status=403)

        if booking.status != 'pending':
            return Response({"error": "Booking is already processed."}, status=400)

        checkin = booking.check_in_date
        checkout = booking.check_out_date
        date_range = get_date_range(checkin, checkout)

        #  Preload all availability rows for the date range
        availability_map = {
            a.date: a for a in RoomAvailability.objects.filter(
                room_type=booking.room_type,
                date__in=date_range
            )
        }

        #  Check availability in memory
        for day in date_range:
            availability = availability_map.get(day)
            if not availability:
                return Response({
                    "error": f"No availability set for {day}. Please update availability first."
                }, status=400)

            if availability.available_rooms < booking.number_of_rooms:
                return Response({
                    "error": f"Only {availability.available_rooms} rooms available on {day}, but {booking.number_of_rooms} requested."
                }, status=400)

        # All checks passed → update availability
        for day in date_range:
            availability = availability_map[day]
            availability.available_rooms -= booking.number_of_rooms
            availability.save()

        booking.status = "confirmed"
        booking.save()

         # Trigger async booking confirmation notification
        send_booking_confirmation_message.delay(booking.id)

        return Response({"success": True, "message": "Booking confirmed and availability updated."})
    

    @action(detail=True, methods=["patch"], url_path="manual-checkin")
    def manual_checkin(self, request, pk=None):

        print("Booking ID:", pk)
        print("User:", request.user)
        

        try:
            booking = get_object_or_404(Booking.objects.select_related("hotel"), pk=pk)
            print("Booking Hotel ID:", booking.hotel_id)
            print("Hotel Owner ID:", getattr(booking.hotel.owner, "id", None))
            print("Request User ID:", request.user.id)


            print("Booking found. Hotel owner:", booking.hotel.owner)
        except Booking.DoesNotExist:
            return Response({"error": "Booking not found."}, status=404)

        if booking.hotel.owner != request.user:
            return Response({"error": "Permission denied. You are not the owner of this booking."}, status=403)

        if booking.status != "confirmed":
            return Response({"error": "Only confirmed bookings can be checked in"}, status=400)

        booking.status = "checked_in"
        booking.checked_in_at = timezone.now()
        booking.save()

        return Response({"success": True, "message": "Guest manually checked in."})



     #  mark_completed action 
    @action(detail=True, methods=["post"], url_path="mark-completed", permission_classes=[IsAuthenticated, IsHotelOwner])
    def mark_completed(self, request, pk=None):
        booking = get_object_or_404(Booking, pk=pk)
        
        if not IsHotelOwnerOfBooking().has_object_permission(request, self, booking):
            return Response({"error": "You can only mark your own hotel's bookings as completed."}, status=403)
        
        if booking.status != "confirmed":
            return Response({"error": "Only confirmed bookings can be marked as completed."}, status=400)
        
        if booking.check_out_date > date.today():
            return Response({"error": "Cannot mark as completed before the check-out date."}, status=400)

        booking.status = "completed"
        booking.save(update_fields=["status"])

        return Response({"success": True, "message": "Booking marked as completed."})



class BaseBookingPayViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Booking.objects.select_related("guest").all()
    permission_classes = [IsAuthenticated,IsHotelOwner]  # Optional: add IsHotelOwner if needed
    # pagination_class = PageNumberPagination

    @action(detail=True, methods=["post"], url_path="pay")
    def pay(self, request, pk=None):
        booking = self.get_object()

        if booking.status not in ["confirmed", "checked_in"]:
            return Response({"error": "You can only pay for bookings that are confirmed."}, status=400)

        if booking.payment_status == "paid":
            return Response({"error": "This booking is already fully paid."}, status=400)
        
        serializer = BookingPaymentSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        data = serializer.validated_data
        amount = data.get("amount")
        payment_type = data.get("payment_type")

        # Prevent overpayment
        total_paid = sum(p.amount for p in booking.payments.filter(status="completed"))
        if total_paid + amount > booking.total_price:
            return Response({
                "error": f"Overpayment detected. You already paid {total_paid:.2f}, total price is {booking.total_price:.2f}."
            }, status=400)

        # Prevent duplicate full payment
        if payment_type == "full":
            full_payment_exists = booking.payments.filter(payment_type="full", status="completed").exists()
            if full_payment_exists:
                return Response({"error": "Full payment already exists."}, status=400)

        transaction_id = str(uuid.uuid4()).replace("-", "")[:12]

        #  Save the payment
        payment = serializer.save(
            booking=booking,
            processed_by=request.user if request.user.is_authenticated else None,
            transaction_id=transaction_id,
            status='completed'  # Mark as completed by default
        )
        total_paid += amount
        if total_paid >= booking.total_price:
            booking.payment_status = "paid"
            send_payment_confirmation_message.delay(booking.id)
            booking.save(update_fields=["payment_status"])

        return Response({
            "success": True,
            "payment_id": payment.id,
            "booking_status": booking.payment_status,
            "transaction_id": payment.transaction_id
        })




# get search suggestion

class BaseSearchSuggestionView(APIView):
    def get(self, request):
        query = request.query_params.get('q', '').strip()
        user = request.user if request.user.is_authenticated else None
        session_id = request.session.session_key

        # Recent searches filter for this user/session
        recent_filters = Q()
        if user:
            recent_filters |= Q(user=user)
        if session_id:
            recent_filters |= Q(session_id=session_id)

        recent_searches_qs = HotelSearchHistory.objects.filter(recent_filters).values('city').distinct()[:5]
        recent_cities = [item['city'] for item in recent_searches_qs]

        if query:
            # Return autocomplete suggestions for cities and hotel names containing query
            # Let's fetch city suggestions from SearchSuggestion model (could be city names)
            city_suggestions = SearchSuggestion.objects.filter(keyword__icontains=query).order_by('keyword')[:10]

            # Additionally, you can implement hotel name search here (optional)
            # For demo, only city suggestions returned

            suggestion_list = [s.keyword for s in city_suggestions]

            return Response({
                "recent_searches": recent_cities,
                "suggestions": suggestion_list,
            })

        else:
            # No query = return default/popular suggestions + recent searches
            default_suggestions = SearchSuggestion.objects.filter(is_default=True).order_by('keyword')[:10]
            default_list = [s.keyword for s in default_suggestions]

            return Response({
                "recent_searches": recent_cities,
                "suggestions": default_list,
            })



# saved the search history and give suggestion

def get_session_key(request):
    session_key = request.session.session_key
    if not session_key:
        request.session.create()
        session_key = request.session.session_key
    return session_key

        
class BaseSaveSearchHistoryView(APIView):
    def post(self, request):
        user = request.user if request.user.is_authenticated else None
        session_id = get_session_key(request)  # Ensure session key exists

        data = request.data
        city = data.get('city')
        checkin = data.get('checkin')
        checkout = data.get('checkout')
        adults = data.get('adults', 1)
        children = data.get('children', 0)
        rooms = data.get('rooms', 1)

        if not city or not checkin or not checkout:
            return Response({"error": "city, checkin and checkout are required."}, status=400)

        HotelSearchHistory.objects.create(
            user=user,
            session_id=session_id,
            city=city,
            checkin=checkin,
            checkout=checkout,
            adults=adults,
            children=children,
            rooms=rooms,
        )
        return Response({"message": "Search history saved."}, status=status.HTTP_201_CREATED)

    

# save search history
class BaseRecentSearchesView(APIView):
    permission_classes = []  # public access; add auth if needed

    def get(self, request, *args, **kwargs):
        user = request.user if request.user.is_authenticated else None
        session_id = get_session_key(request)  # Ensure session key exists

        if user:
            queryset = HotelSearchHistory.objects.filter(user=user)
        elif session_id:
            queryset = HotelSearchHistory.objects.filter(session_id=session_id)
        else:
            queryset = HotelSearchHistory.objects.none()

        queryset = queryset.order_by('-created_at')[:10]

        serializer = HotelSearchHistorySerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)



class BaseSavedHotelViewSet(viewsets.ModelViewSet):
    serializer_class = SavedHotelSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return SavedHotel.objects.filter(user=self.request.user)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        hotel = serializer.validated_data['hotel']

        instance, created = SavedHotel.objects.get_or_create(user=request.user, hotel=hotel)
        serializer = self.get_serializer(instance)

        status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return Response(serializer.data, status=status_code)



class BaseNearbyPlaceViewSet(viewsets.ModelViewSet):
    serializer_class = NearbyPlaceSerializer
    queryset = NearbyPlace.objects.all()
    permission_classes = [AllowAny]




# OMS


class BaseHotelOwnerViewSet(viewsets.ModelViewSet):
    """
    Hotel Admins: full access.
    Hotel Owners: can only view and update their own hotels.
    """
    serializer_class = HotelManageSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = HotelStandardResultsSetPagination

    def get_queryset(self):
        qs = Hotel.objects.select_related("owner").prefetch_related(
            "images",
            "room_types__availability"
        )
        user = self.request.user
        if user.is_staff and user.hotel_admin:
            # Hotel admin sees all hotels
            return qs
        # Hotel owner sees only their hotels
        return qs.filter(owner=user)

    def create(self, request, *args, **kwargs):
        user = request.user

        if not (user.is_staff and user.hotel_admin):
            # Hotel Owners cannot create hotels
            raise PermissionDenied("You do not have permission to create hotels.")

        return super().create(request, *args, **kwargs)

    def perform_create(self, serializer):
        """
        Hotel admin must specify an owner.
        """
        owner = serializer.validated_data.get("owner")
        if not owner:
            raise ValidationError({"owner": "This field is required."})
        if owner.role != owner.RoleType.HOTEL_OWNER:
            raise ValidationError({"owner": "Selected user must be a Hotel Owner."})

        try:
            serializer.save()
        except IntegrityError:
            raise ValidationError("You already have a hotel with this name and address.")

    def perform_update(self, serializer):
        """
        Hotel admins can update any hotel.
        Hotel owners can update only their own hotels.
        """
        instance = self.get_object()
        user = self.request.user

        if user.is_staff and user.hotel_admin:
            serializer.save()
        else:
            if instance.owner != user:
                raise PermissionDenied("You cannot update this hotel.")
            serializer.save()

    def destroy(self, request, *args, **kwargs):
        user = self.request.user
        instance = self.get_object()

        if not (user.is_staff and user.hotel_admin):
            # Hotel owners cannot delete hotels
            raise PermissionDenied("You do not have permission to delete hotels.")

        return super().destroy(request, *args, **kwargs)




class BaseRoomTypeOwnerViewSet(viewsets.ModelViewSet):
    serializer_class = RoomTypeBaseSerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    pagination_class = StandardResultsSetPagination
    parser_classes = (MultiPartParser, FormParser,JSONParser)  

    def get_queryset(self):
        return RoomType.objects.select_related("hotel").prefetch_related("availability").filter(hotel__owner=self.request.user)

    def perform_create(self, serializer):
        hotel_id = self.request.data.get("hotel")
        if not hotel_id:
            raise ValidationError({"hotel": "Hotel ID is required for creating a room type."})
        hotel = get_object_or_404(Hotel, id=hotel_id, owner=self.request.user)
        serializer.save(hotel=hotel)

    def perform_update(self, serializer):
        instance = self.get_object()
        if instance.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot update this room type.")
        serializer.save()

    def perform_destroy(self, instance):
        if instance.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot delete this room type.")
        instance.delete()

    @action(detail=True, methods=["get"], url_path="availability-summary")
    def availability_summary(self, request, pk=None):
        try:
            start_str = request.query_params.get("start")
            end_str = request.query_params.get("end")

            # Require start param for clarity, fallback to today if missing
            start_date = date.fromisoformat(start_str) if start_str else date.today()
            end_date = date.fromisoformat(end_str) if end_str else start_date + timedelta(days=30)

            if end_date < start_date:
                return Response({"error": "end date cannot be before start date."}, status=400)

        except ValueError:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

        checkin_time_str = request.query_params.get("checkin_time")
        checkout_time_str = request.query_params.get("checkout_time")

        room_type = get_object_or_404(RoomType, pk=pk, hotel__owner=request.user)

        if room_type.is_hourly and checkin_time_str and checkout_time_str:
            # Parse times and validate
            try:
                checkin_time = datetime.strptime(checkin_time_str, "%H:%M").time()
                checkout_time = datetime.strptime(checkout_time_str, "%H:%M").time()
                if checkout_time <= checkin_time:
                    return Response({"error": "checkout_time must be after checkin_time."}, status=400)
            except ValueError:
                return Response({"error": "Invalid time format. Use HH:MM."}, status=400)

            date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

            availability_summary = []
            for single_date in date_range:
                slots = RoomHourlyAvailability.objects.filter(
                    room_type=room_type,
                    date=single_date,
                    start_time__lt=checkout_time,
                    end_time__gt=checkin_time
                )

                min_available = min((slot.available_rooms for slot in slots), default=0)

                if min_available == 0:
                    status = "booked"
                    priority = 1
                elif min_available <= 3:
                    status = "reserved"
                    priority = 2
                else:
                    status = "available"
                    priority = 6

                availability_summary.append({
                    "date": single_date.isoformat(),
                    "available_rooms": min_available,
                    "total_rooms": room_type.total_rooms,
                    "is_fully_booked": min_available == 0,
                    "status": status,
                    "priority": priority,
                    "checkin_time": checkin_time_str,
                    "checkout_time": checkout_time_str,
                })

            return Response({
                "room_type_id": pk,
                "availability": availability_summary,
                "supports_hourly_booking": True,
                "hourly_price": str(room_type.hourly_price) if room_type.hourly_price else None,
            })

        else:
            availability_qs = RoomAvailability.objects.filter(
                room_type=room_type,
                date__range=(start_date, end_date)
            ).order_by("date")

            summary = []
            for day in availability_qs:
                if day.available_rooms == 0:
                    status = "booked"
                    priority = 1
                elif day.available_rooms <= 3:
                    status = "reserved"
                    priority = 2
                elif day.is_special_offer:
                    status = "pending"
                    priority = 3
                else:
                    status = "available"
                    priority = 6

                summary.append({
                    "id": day.id,
                    "date": str(day.date),
                    "available_rooms": day.available_rooms,
                    "total_rooms": day.room_type.total_rooms,
                    "is_fully_booked": day.available_rooms == 0,
                    "price_override": str(day.price_override) if day.price_override is not None else None,
                    "original_price": str(day.room_type.price_per_night),
                    "is_special_offer": day.is_special_offer,
                    "special_offer_name": day.special_offer_name,
                    "min_nights_stay": day.min_nights_stay,
                    "status": status,
                    "priority": priority
                })

            return Response({
                "room_type_id": pk,
                "availability": summary,
                "supports_hourly_booking": room_type.is_hourly,
                "hourly_price": str(room_type.hourly_price) if room_type.hourly_price else None,
            })


    @action(detail=True, methods=["post"], url_path="bulk-inventory-upload")
    def bulk_inventory_upload(self, request, pk=None):
        room_type = self.get_object()
        if room_type.hotel.owner != request.user:
            return Response({"detail": "You do not own this room type."}, status=403)

        csv_file = request.FILES.get("file")
        if not csv_file:
            return Response({"detail": "CSV file is required."}, status=400)

        # Validate CSV BEFORE saving/uploading
        errors = validate_bulk_inventory_csv(csv_file, room_type)
        if errors:
            return Response({"success": 0, "errors": errors, "message": "File validation failed."}, status=400)

        # Save file properly
        csv_file.seek(0)
        unique_name = f"bulk_inventory/{uuid.uuid4()}.csv"
        saved_path = default_storage.save(unique_name, ContentFile(csv_file.read()))

        # Trigger async Celery task
        task = process_bulk_inventory_csv_task.delay(room_type.id, saved_path)

        return Response({
            "task_id": task.id,
            "message": "File accepted and processing started."
        }, status=status.HTTP_202_ACCEPTED)
    
    
    
        # hotel all  room availability
    
    def process_availability(self, day, bookings, available_rooms, is_hourly=False, min_available=0):
        priority_map = {
            "checked_in": 1,
            "reserved": 2,
            "booked": 3,
            "available": 4,
        }

        checked_in_count = bookings.filter(status="checked_in").count()
        confirmed_count = bookings.filter(status="confirmed").count()
        booked_rooms = checked_in_count + confirmed_count

        if checked_in_count:
            status = "checked_in"
        elif confirmed_count:
            status = "reserved"
        elif (min_available == 0 if is_hourly else available_rooms == 0):
            status = "booked"
        else:
            status = "available"

        return {
            "date": day.isoformat(),
            "available_rooms": min_available if is_hourly else available_rooms,
            "booked_rooms": booked_rooms,
            "is_fully_booked": (min_available == 0 if is_hourly else available_rooms == 0),
            "status": status,
            "priority": priority_map[status],
        }

    @action(detail=True, methods=["get"], url_path="room-availability")
    def room_availability(self, request, pk=None):
        if pk is None:
            hotels = Hotel.objects.filter(owner=request.user)
            if hotels.count() == 1:
                hotel = hotels.first()
            else:
                return Response({"error": "Multiple hotels found. Specify hotel ID."}, status=400)
        else:
            hotel = get_object_or_404(Hotel, pk=pk, owner=request.user)

        start_date_str = request.query_params.get("start_date")
        end_date_str = request.query_params.get("end_date")
        checkin_time_str = request.query_params.get("checkin_time")
        checkout_time_str = request.query_params.get("checkout_time")
        booking_type = request.query_params.get("booking_type", "daily")
        rooms_requested = int(request.query_params.get("number_of_rooms", 1))

        if not start_date_str:
            return Response({"error": "start_date is required."}, status=400)
        if not end_date_str:
            end_date_str = start_date_str

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date < start_date:
                return Response({"error": "end_date cannot be before start_date."}, status=400)
        except ValueError:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

        if booking_type == "hourly":
            if not checkin_time_str or not checkout_time_str:
                return Response({"error": "checkin_time and checkout_time are required for hourly booking."}, status=400)
            try:
                checkin_time = datetime.strptime(checkin_time_str, "%H:%M").time()
                checkout_time = datetime.strptime(checkout_time_str, "%H:%M").time()
                if checkout_time <= checkin_time:
                    return Response({"error": "checkout_time must be after checkin_time."}, status=400)
            except ValueError:
                return Response({"error": "Invalid time format. Use HH:MM."}, status=400)
        else:
            checkin_time = checkout_time = None

        date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        room_types = RoomType.objects.filter(hotel=hotel).order_by("name")
        result = []

        for room_type in room_types:
            availability_list = []

            if booking_type == "hourly" and room_type.is_hourly:
                for day in date_range:
                    slots = RoomHourlyAvailability.objects.filter(
                        room_type=room_type,
                        date=day,
                        start_time__lt=checkout_time,
                        end_time__gt=checkin_time
                    )
                    min_available = min((s.available_rooms for s in slots), default=0)

                    bookings = Booking.objects.filter(
                        hotel=hotel,
                        room_type=room_type,
                        booking_type="hourly",
                        check_in_date=day,
                        check_in_time__lte=checkout_time,
                        check_out_time__gte=checkin_time
                    )

                    availability_list.append(
                        self.process_availability(
                            day=day,
                            bookings=bookings,
                            available_rooms=0,  # not used in hourly
                            is_hourly=True,
                            min_available=min_available
                        )
                    )

            else:
                availability_qs = RoomAvailability.objects.filter(room_type=room_type, date__in=date_range)
                availability_map = {a.date: a.available_rooms for a in availability_qs}

                for day in date_range:
                    available_rooms = availability_map.get(day, 0)
                    bookings = Booking.objects.filter(
                        hotel=hotel,
                        room_type=room_type,
                        check_in_date__lte=day,
                        check_out_date__gt=day
                    )
                    availability_list.append(
                        self.process_availability(
                            day=day,
                            bookings=bookings,
                            available_rooms=available_rooms,
                            is_hourly=False
                        )
                    )

            result.append({
                "room_type_id": room_type.id,
                "room_type_name": room_type.name,
                "total_rooms": room_type.total_rooms,
                "availability": availability_list,
                "supports_hourly_booking": room_type.is_hourly,
                "hourly_price": str(room_type.hourly_price) if room_type.is_hourly else None,
                "requested_checkin_time": checkin_time_str if booking_type == "hourly" else None,
                "requested_checkout_time": checkout_time_str if booking_type == "hourly" else None,
                "is_available_for_requested_time": None
            })

        return Response({
            "hotel_id": hotel.id,
            "hotel_name": hotel.name,
            "hotel_address":hotel.address,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "booking_type": booking_type,
            "room_availability": result
        })





class BaseAvailabilityOwnerViewSet(viewsets.ModelViewSet):
    serializer_class = RoomAvailabilitySerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return RoomAvailability.objects.select_related("room_type", "room_type__hotel").filter(
            room_type__hotel__owner=self.request.user
        )

    def perform_create(self, serializer):
        room_type_id = self.request.data.get("room_type")
        date = self.request.data.get("date")

        if not room_type_id or not date:
            raise ValidationError({"detail": "Both 'room_type' and 'date' are required."})

        room_type = get_object_or_404(RoomType, id=room_type_id)

        if room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You do not own this room type.")

        # Check for duplicates
        if RoomAvailability.objects.filter(room_type=room_type, date=date).exists():
            raise ValidationError({"detail": f"Availability already exists for {date}."})

        serializer.save(room_type=room_type)

    def perform_update(self, serializer):
        instance = self.get_object()
        if instance.room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot update this availability.")
        serializer.save()

    def perform_destroy(self, instance):
        if instance.room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot delete this availability.")
        instance.delete()


class BaseHourlyAvailabilityOwnerViewSet(viewsets.ModelViewSet):
    serializer_class = RoomHourlyAvailabilitySerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        # Only availability records for hotels owned by the user
        return RoomHourlyAvailability.objects.select_related(
            "room_type", "room_type__hotel"
        ).filter(room_type__hotel__owner=self.request.user)

    def perform_create(self, serializer):
        room_type_id = self.request.data.get("room_type")
        date = self.request.data.get("date")
        start_time = self.request.data.get("start_time")
        end_time = self.request.data.get("end_time")

        # Validate required fields
        if not all([room_type_id, date, start_time, end_time]):
            raise ValidationError({"detail": "room_type, date, start_time and end_time are required."})

        room_type = get_object_or_404(RoomType, id=room_type_id)

        # Owner check
        if room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You do not own this room type.")

        # Check for duplicate slot (unique_together enforcement)
        if RoomHourlyAvailability.objects.filter(
            room_type=room_type, date=date, start_time=start_time, end_time=end_time
        ).exists():
            raise ValidationError({"detail": f"Hourly availability already exists for {date} {start_time}-{end_time}."})

        serializer.save(room_type=room_type)

    def perform_update(self, serializer):
        instance = self.get_object()
        if instance.room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot update this availability.")
        serializer.save()

    def perform_destroy(self, instance):
        if instance.room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You cannot delete this availability.")
        instance.delete()



class BaseBookingOwnerViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = BookingDetailSerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        queryset = Booking.objects.select_related("guest", "room_type", "hotel").filter(
            hotel__owner=self.request.user
        ).order_by("-created_at")

        start = self.request.query_params.get("start")
        end = self.request.query_params.get("end")
        status = self.request.query_params.get("status")
        payment_status = self.request.query_params.get("payment_status")

        # Validate and apply start date filter
        if start:
            start_date = parse_date(start)
            if start_date:
                queryset = queryset.filter(created_at__date__gte=start_date)

        # Validate and apply end date filter
        if end:
            end_date = parse_date(end)
            if end_date:
                queryset = queryset.filter(created_at__date__lte=end_date)

        # Validate booking status filter
        valid_statuses = [choice[0] for choice in Booking.STATUS_CHOICES]
        if status and status in valid_statuses:
            queryset = queryset.filter(status=status)
        elif status:
            queryset = queryset.none()  # invalid status value => empty queryset

        # Validate payment status filter
        valid_payment_statuses = [choice[0] for choice in Booking.PAYMENT_STATUS_CHOICES]
        if payment_status and payment_status in valid_payment_statuses:
            queryset = queryset.filter(payment_status=payment_status)
        elif payment_status:
            queryset = queryset.none()  # invalid payment status => empty queryset

        return queryset


from django.utils import timezone
from datetime import datetime, time

class BasePaymentOwnerViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = BookingPaymentSerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    pagination_class = StandardResultsSetPagination

    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = ['status']  # assuming BookingPayment has a status field
   

    def get_queryset(self):
        queryset = BookingPayment.objects.select_related("booking", "booking__hotel").filter(
            booking__hotel__owner=self.request.user
        ).order_by("-payment_date")

        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')

        def make_aware_datetime(date_obj, start=True):
            if start:
                dt = datetime.combine(date_obj, time.min)  # 00:00:00
            else:
                dt = datetime.combine(date_obj, time.max)  # 23:59:59.999999
            return timezone.make_aware(dt, timezone.get_current_timezone())

        if start_date and end_date:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            if start_date_parsed and end_date_parsed:
                start_dt = make_aware_datetime(start_date_parsed, start=True)
                end_dt = make_aware_datetime(end_date_parsed, start=False)
                queryset = queryset.filter(payment_date__range=[start_dt, end_dt])

        return queryset
    
    @action(detail=True, methods=["patch"], url_path="update-status")
    def update_status(self, request, pk=None):
        payment = self.get_object()
        new_status = request.data.get("status")

        valid_statuses = dict(BookingPayment.PAYMENT_STATUS_CHOICES).keys()

        if new_status not in valid_statuses:
            return Response(
                {"error": f"Invalid status. Choose from: {', '.join(valid_statuses)}"},
                status=drf_status.HTTP_400_BAD_REQUEST
            )

        # Optional: Prevent refund before completion
        if new_status == "refunded" and payment.status != "completed":
            return Response(
                {"error": "Only completed payments can be refunded."},
                status=drf_status.HTTP_400_BAD_REQUEST
            )

        payment.status = new_status
        payment.processed_by = request.user
        payment.save()

        return Response({
            "success": True,
            "message": f"Payment status updated to '{new_status}'.",
            "payment_id": payment.id,
            "booking_id": payment.booking.id,
            "booking_payment_status": payment.booking.payment_status
        })






class BaseHotelPolicyOwnerViewSet(viewsets.ModelViewSet):
    serializer_class = HotelPolicySerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    queryset = HotelPolicy.objects.select_related("hotel")

    def get_queryset(self):
        return HotelPolicy.objects.filter(hotel__owner=self.request.user)

    def perform_create(self, serializer):
        hotel = serializer.validated_data["hotel"]
        if hotel.owner != self.request.user:
            raise PermissionDenied("You can only add policies to your own hotels.")
        serializer.save()

    def perform_update(self, serializer):
        if serializer.instance.hotel.owner != self.request.user:
            raise PermissionDenied("You can only update your own hotel's policy.")
        serializer.save()



class BaseHotelImageViewSet(viewsets.ModelViewSet):
    serializer_class = HotelImageSerializer
    permission_classes = [IsAuthenticated, IsHotelOwnerOrHotelAdmin]
    parser_classes = [MultiPartParser]  # Enable multipart uploads

    def get_queryset(self):
        """
        Hotel Owner: See own hotel images
        Hotel Admin: See all images
        """
        user = self.request.user
        qs = HotelImage.objects.all()
        if user.is_staff and user.hotel_admin:
            return qs
        return qs.filter(hotel__owner=user)

    def perform_create(self, serializer):
        hotel_id = self.request.data.get("hotel")
        if not hotel_id:
            raise ValidationError({"hotel": "Hotel ID is required."})
        hotel = get_object_or_404(Hotel, id=hotel_id)
        user = self.request.user
        if not (
            (hotel.owner == user)
            or (user.is_staff and user.hotel_admin)
        ):
            raise PermissionDenied("You do not have permission to upload images for this hotel.")
        serializer.save(hotel=hotel)

    def perform_destroy(self, instance):
        user = self.request.user
        if not (
            (instance.hotel.owner == user)
            or (user.is_staff and user.hotel_admin)
        ):
            raise PermissionDenied("You do not have permission to delete this hotel image.")
        instance.delete()

    @action(detail=False, methods=["post"], url_path="bulk-upload", parser_classes=[MultiPartParser])
    def bulk_upload(self, request):
        hotel_id = request.data.get("hotel")
        if not hotel_id:
            return Response({"error": "hotel is required."}, status=400)

        hotel = get_object_or_404(Hotel, id=hotel_id)
        user = request.user
        if not (
            (hotel.owner == user)
            or (user.is_staff and user.hotel_admin)
        ):
            raise PermissionDenied("You do not have permission to upload images for this hotel.")

        images = request.FILES.getlist("images")
        if not images:
            return Response({"error": "No images uploaded."}, status=400)

        # Optional metadata input
        image_data_map = {}
        if "image_data" in request.data:
            try:
                image_data_list = json.loads(request.data["image_data"])
                for item in image_data_list:
                    image_data_map[item["filename"]] = item
            except Exception:
                return Response({"error": "Invalid JSON format for image_data."}, status=400)

        created_images = []
        for image_file in images:
            meta = image_data_map.get(image_file.name, {})

            image = HotelImage.objects.create(
                hotel=hotel,
                image=image_file,
                image_type=meta.get("image_type", "exterior"),
                title=meta.get("title"),
                caption=meta.get("caption"),
                alt_text=meta.get("alt_text"),
                is_primary=meta.get("is_primary", False),
                display_order=meta.get("display_order", 0),
            )
            created_images.append(HotelImageSerializer(image).data)

        return Response({"uploaded": created_images}, status=201)
    

# upload room single or multiple image
class BaseRoomImageViewSet(viewsets.ModelViewSet):
    serializer_class = RoomImageSerializer
    permission_classes = [IsAuthenticated, IsHotelOwner]
    parser_classes = [MultiPartParser]  # Required for multiple file uploads

    def get_queryset(self):
        return RoomImage.objects.filter(room_type__hotel__owner=self.request.user)

    def perform_create(self, serializer):
        room_type_id = self.request.data.get("room_type")
        if not room_type_id:
            raise ValidationError({"room_type": "RoomType ID is required."})
        room_type = get_object_or_404(RoomType, id=room_type_id)
        if room_type.hotel.owner != self.request.user:
            raise PermissionDenied("You do not own this room type.")
        serializer.save(room_type=room_type)

    @action(detail=False, methods=["post"], url_path="bulk-upload", parser_classes=[MultiPartParser])
    def bulk_upload(self, request):
        room_type_id = request.data.get("room_type")
        if not room_type_id:
            return Response({"error": "room_type is required"}, status=400)

        room_type = get_object_or_404(RoomType, id=room_type_id)
        if room_type.hotel.owner != request.user:
            raise PermissionDenied("You do not own this room type.")

        images = request.FILES.getlist("images")
        if not images:
            return Response({"error": "No images uploaded."}, status=400)

        # Optional metadata input
        image_data_map = {}
        if "image_data" in request.data:
            try:
                image_data_list = json.loads(request.data["image_data"])
                for item in image_data_list:
                    image_data_map[item["filename"]] = item
            except Exception:
                return Response({"error": "Invalid JSON format for image_data."}, status=400)

        created_images = []
        for image_file in images:
            meta = image_data_map.get(image_file.name, {})

            image = RoomImage.objects.create(
                room_type=room_type,
                image=image_file,
                image_type=meta.get("image_type", "room_view"),
                title=meta.get("title"),
                caption=meta.get("caption"),
                alt_text=meta.get("alt_text"),
                is_primary=meta.get("is_primary", False),
                display_order=meta.get("display_order", 0),
            )
            created_images.append(RoomImageSerializer(image).data)

        return Response({"uploaded": created_images}, status=201)


class BaseDashboardViewSet(viewsets.ViewSet):
    permission_classes = [IsAuthenticated, IsHotelOwner]

    @action(detail=False, methods=["get"], url_path="overview")
    def overview(self, request):
        user = request.user
        bd_tz = pytz.timezone("Asia/Dhaka")
        today = now().astimezone(bd_tz).date()

        # Parse date filters from query parameters
        custom_date = request.query_params.get("date")  # yyyy-mm-dd
        month_param = request.query_params.get("month")  # yyyy-mm
        start_param = request.query_params.get("start")  # yyyy-mm-dd
        end_param = request.query_params.get("end")      # yyyy-mm-dd

        # Determine date range
        if start_param and end_param:
            try:
                start_of_period = make_aware(datetime.strptime(start_param, "%Y-%m-%d"), bd_tz)
                end_of_period = make_aware(datetime.strptime(end_param, "%Y-%m-%d"), bd_tz) + timedelta(days=1)
                view_type = "range"
                date_label = f"{start_param} to {end_param}"
            except ValueError:
                return Response({"error": "Invalid start or end date format. Use YYYY-MM-DD."}, status=400)

        elif month_param:
            try:
                year, month = map(int, month_param.split("-"))
                start_of_period = make_aware(datetime(year, month, 1), bd_tz)
                if month == 12:
                    end_of_period = make_aware(datetime(year + 1, 1, 1), bd_tz)
                else:
                    end_of_period = make_aware(datetime(year, month + 1, 1), bd_tz)
                view_type = "month"
                date_label = month_param
            except Exception:
                return Response({"error": "Invalid month format. Use YYYY-MM"}, status=400)

        elif custom_date:
            try:
                bd_today = parse_date(custom_date)
                start_of_period = make_aware(datetime.combine(bd_today, datetime.min.time()), bd_tz)
                end_of_period = start_of_period + timedelta(days=1)
                view_type = "day"
                date_label = custom_date
            except Exception:
                return Response({"error": "Invalid date format. Use YYYY-MM-DD"}, status=400)

        else:
            # Default to today
            bd_today = today
            start_of_period = make_aware(datetime.combine(bd_today, datetime.min.time()), bd_tz)
            end_of_period = start_of_period + timedelta(days=1)
            view_type = "day"
            date_label = str(bd_today)

        # Cache key depends on user + date range or month
        cache_key = f"dashboard_{user.id}_{view_type}_{date_label}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return Response(cached_data)

        hotels = Hotel.objects.filter(owner=user)
        bookings = Booking.objects.filter(hotel__in=hotels)

        income = BookingPayment.objects.filter(
            booking__hotel__owner=user,
            status="completed",
            payment_type__in=["full", "partial", "deposit"],  
            payment_date__gte=start_of_period,
            payment_date__lt=end_of_period
        ).aggregate(total=Sum("amount"))["total"] or 0

        rooms = RoomType.objects.filter(hotel__in=hotels)
        total_rooms = rooms.aggregate(total=Sum("total_rooms"))["total"] or 0

        # Active bookings overlapping with date range
        active_bookings = bookings.filter(
            check_in_date__lt=end_of_period,
            check_out_date__gt=start_of_period
        )

        guests_today = active_bookings.aggregate(total=Sum("number_of_guests"))["total"] or 0
        booked_rooms = active_bookings.aggregate(booked=Sum("number_of_rooms"))["booked"] or 0

        occupancy_rate = round((booked_rooms / total_rooms) * 100, 1) if total_rooms else 0

        yet_to_checkin = bookings.filter(
            check_in_date__gte=start_of_period,
            check_in_date__lt=end_of_period,
        ).exclude(
            status__in=["cancelled", "completed", "no_show"]
        ).count()

        yet_to_checkout = bookings.filter(
            check_out_date__gte=start_of_period,
            check_out_date__lt=end_of_period,
            status__in=["confirmed", "completed"]
        ).count()

        new_individuals = bookings.filter(
            created_at__gte=start_of_period,
            created_at__lt=end_of_period,
            guest__user_type="individual"
        ).count()

        new_corporates = bookings.filter(
            created_at__gte=start_of_period,
            created_at__lt=end_of_period,
            guest__user_type="corporate"
        ).count()

        data = {
            "income": income,
            "revenue_per_room": round(float(income) / booked_rooms, 2) if booked_rooms else 0,
            "occupancy_rate": occupancy_rate,
            "guest_count": guests_today,
            "expected_today": {
                "yet_to_checkin": yet_to_checkin,
                "yet_to_checkout": yet_to_checkout,
            },
            "new_arrivals": {
                "individuals": new_individuals,
                "corporates": new_corporates,
            },
            "view_type": view_type,
            "date": date_label,
        }

        cache.set(cache_key, data, timeout=60)  # cache for 1 minute
        return Response(data)


class BaseHotelReviewViewSet(viewsets.ModelViewSet):
    queryset = HotelReview.objects.select_related("hotel", "user", "booking")
    serializer_class = HotelReviewSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return HotelReview.objects.filter(user=self.request.user).select_related("hotel")

    def perform_create(self, serializer):
        booking_id = self.request.data.get("booking")
        if not booking_id:
            raise ValidationError({"booking": "Booking ID is required."})

        try:
            booking = Booking.objects.select_related("guest", "hotel").get(id=booking_id)

            if booking.guest.user:
                if booking.guest.user != self.request.user:
                    raise PermissionDenied("You are not the owner of this booking.")
            else:
                if booking.guest.email.lower() != self.request.user.email.lower():
                    raise PermissionDenied("Booking email does not match your account.")

        except Booking.DoesNotExist:
            raise PermissionDenied("Invalid booking or permission denied.")

        if HotelReview.objects.filter(booking=booking).exists():
            raise PermissionDenied("You've already reviewed this booking.")

        if booking.status not in ["confirmed", "completed"]:
            raise ValidationError({"booking": "Only confirmed or completed bookings can be reviewed."})

        serializer.save(hotel=booking.hotel, user=self.request.user, booking=booking)
        self._update_hotel_rating(booking.hotel_id)

    @staticmethod
    def _update_hotel_rating(hotel_id):
        from hotel.models import Hotel
        reviews = HotelReview.objects.filter(hotel_id=hotel_id)
        Hotel.objects.filter(id=hotel_id).update(
            review_score=reviews.aggregate(avg=Avg("rating"))['avg'] or 0,
            review_count=reviews.count(),
            staff_rating=reviews.aggregate(avg=Avg("staff"))['avg'] or 0,
            cleanliness_rating=reviews.aggregate(avg=Avg("cleanliness"))['avg'] or 0,
            location_rating=reviews.aggregate(avg=Avg("location"))['avg'] or 0,
        )



class BaseHotelTagAssignmentViewSet(viewsets.ModelViewSet):
    serializer_class = HotelTagAssignmentSerializer
    permission_classes = [IsHotelOwner]

    def get_queryset(self):
        return HotelTagAssignment.objects.filter(hotel__owner=self.request.user)

    def perform_create(self, serializer):
        # Automatically assign the hotel of the authenticated user to the tag assignment
        hotel = self.request.user.hotels.get(id=self.request.data['hotel'])
        serializer.save(hotel=hotel)

    @action(detail=True, methods=["post"], url_path="assign-tag")
    def assign_tag(self, request, pk=None):
        hotel = Hotel.objects.get(id=pk)

        # Check if the logged-in user is the owner of the hotel
        if hotel.owner != request.user:
            return Response({"error": "You cannot assign tags to hotels you do not own."}, status=status.HTTP_400_BAD_REQUEST)

        tag_ids = request.data.get("tags", [])

        # Check if tags are provided and assign them
        if not tag_ids:
            return Response({"error": "No tags provided."}, status=status.HTTP_400_BAD_REQUEST)

        tag_objects = HotelTag.objects.filter(id__in=tag_ids)
        if not tag_objects:
            return Response({"error": "Invalid tags."}, status=status.HTTP_400_BAD_REQUEST)

        # Assign the tags to the hotel
        for tag in tag_objects:
            HotelTagAssignment.objects.get_or_create(hotel=hotel, tag=tag)

        return Response({"message": "Tags assigned successfully."}, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"], url_path="remove-tag")
    def remove_tag(self, request, pk=None):
        hotel = Hotel.objects.get(id=pk)

        # Check if the logged-in user is the owner of the hotel
        if hotel.owner != request.user:
            return Response({"error": "You cannot remove tags from hotels you do not own."}, status=status.HTTP_400_BAD_REQUEST)

        tag_ids = request.data.get("tags", [])

        # Check if tags are provided
        if not tag_ids:
            return Response({"error": "No tags provided."}, status=status.HTTP_400_BAD_REQUEST)

        tag_objects = HotelTag.objects.filter(id__in=tag_ids)
        if not tag_objects:
            return Response({"error": "Invalid tags."}, status=status.HTTP_400_BAD_REQUEST)

        # Remove the tags from the hotel
        for tag in tag_objects:
            tag_assignment = HotelTagAssignment.objects.filter(hotel=hotel, tag=tag)
            if tag_assignment.exists():
                tag_assignment.delete()

        return Response({"message": "Tags removed successfully."}, status=status.HTTP_204_NO_CONTENT)




# Hotel Admin
class BaseHotelOwnerUserViewSet(viewsets.ModelViewSet):
    """
    API to create, list, update, delete Hotel Owner users.
    """
    queryset = User.objects.filter(role=User.RoleType.HOTEL_OWNER)
    serializer_class =  HotelOwnerUserSerializer
    permission_classes = [IsAuthenticated, IsStaffAndHotelAdmin]

    def perform_create(self, serializer):
        serializer.save(role=User.RoleType.HOTEL_OWNER)