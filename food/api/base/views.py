from datetime import timedelta, datetime
import hashlib
import json
import uuid
from tempfile import NamedTemporaryFile
from threading import Thread
from django.utils import timezone
import django.db
import numpy
import openpyxl
import pandas as pd
import pytz
from django.core.files.uploadedfile import (InMemoryUploadedFile,
                                            TemporaryUploadedFile)
from django.db.models import Prefetch
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django_celery_beat.models import ClockedSchedule, PeriodicTask
from PIL import Image as check_image
from rest_framework import status, viewsets
from rest_framework.exceptions import ParseError, ValidationError
from rest_framework.generics import (GenericAPIView, ListCreateAPIView,
                                     RetrieveUpdateDestroyAPIView)
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import (IsAuthenticated,
                                        IsAuthenticatedOrReadOnly)
from rest_framework.response import Response
from rest_framework.views import APIView

from accounts.models import Company
from core.api.mixins import GetObjectWithParamMixin, UserCompanyListCreateMixin
from core.api.paginations import StandardResultsSetPagination,CustomPageSizePagination
from core.api.permissions import HasCompanyAccess, HasRestaurantAccess
from core.utils import get_logger
from food.api.base.serializers import (
    ALlLocationSerializer, BaseCategorySerializer,
    BaseExcelMenuUploadSerializer, BaseExcelModifierUploadSerializer,
    BaseImageSerializer, BaseListOfRestaurantsForOrderCallSerializer,
    BaseLocationDetailSerializer, BaseLocationSerializer,
    BaseMenuDetailSerializer, BaseMenuItemDetailWithoutModifierSerializer,
    BaseMenuItemInflationSerializer, BaseMenuItemSerializer,
    BaseMenuSerializer, BaseMenuUpdateOpeningHourSerializer,
    BaseModifierGroupOrderSerializer, BaseModifierGroupSerializer,
    BaseModifiersItemsOrderSerializer, BaseRestaurantDetailSerializer,
    BaseRestaurantGETSerializer, BaseRestaurantOMSUsagesTrackerSerializer,
    BaseRestaurantSerializer, ModifierGroupSerializer,
    OpeningHourModelSerializer)
from food.models import (Category, Image, Location, Menu, MenuItem,
                         ModifierGroup, ModifierGroupOrder,
                         ModifiersItemsOrder, OpeningHour, Restaurant,
                         RestaurantOMSUsagesTracker, TimeTable, RemoteKitchenCuisine)
from food.tasks import location_scheduled_pause_unpause
from food.utilities.create_modifier_via_excel import (
    create_menu_via_excel, create_modifiers_via_excel_wrapper)
from food.utilities.save_item_images import save_item_images
from food.utilities.save_modifiers import (save_item_modifiers,
                                           save_modifier_items,
                                           save_modifiers_items)
from food.utils import is_closed, get_recommendations_from_cart
from .serializers import BaseRemoteKitchenCuisineSerializer
from django.contrib.postgres.search import TrigramSimilarity

from rest_framework import serializers

import re
logger = get_logger()


class BaseRestaurantListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    serializer_class = BaseRestaurantSerializer
    model_class = Restaurant
    pagination_class = StandardResultsSetPagination
    permission_classes = [IsAuthenticated]
    user_field_name = "owner"


class BaseRestaurantListAPIView(ListCreateAPIView):
    serializer_class = BaseRestaurantGETSerializer
    model_class = Restaurant
    pagination_class = StandardResultsSetPagination
    permission_classes = [IsAuthenticated]
    queryset = Restaurant.objects.all()


class BaseRestaurantRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    serializer_class = BaseRestaurantDetailSerializer
    model_class = Restaurant
    permission_classes = [HasCompanyAccess]
    filterset_fields = ["id", "slug"]


class BaseRestaurantBannerImageAddAPIView(APIView):
    permission_classes = [IsAuthenticated, HasCompanyAccess]

    def patch(self, request, pk: None):
        if not Restaurant.objects.filter(id=pk).exists():
            return Response({'message': 'invalid restaurant id!'}, status=status.HTTP_400_BAD_REQUEST)
        restaurant = Restaurant.objects.get(id=pk)

        if not 'images' in request.data:
            return Response({'message': 'invalid images!'}, status=status.HTTP_400_BAD_REQUEST)

        images = request.data.getlist('images')
        for image in images:
            if not self.is_valid_image(image):
                raise ValidationError(f'Invalid file format: {image.name}')

            sr = BaseImageSerializer(data={"local_url": image})
            sr.is_valid(raise_exception=True)
            image_obj = sr.save()

            restaurant.banner_image.add(image_obj)

        return Response('image added!')

    def is_valid_image(self, file):
        try:
            if isinstance(file, (InMemoryUploadedFile, TemporaryUploadedFile)):
                check_image.open(file).verify()
            else:
                return False
        except (IOError, SyntaxError) as e:
            return False
        return True


class BaseRestaurantBannerImageRemoveAPIView(APIView):
    permission_classes = [IsAuthenticated, HasCompanyAccess]

    def patch(self, request, pk=None):
        if not Restaurant.objects.filter(id=pk).exists():
            return Response({'message': 'invalid restaurant id!'}, status=status.HTTP_400_BAD_REQUEST)

        if not 'ids' in request.data:
            return Response({'message': 'invalid ids!'}, status=status.HTTP_400_BAD_REQUEST)

        images = Image.objects.filter(id__in=request.data.get('ids'))

        if not images.exists():
            return Response({'detail': 'No images found with the provided IDs.'}, status=status.HTTP_404_NOT_FOUND)

        images.delete()

        return Response('images remove')


class BaseLocationListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseLocationSerializer
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination
    filterset_fields = ["restaurant"]

    def get_queryset(self):
        if self.request.user.is_superuser:
            return Location.objects.all()

        query_set = Location.objects.filter(
            restaurant__company__user=self.request.user
        )
        if not query_set:
            query_set = Location.objects.filter(
                restaurant=self.request.query_params.get("restaurant", "")
            )
        return query_set


    # def get_queryset(self):
    #     restaurant = self.request.query_params.get('restaurant', None)
    #     restaurants = [restaurant] \
    #         if restaurant is not None \
    #         else self.request.user.restaurant_set. \
    #         values_list('id', flat=True)
    #     q_exp = Q(restaurant_id__in=restaurants)
    #     return Location.objects.filter(q_exp)


class BaseLocationRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    serializer_class = BaseLocationDetailSerializer
    model_class = Location
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ["slug", "id"]


class BaseAllLocationAPIView(ListCreateAPIView):
    serializer_class = ALlLocationSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        queryset = None
        restaurant = self.request.query_params.get("slug", None)
        if restaurant:
            queryset = Location.objects.filter(restaurant__slug=restaurant)

        return queryset


class BaseLocationAvailibilityApiView(APIView):
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def patch(self, request):
        itemId = request.data.get("itemId")
        indefinite = request.data.get("indefinite")
        today = request.data.get("today")
        create_task = False

        if not itemId:
            raise ParseError("Item selection required")

        query_set = MenuItem.objects.filter(id__in=itemId)

        for item in query_set:
            if 'today' in request.data and isinstance(today, bool):
                item.is_available_today = today
                create_task = True

            if 'indefinite' in request.data and isinstance(indefinite, bool):
                item.is_available = indefinite

        total_update = MenuItem.objects.bulk_update(
            query_set, ["is_available", "is_available_today"]
        )

        if create_task:
            for item in query_set:
                if not item.is_available_today:
                    restaurant = item.restaurant  # Assuming MenuItem has a relation to Restaurant
                    next_open_time = self.get_next_opening_time(restaurant)
                    print('next_open_time ----------> ', next_open_time)

                    # Create a clocked schedule at the next opening hour
                    scheduled_obj = ClockedSchedule.objects.create(clocked_time=next_open_time)

                    PeriodicTask.objects.create(
                        name=f"Make item available --> {next_open_time} --> {item.id}",
                        task="chatchef.menu_item_unavailable_for_today",
                        args=json.dumps([item.id]),
                        clocked=scheduled_obj,
                        one_off=True,
                    )

        return Response(f"Availability of {total_update} items has been updated.")

    def get_next_opening_time(self, restaurant):
        """
        Get the next opening time for the restaurant based on its timezone and opening hours.
        """
        current_time = timezone.now()

        # Convert current time to the restaurant's timezone
        offset_hours, offset_minutes = map(int, restaurant.timezone.split(":"))
        offset_delta = timedelta(hours=offset_hours, minutes=offset_minutes)
        restaurant_time = current_time + offset_delta

        next_day = restaurant_time + timedelta(days=1)
        next_day_name = next_day.strftime("%a").lower()

        # Get the opening hours for the next day
        opening_hours = restaurant.opening_hours.filter(day_index=next_day_name, is_close=False)

        if opening_hours.exists():
            # Assuming opening at midnight unless specific handling is defined for time
            next_opening_datetime = next_day.replace(hour=0, minute=0, second=0, microsecond=0)

            # Convert back to UTC
            return next_opening_datetime - offset_delta

        # Default to 24 hours from now if no opening hours are found
        return current_time + timedelta(days=1)


class BaseMenuListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    model_class = Menu
    pagination_class = StandardResultsSetPagination
    permission_classes = [HasRestaurantAccess]
    # filterset_fields = ['locations', 'restaurant']
    search_fields = ["title"]

    # filterset_class = MenuFilter

    def get_queryset(self):
        location = self.request.query_params.get("locations", None)
        restaurant = self.request.query_params.get("restaurant", None)
        da = self.request.query_params.get("da", None)

        # query_set = super().get_queryset(
        # ) if location is None and restaurant is None else Menu.objects.all()

        # for menu in query_set:
        #     if menu.opening_hours.all().exists():
        #         for date in menu.opening_hours.all():
        #             current_day = datetime.datetime.now().strftime("%a").lower()
        #             if TimeTable.objects.filter(opening_hour=date.id).exists():
        #                 is_times = TimeTable.objects.filter(
        #                     opening_hour=date.id)
        #                 if is_times:
        #                     for time in is_times:
        #                         current_time = datetime.datetime.now().time()
        #                         if time.start_time < current_time < time.end_time:
        #                             print(time.start_time)
        #                             print(current_time)
        #                             print(time.end_time)
        #                         date.is_close = False if time.start_time < current_time < time.end_time else True
        #                 # if current_day != date.day_index:
        #                 #     date.is_close = True
        #         date.save()

        query_set = super().get_queryset()
        if restaurant is not None and location is not None:
            query_set = query_set.filter(
                restaurant=restaurant, locations=location)
        if restaurant is not None and da is not None:
            query_set = query_set.filter(
                restaurant=restaurant)
        return query_set

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        paginator = StandardResultsSetPagination()
        paginator.page_size = 10
        page = paginator.paginate_queryset(queryset, request)
        serializer = self.get_serializer(page, many=True)
        data = serializer.data
        data_list = json.loads(json.dumps(data))
        status_array = [i.get("is_closed") for i in data_list]
        store_closed_status = True
        if any(status is False for status in status_array):
            store_closed_status = False

        _data = data

        if not store_closed_status:
            _data = sorted(data, key=lambda x: x['is_closed'])

            return paginator.get_paginated_response(_data)

        return super().list(request, *args, **kwargs)

    def get_serializer_class(self):
        location = self.request.query_params.get("locations", None)
        if location is None:
            return BaseMenuSerializer
        return BaseMenuDetailSerializer


class BaseMenuRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    # serializer_class = BaseMenuDetailSerializer
    model_class = Menu
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ["id", "slug"]

    def get_serializer_class(self):
        query = self.request.query_params.get("id", None)
        if query is not None:
            if self.request.method == "GET":
                return BaseMenuItemDetailWithoutModifierSerializer
            if self.request.method == "PATCH":
                return BaseMenuSerializer
        return BaseMenuDetailSerializer

    # def get_queryset(self):
    #     search = self.request.query_params.get("search", None)
    #     rating = self.request.query_params.get("rating", None)

    #     if not search:
    #         key = "id"
    #         if rating:
    #             key = "-rating"

    #         print('sorting --> ', key)

    #         menu_items_prefetch = Prefetch(
    #             "menuitem_set",
    #             queryset=MenuItem.objects.order_by(key)
    #         )
    #         return super().get_queryset().prefetch_related(
    #             menu_items_prefetch
    #         )

    #     menu_items_prefetch = Prefetch(
    #         "menuitem_set",
    #         queryset=MenuItem.objects.filter(name__icontains=search)
    #     )
    #     return super().get_queryset().prefetch_related(
    #         menu_items_prefetch
    #     )
    def get_queryset(self):
        search = self.request.query_params.get("search")
        rating = self.request.query_params.get("rating")

        key = "id"
        if rating:
            key = "-rating"

        # Base queryset: always prefetch related objects
        base_qs = (
            super()
            .get_queryset()
            .select_related("restaurant")
            .prefetch_related("locations")
        )

        # MenuItem queryset with optimized prefetching
        menu_items_qs = (
            MenuItem.objects
            .select_related("original_image")
            .prefetch_related("images", "category")
            .order_by(key)
        )

        if not search:
            return base_qs.prefetch_related(
                Prefetch("menuitem_set", queryset=menu_items_qs)
            )

        return base_qs.prefetch_related(
            Prefetch(
                "menuitem_set",
                queryset=menu_items_qs.filter(name__icontains=search)
            )
        )



# Add API views for search/autocomplete

# class RestaurantSearchAPIView(APIView):
#     permission_classes = [IsAuthenticatedOrReadOnly]

#     def get(self, request):
#         q = request.query_params.get('q', '').strip()
#         if not q:
#             # Optionally return top 20 popular restaurants or empty list
#             restaurants = Restaurant.objects.all()[:20]
#         else:
#             restaurants = Restaurant.objects.annotate(
#                 similarity=TrigramSimilarity('name', q)
#             ).filter(similarity__gt=0.3).order_by('-similarity')[:20]

#         serializer = RestaurantSearchSerializer(restaurants, many=True)
#         return Response(serializer.data)


# class LocationSearchAPIView(APIView):
#     permission_classes = [IsAuthenticatedOrReadOnly]

#     def get(self, request):
#         restaurant_id = request.query_params.get('restaurant_id')
#         q = request.query_params.get('q', '').strip()

#         if not restaurant_id:
#             return Response({"error": "restaurant_id is required"}, status=400)

#         locations = Location.objects.filter(restaurant_id=restaurant_id)

#         if q:
#             locations = locations.annotate(
#                 similarity=TrigramSimilarity('name', q)
#             ).filter(similarity__gt=0.3).order_by('-similarity')

#         locations = locations[:20]
#         serializer = LocationSearchSerializer(locations, many=True)
#         return Response(serializer.data)
    

# All restaurant list for dropdown

class BaseRestaurantDropdownListAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        restaurants = Restaurant.objects.all().only('id', 'name')  # optimized query
        data = [{"id": r.id, "name": r.name} for r in restaurants]
        return Response(data)


# All location list for dropdown

class BaseLocationDropdownListAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        locations = Location.objects.select_related("restaurant").all().only('id', 'name', 'restaurant__name')
        data = [{"id": loc.id, "location_name": loc.name, "restaurant_name": loc.restaurant.name} for loc in locations]
        return Response(data)


class BaseCategoryListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseCategorySerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ["menu", "restaurant", "locations"]
    search_fields = ["name"]

    def get_queryset(self):
        user = self.request.user

        if not user.is_authenticated:
            raise ParseError("Authentication required.")

        if user.is_superuser:
            return Category.objects.all()

        # For non-superusers (existing production behaviour)
        return Category.objects.filter(restaurant__company=user.company)



class BaseCategoryRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    serializer_class = BaseCategorySerializer
    model_class = Category
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    filterset_fields = ["id", "slug"]

class BaseMenuItemListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseMenuItemSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [HasRestaurantAccess]
    # model_class = MenuItem
    filterset_fields = ["menu", "category", "restaurant"]
    search_fields = ["name"]

    def get_queryset(self):
        query = self.request.query_params
        menu, category, direct_order = (
            query.get("menu", None),
            query.get("category", None),
            query.get("direct_order", None),
        )

        # If the user is not authenticated our queryset is all the menu items, else we are returning only the
        # authenticated user's items
        if not self.request.user.is_authenticated and menu is None:
            raise ParseError(
                "menu id must be provided when not authenticated!")
        # if menu is not None or category is not None:
        #     raise ParseError(
        #         'menu id must be provided when not authenticated!')

         # If superuser, always return all items
        if self.request.user.is_authenticated and self.request.user.is_superuser:
            return MenuItem.objects.all()
        
        if direct_order or not self.request.user.is_authenticated:
            return MenuItem.objects.all()
        return MenuItem.objects.filter(restaurant__company=self.request.user.company)

    def perform_create(self, serializer):
        super().perform_create(serializer)
        save_item_images(self, serializer)
        save_item_modifiers(self, serializer)



class BaseMenuItemRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    serializer_class = BaseMenuItemSerializer
    model_class = MenuItem
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ["id", "slug"]

    def perform_update(self, serializer):
        super().perform_update(serializer)
        save_item_images(self, serializer)
        save_item_modifiers(self, serializer)

    def get_queryset(self):
        modifier_items_prefetch = Prefetch(
            "modifier_items",
            queryset=MenuItem.objects.order_by("id")
        )
        modifier_group_prefetch = Prefetch(
            "modifiergroup_set",
            queryset=ModifierGroup.objects.prefetch_related(
                modifier_items_prefetch).order_by("id")
        )
        return super().get_queryset().prefetch_related(
            modifier_group_prefetch
        )

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.menu:
            if instance.menu.modifiers_show_reverse:
                instance.modifiergroup_set.set(
                    instance.modifiergroup_set.all())
        serializer = self.get_serializer(instance)
        return Response(serializer.data)


# TODO(have to set permission)
class BaseMenuItemAvailibilityApiView(APIView):
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def patch(self, request):
        itemId = request.data.get("itemId")
        indefinite = request.data.get("indefinite")
        today = request.data.get("today")
        create_task = False

        if not itemId:
            raise ParseError("Item selection required")

        if isinstance(itemId, (str, int)):  
            itemId = [itemId]  # Ensure itemId is always a list

        query_set = list(MenuItem.objects.filter(id__in=itemId))

        if not query_set:
            return Response({"message": "No items found for the given IDs."}, status=400)

        fields_to_update = []

        for item in query_set:
            updated = False

            if 'today' in request.data and isinstance(today, bool):
                if item.is_available_today != today:
                    item.is_available_today = today
                    updated = True
                    fields_to_update.append("is_available_today")
                    create_task = True  # If today is updated, we may need to schedule a task

            if 'indefinite' in request.data and isinstance(indefinite, bool):
                if item.is_available != indefinite:
                    item.is_available = indefinite
                    updated = True
                    fields_to_update.append("is_available")

            if updated:
                item.save()  # Save each item individually to ensure changes persist

        # Handle periodic task creation for unavailable items
        if create_task:
            for item in query_set:
                if not item.is_available_today:
                    restaurant = item.menu
                    logger.info(f"{item.restaurant} is unavailable today. Scheduling task...")
                    next_open_time = self.get_next_opening_time(item)

                    logger.info(f"Next open time for item {item.id}: {next_open_time}")

                    # Delete existing scheduled task if it exists
                    existing_task = PeriodicTask.objects.filter(
                        name__contains=f"Make item unavailable --> item_{item.id}"
                    ).first()
                    
                    print(f"Make item unavailable --> item_{item.id} --> {next_open_time} --> {uuid.uuid4()}")

                    if existing_task:
                        existing_task.delete()

                    # Create a new scheduled task
                    try:
                        unique_task_name = f"Make item unavailable --> item_{item.id} --> {next_open_time} --> {uuid.uuid4()}"
                        scheduled_obj = ClockedSchedule.objects.create(clocked_time=next_open_time)
                        print('scheduled_obj --> ', scheduled_obj)
                        PeriodicTask.objects.create(
                            name=unique_task_name,
                            task="chatchef.menu_item_unavailable_for_today",
                            args=json.dumps([item.id]),
                            clocked=scheduled_obj,
                            one_off=True,
                        )
                    except Exception as e:
                        logger.error(f"Error scheduling task for item {item.id}: {e}")

        # Serialize the updated items to return in the response
        # serialized_data = BaseMenuItemSerializer(query_set, many=True).data

        return Response({
            "message": f"Availability of {len(query_set)} items has been updated.",
            "updated_fields": list(set(fields_to_update)),  # Ensure unique field names
        })

    def get_next_opening_time(self, item):
        """
        Get the next opening time for the restaurant based on its timezone and menu opening hours.
        Ensures that the next opening time is always in the future.
        """
        try:
            current_time_utc = timezone.now()
            
            # Fetch timezone from item.restaurant
            try:
                restaurant_timezone_str = item.restaurant.timezone.strip()
                
                if re.match(r"^[+-]\d{2}:\d{2}$", restaurant_timezone_str):  
                    # Matches formats like "-08:00" or "+05:30"
                    hours, minutes = map(int, restaurant_timezone_str.split(":"))
                    offset_minutes = hours * 60 + minutes if hours >= 0 else hours * 60 - minutes
                    restaurant_tz = pytz.FixedOffset(offset_minutes)
                else:
                    restaurant_tz = pytz.timezone(restaurant_timezone_str)

            except Exception as e:
                logger.error(f"Invalid timezone for restaurant {item.restaurant.id}: {item.restaurant.timezone} | Error: {e}")
                return current_time_utc + timedelta(days=1)

            restaurant_time = current_time_utc.astimezone(restaurant_tz)
            logger.info(f"Converted restaurant time: {restaurant_time} ({item.restaurant.timezone} offset)")

            days_checked = 0

            while days_checked < 7:  # Limit search to avoid infinite loops
                day_name = restaurant_time.strftime("%a").lower()
                logger.info(f"Checking opening hours for {day_name}")

                # Fetch opening hours for the given day
                opening_hours = item.menu.opening_hours.filter(day_index=day_name, is_close=False)

                if opening_hours.exists():
                    # Get all timetables for the given day, sorted by start time
                    timetables = TimeTable.objects.filter(opening_hour__in=opening_hours).order_by("start_time")

                    for timetable in timetables:
                        next_opening_time = timetable.start_time
                        logger.info(f"Checking possible next opening time: {next_opening_time}")

                        # Construct the next opening datetime in the restaurant's timezone
                        next_opening_datetime = restaurant_time.replace(
                            hour=next_opening_time.hour,
                            minute=next_opening_time.minute,
                            second=0,
                            microsecond=0
                        )

                        # ✅ If it's in the future, return it
                        if next_opening_datetime > restaurant_time:
                            logger.info(f"Returning next opening time: {next_opening_datetime}")
                            return next_opening_datetime

                # If no opening times found or all have passed, move to the next day
                restaurant_time = (restaurant_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                days_checked += 1

            logger.warning(f"No opening hours found for the next 7 days in menu {item.menu.id}")
            return restaurant_time + timedelta(days=1)

        except Exception as e:
            logger.error(f"Error getting next opening time for menu {item.menu.id}: {e}")
            return current_time_utc + timedelta(days=1)


class BaseModifierGroupListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseModifierGroupSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    filterset_fields = ["menu", "modifier_items", "used_by", "restaurant"]
    queryset = ModifierGroup.objects.all()

    def get_serializer_class(self):
        if self.request.method == "GET":
            return BaseModifierGroupSerializer
        else:
            return ModifierGroupSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        save_modifiers_items(serializer.instance)
        save_modifier_items(serializer.instance)

        return Response(BaseModifierGroupSerializer(serializer.instance).data, status=status.HTTP_201_CREATED)


class BaseModifierGroupRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    # serializer_class = BaseModifierGroupSerializer
    model_class = ModifierGroup
    permission_classes = [IsAuthenticatedOrReadOnly, HasRestaurantAccess]
    filterset_fields = ["slug", "id", "menu"]

    def get_serializer_class(self):
        if self.request.method == "PATCH":
            return ModifierGroupSerializer
        else:
            return BaseModifierGroupSerializer


class BaseModifiersGroupOrderModelView(viewsets.ModelViewSet):
    serializer_class = BaseModifierGroupOrderSerializer
    queryset = ModifierGroupOrder.objects.all()
    filterset_fields = ["menu_item"]


class BaseModifiersItemOrderModelView(viewsets.ModelViewSet):
    serializer_class = BaseModifiersItemsOrderSerializer
    queryset = ModifiersItemsOrder.objects.all()
    filterset_fields = ["menu_item", "modifier_item"]


class BaseModifiersAvailabilityAPIView(APIView):
    def patch(self, request, pk=None):
        if not ModifierGroup.objects.filter(id=pk).exists():
            return Response("invalid request!", status=status.HTTP_400_BAD_REQUEST)

        obj = ModifierGroup.objects.get(id=pk)

        today = request.data.get("today")
        infinite = request.data.get("infinite")

        if "infinite" in request.data:
            print("infinite")
            obj.is_available = infinite
            obj.save()
            return Response("modifier availability updated")

        if "today" in request.data:
            print("Today")
            obj.is_available_today = today
            obj.save()

            print('obj --', obj.is_available_today)
            

            if obj.is_available_today == False:
                _time = datetime.datetime.now()
                _time = _time + datetime.timedelta(
                    days=1, hours=-_time.hour, minutes=-_time.minute + 1
                )

                scheduled_obj = ClockedSchedule.objects.create(
                    clocked_time=_time)

                PeriodicTask.objects.create(
                    name=f"make modifiers available --> {_time} --> {obj.id}",
                    task="chatchef.modifier_available",
                    kwargs=json.dumps({'pk': obj.id}),
                    clocked=scheduled_obj,
                    one_off=True,
                )

            return Response("modifier availability updated")
        return Response("invalid request!", status=status.HTTP_400_BAD_REQUEST)


class BaseExcelMenuUploadAPIView(GenericAPIView):
    serializer_class = BaseExcelMenuUploadSerializer
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    # parser_classes = [FileUploadParser]

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        menu_file = serializer.validated_data.get("menu_file")
        name = serializer.validated_data.get("name")
        restaurant = serializer.validated_data.get("restaurant")
        locations = serializer.validated_data.get("locations")
        df = pd.read_excel(menu_file)
        row_list = df.replace(numpy.nan, None).values.tolist()

        menu = Menu.objects.create(
            title=name, restaurant_id=restaurant, company=request.user.company
        )
        if locations is not None:
            menu.locations.add(*locations)
        # print(row_list)

        thread = Thread(
            target=create_menu_via_excel, args=(row_list, menu))
        thread.start()

        return Response(BaseMenuSerializer(instance=menu).data)


def menu_time_handler(menu, data):
    days = [
        {
            "opening_hour_id": item.get("opening_hour_id", None),
            "day_index": item["day_index"],
            "is_close": item["is_close"],
            "times": [
                {
                    "id": time_slot.get("id", None),
                    "start_time": time_slot["start_time"],
                    "end_time": time_slot["end_time"],
                    "is_delete": time_slot["is_delete"],
                }
                for time_slot in item["times"]
            ],
        }
        for item in data["opening_hour"]
    ]
    is_success = True

    for day in days:
        daysExists = menu.opening_hours.all()
        opening_hour_id = None
        if (
            day["opening_hour_id"]
            and OpeningHour.objects.filter(id=day["opening_hour_id"]).exists()
        ):
            opening_hour_id = OpeningHour.objects.get(
                id=day["opening_hour_id"])
            opening_hour_id.day_index = day["day_index"]
            opening_hour_id.is_close = day["is_close"]
            opening_hour_id.save()
        else:
            is_exists = False
            current_data = None
            for exists in daysExists:
                if exists.day_index == day["day_index"]:
                    is_exists = True
                    current_data = exists
                    break
            if is_exists:
                opening_hour_id = current_data
            else:
                opening_hour_id = OpeningHour.objects.create(
                    day_index=day["day_index"], is_close=day["is_close"]
                )

        for time in day["times"]:
            if time["id"] and TimeTable.objects.filter(id=time["id"]).exists():
                times = TimeTable.objects.get(id=time["id"])
                if time["is_delete"]:
                    times.delete()
                else:
                    times.start_time = time["start_time"]
                    times.end_time = time["end_time"]
                    times.save()
            else:
                times = TimeTable.objects.create(
                    start_time=time["start_time"],
                    end_time=time["end_time"],
                    opening_hour=opening_hour_id,
                )
        menu.opening_hours.add(opening_hour_id)
    return is_success


class BaseUpdateMenuOpeningTime(APIView):
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def get(self, request, pk):
        try:
            menu = Menu.objects.get(id=pk)
            opening_hours = list(menu.opening_hours.all())

            opening_hours_data = []
            for opening_hour in opening_hours:
                serializer = OpeningHourModelSerializer(opening_hour)
                opening_hours_data.append(serializer.data)

            context = {"opening_hours": opening_hours_data}
            return Response(context)

        except Menu.DoesNotExist:
            return Response(
                {"error": "Menu not found"}, status=status.HTTP_404_NOT_FOUND
            )

    def patch(self, request, pk):
        sr = BaseMenuUpdateOpeningHourSerializer(data=request.data)
        if sr.is_valid(raise_exception=True):
            menu = Menu.objects.get(id=pk)
            is_success = menu_time_handler(menu, sr.data)
            if is_success:
                return Response({"status": "success"})
        return Response({"status": "failed"})


class BaseMenuPriceInflationApiView(APIView):
    def patch(self, request, pk):
        instance = None
        try:
            instance = Restaurant.objects.get(pk=pk)
        except:
            return Response(
                {"error": "Instance not found"}, status=status.HTTP_404_NOT_FOUND
            )

        sr = BaseMenuItemInflationSerializer(
            instance, data=request.data, partial=True)
        sr.is_valid(raise_exception=True)
        sr.save()

        menu_items = MenuItem.objects.filter(restaurant=pk)
        for item in menu_items:
            item.virtual_price = round(
                (item.base_price / (1 - (instance.inflation_percent / 100))), 2
            )
        total_update = MenuItem.objects.bulk_update(
            menu_items, ["virtual_price"])

        return Response(f"inflation applied on {total_update} item")


class BaseExcelModifierGroupUploadAPIView(GenericAPIView):
    serializer_class = BaseExcelModifierUploadSerializer
    # permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        menu_file = serializer.validated_data.get("modifiers_file")
        restaurant_id = serializer.validated_data.get("restaurant")
        location_id = serializer.validated_data.get("location")
        df = pd.read_excel(menu_file)
        row_list = df.replace(numpy.nan, None).values.tolist()

        restaurant = Restaurant.objects.get(id=restaurant_id)
        location = Location.objects.get(id=location_id)

        thread = Thread(
            target=create_modifiers_via_excel_wrapper, args=(row_list, restaurant, location))
        thread.start()

        return Response('Modifiers creating process started')


# Export menu items to excel --> new format
class BaseMenuExportToExcelAPIView(APIView):
    def get(self, request, pk=None):
        menu = None

        if not pk:
            return Response({'message': 'invalid request'}, status=status.HTTP_400_BAD_REQUEST)

        if Menu.objects.filter(id=pk).exists():
            menu = Menu.objects.get(id=pk)

        else:
            return Response({'message': 'invalid request'}, status=status.HTTP_400_BAD_REQUEST)

        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.append([])
        sheet.append([
            'Category', 'Item Name', 'Description', 'Price',
            'Currency', 'Image', 'Add ons'
        ])

        menu_items = MenuItem.objects.filter(menu=menu).reverse()

        for menu_item in menu_items:
            category_names = ''.join(
                [f"{category.name}," for category in menu_item.category.all()])

            image_links = ""
            if menu_item.original_image:
                image_links = f"{menu_item.original_image.working_url}"
            elif menu_item.images:
                first_image = menu_item.images.all().first()
                if first_image:
                    image_links = f"{first_image.working_url}"

            modifiers = ''.join(
                [f'["{modifier.name}"], ' for modifier in ModifierGroup.objects.filter(used_by=menu_item)])

            sheet.append([
                f'{category_names}',
                f'{menu_item.name}',
                f'{menu_item.description}',
                f'{menu_item.base_price}',
                f'{menu_item.currency}',
                f'{image_links}',
                f'{modifiers}'
            ])

        with NamedTemporaryFile() as tmp:
            workbook.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()
        response = HttpResponse(
            content=stream,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f"attachment; filename={menu.title}_{menu.restaurant.name}.xlsx"

        return response


# Export Modifiers to excel
class BaseExportModifiersToExcelAPIView(APIView):
    def get(self, request):
        restaurant = request.query_params.get('restaurant', None)
        location = request.query_params.get('location', None)

        if restaurant is None:
            return Response('Invalid Request', status=status.HTTP_400_BAD_REQUEST)

        modifier_groups = None

        if location:
            modifier_groups = ModifierGroup.objects.filter(
                restaurant=restaurant, locations=location)
        else:
            modifier_groups = ModifierGroup.objects.filter(
                restaurant=restaurant)

        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.append([])
        sheet.append([
            'Modifier Name', 'Modifiers Description', 'Requirements Status', 'Item Limit',
            'Modifiers Items'
        ])

        for modifier_group in modifier_groups:

            modifiers_items = ''.join(
                f"['{item.name}',  {item.base_price}]," for item in modifier_group.modifier_items.all())
            sheet.append([
                f'{modifier_group.name}',
                f'{modifier_group.description}',
                f'{modifier_group.requirement_status}',
                f'{modifier_group.item_limit}',
                f'{modifiers_items}',
            ])

        with NamedTemporaryFile() as tmp:
            workbook.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()
        response = HttpResponse(
            content=stream,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f"attachment; filename=modifiers.xlsx"

        return response


class BaseRestaurantOMSUsagesTrackerAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        restaurant = request.query_params.get("restaurant")
        location = request.query_params.get("location")

        obj = RestaurantOMSUsagesTracker.objects.filter(
            restaurant=restaurant, location=location).first()

        if not obj:
            queryset = RestaurantOMSUsagesTracker.objects.all()
            paginator = PageNumberPagination()
            paginator.page_size = 10
            page = paginator.paginate_queryset(queryset, request)

            return paginator.get_paginated_response(BaseRestaurantOMSUsagesTrackerSerializer(
                page, many=True).data)

        return Response(BaseRestaurantOMSUsagesTrackerSerializer(obj).data)

    def put(self, request):
        instance = RestaurantOMSUsagesTracker.objects.filter(
            restaurant=request.data.get("restaurant"), location=request.data.get("location")).first()

        sr = BaseRestaurantOMSUsagesTrackerSerializer(
            instance, data=request.data)

        sr.is_valid(raise_exception=True)
        sr.save()
        return Response(sr.data)


class BaseEmployRegisterGetAPIView(APIView):
    def get(self, request, pk=None):
        if not Restaurant.objects.filter(id=pk).exists():
            data = []
            restaurants = Restaurant.objects.all()
            for restaurant in restaurants:
                company = restaurant.company
                if not company.register_code:
                    company.register_code = self.generate_unique_register_code(
                        company)
                    company.save()
                data.append({f"{restaurant.name}": f"{company.register_code}"})

            return Response(data, status=status.HTTP_200_OK)
        company = Restaurant.objects.get(id=pk).company
        if not company.register_code:
            company.register_code = self.generate_unique_register_code(company)
            company.save()

        return Response(f"{company.register_code}")

    def generate_unique_register_code(self, company):
        hash_input = (company.name + str(uuid.uuid4())).encode('utf-8')
        register_code = hashlib.sha256(hash_input).hexdigest()[
            :5]  # Use first 5 characters of the hash
        while Company.objects.filter(register_code=register_code).exists():
            hash_input = (company.name + str(uuid.uuid4())).encode('utf-8')
            register_code = hashlib.sha256(hash_input).hexdigest()[:5]
        return register_code


class BaseListOfRestaurantsForOrderCallAPIview(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        context = {}
        restaurants = Restaurant.objects.all()
        context['enabled'] = BaseListOfRestaurantsForOrderCallSerializer(
            restaurants.filter(receive_call_for_order=True), many=True).data
        context['disabled'] = BaseListOfRestaurantsForOrderCallSerializer(
            restaurants.filter(receive_call_for_order=False), many=True).data
        context["active"] = Restaurant.objects.filter(
            receive_call_for_order=True).count()
        context["pause"] = Restaurant.objects.filter(
            receive_call_for_order=True).count()
        context["total"] = Restaurant.objects.filter().count()

        return Response(context)
      
      
class BaseRecommendedDishView(APIView):
    def post(self, request, *args, **kwargs):
        """
        View to fetch recommendations based on cart items sent in the request body.
        """
        try:
            # Parse the JSON body
            body = request.data  # DRF automatically parses JSON payload
            cart_item_ids = body.get('cart_items', [])  # Expecting 'cart_items' key in the JSON
            
            print(cart_item_ids, '----------------------->1019')

            if not cart_item_ids or not isinstance(cart_item_ids, list):
                return Response({'error': 'Invalid cart items'}, status=400)

            # Get recommendations
            recommendations = get_recommendations_from_cart(cart_item_ids)

            # Serialize the recommended dishes
            serializer = BaseMenuItemSerializer(recommendations, many=True, context={'request': request})

            return Response(serializer.data)

        except Exception as e:
            return Response({'error': str(e)}, status=500)
        


class BaseRemoteKitchenCuisineView(APIView):
    queryset = RemoteKitchenCuisine.objects.all()
    serializer_class = BaseRemoteKitchenCuisineSerializer

    def create(self, request, *args, **kwargs):
        """Create a new cuisine for a restaurant"""
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"message": "Cuisine added successfully", "data": serializer.data}, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class MenuItemAvailabilitySerializer(serializers.ModelSerializer):
    class Meta:
        model = MenuItem
        fields = ['available_start_time', 'available_end_time']
class BaseMenuItemAvailabilityUpdateAPIView(APIView):
    def patch(self, request, pk):
        menu_item = get_object_or_404(MenuItem, pk=pk)
        serializer = MenuItemAvailabilitySerializer(menu_item, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

