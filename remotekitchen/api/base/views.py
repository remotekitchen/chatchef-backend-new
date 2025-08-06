from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotAuthenticated
from food.api.base.views import BaseRestaurantListAPIView
from food.models import Category, MenuItem, Restaurant, VisitHistory, ItemVisitHistorySingle, RemoteKitchenCuisine
from marketing.models import Rating
from remotekitchen.api.base.serializers import (
    BaseRemoteKitchenCuisineSerializer, RemoteKitchenRestaurantSerializer)
from remotekitchen.models import Cuisine, SearchKeyword, Favorite, FavoriteRestaurant, DeliveryFeeRule
from accounts.models import UserAddress,RedZone
from remotekitchen.utils import StandardRemoteKitchenResultsSetPagination
from django_ratelimit.decorators import ratelimit
from django.utils.decorators import method_decorator
from rest_framework.permissions import IsAuthenticated
from django.db.models import Count
from django.shortcuts import get_object_or_404
from rest_framework import status
from food.api.base.serializers import (BaseMenuItemSerializer)
from remotekitchen.api.base.serializers import FavoriteSerializer, UserAddressSerializer, RestaurantFavoriteSerializer,BaseCuisineSerializer
from datetime import date
from django.utils.timezone import now, timedelta
import time
import requests
from hungrytiger.settings import env 
from billing.models import Order
from remotekitchen.utils import haversine_distance
GOOGLE_MAPS_API_KEY =env.str("GOOGLE_MAPS_API_KEY")

class BaseRemoteKitchenCuisineModelView(viewsets.ModelViewSet):
    queryset = Cuisine.objects.all()
    serializer_class = BaseRemoteKitchenCuisineSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    pagination_class = StandardRemoteKitchenResultsSetPagination


def log_search_keyword(keyword, user=None, result_count=0):
    print(f"Saving keyword: {keyword}, User: {user}, Result Count: {result_count}")
    if user is None or not user.is_authenticated:
        user = None  # Allow saving the keyword without a user
    SearchKeyword.objects.create(
        keyword=keyword,
        user=user,
        result_count=result_count
    )
    
class BaseTopSearchKeywordsAPIView(APIView):
    def get(self, request):
        # Aggregate and order keywords by their count
        top_keywords = (
            SearchKeyword.objects.values('keyword')  # Group by keyword
            .annotate(search_count=Count('keyword'))  # Count occurrences
            .order_by('-search_count')[:5]  # Get top 5
        )

        # Format the response
        data = [
            {
                'keyword': keyword['keyword'],
                'search_count': keyword['search_count']
            }
            for keyword in top_keywords
        ]
        return Response(data)
    
class BaseSearchSuggestionApiView(APIView):
    permission_classes = [IsAuthenticated]  # Ensure the user is authenticated

    def get(self, request):
        user = request.user  # Get the authenticated user
        search_keywords = SearchKeyword.objects.filter(user=user).order_by('-created_at')  # Retrieve user's keywords

        # Serialize the data (only return keywords and count)
        data = [
            {
                'keyword': keyword.keyword,
                'result_count': keyword.result_count,
                'created_at': keyword.created_at,
            }
            for keyword in search_keywords
        ]
        return Response(data)

class BaseRemoteKitchenRestaurantList(BaseRestaurantListAPIView):
    pagination_class = StandardRemoteKitchenResultsSetPagination
    serializer_class = RemoteKitchenRestaurantSerializer

    def get_queryset(self):
        query_set = super().get_queryset().filter(is_remote_Kitchen=True)
        lat = self.request.query_params.get("lat")
        lon = self.request.query_params.get("lng") 
        self.user_in_red_zone = False
        if lat and lon:
            try:
                lat = float(lat)
                lon = float(lon)

                for zone in RedZone.objects.filter(active=True):
                    distance_to_zone = haversine_distance(lat, lon, zone.center_lat, zone.center_lon)
                    print(f"Distance to {zone.name}: {distance_to_zone:.3f} km (Radius: {zone.radius_km})")
                    if distance_to_zone <= zone.radius_km:
                        # User is in a red zone — show only allowed restaurants
                        print(f"User is inside red zone: {zone.name}")
                        self.user_in_red_zone = True  
                        return zone.allowed_restaurants.filter(is_remote_Kitchen=True, is_store_close=False)


            except Exception as e:
                print(f"Error in red zone distance check: {e}")
                pass  # Fail gracefully if bad input

        cuisines = self.request.query_params.get("cuisine")  
        price_lte = self.request.query_params.get("price_lte")
        price_gte = self.request.query_params.get("price_gte")
        search = self.request.query_params.get("search")
        restaurant_search = self.request.query_params.get("restaurant_search")

        #  Parse most_rated properly
        most_rated = self.request.query_params.get("most_rated", "").lower() == "true"

        if restaurant_search:
            search_ids = Restaurant.objects.filter(
                name__icontains=restaurant_search.lower()
            ).values_list('id', flat=True).distinct()
            query_set = query_set.filter(id__in=search_ids)

            result_count = query_set.distinct().count()
            log_search_keyword(restaurant_search, user=self.request.user, result_count=result_count)

        if search:
            search_ids = MenuItem.objects.filter(
                name__icontains=search.lower()
            ).values_list('restaurant_id', flat=True).distinct()
            query_set = query_set.filter(id__in=search_ids)

            result_count = query_set.distinct().count()
            log_search_keyword(search, user=self.request.user, result_count=result_count)

        # Filter by cuisine
        if cuisines:
            cuisine_list = [cuisine.strip() for cuisine in cuisines.split(',')]
            query_set = query_set.filter(cuisines__name__in=cuisine_list).distinct()

        # Filter by price range
        if price_lte and price_gte:
            price_range_ids = MenuItem.objects.filter(
                base_price__gte=price_gte,
                base_price__lte=price_lte
            ).values_list('restaurant_id', flat=True).distinct()
            query_set = query_set.filter(id__in=price_range_ids)

        # Filter by most rated
        # if most_rated:
        #     rating_ids = Rating.objects.order_by("-rating").values_list(
        #         'menuItem__restaurant_id', flat=True).distinct()
        #     query_set = query_set.filter(id__in=rating_ids)

        return query_set

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        #  Convert query params to booleans
        nearest = request.query_params.get("nearest", "").lower() == "true"
        ldf = request.query_params.get("ldf", "").lower() == "true"
        most_rated = request.query_params.get("most_rated", "").lower() == "true"
        comprehensive_ranking = request.query_params.get("comprehensive_ranking", "").lower() == "true"

        distance_km_default = 5
        distance_km = request.query_params.get("km", None)
        distance_min = request.query_params.get("min", None)

        # Convert delivery time (min) to distance (km)
        if distance_min:
            try:
                distance_km = (float(distance_min) / 60) * 10.64
            except ValueError:
                distance_km = None

        if distance_km is not None:
            try:
                distance_km = float(distance_km)
            except ValueError:
                distance_km = None

        serializer = self.get_serializer(queryset, many=True)
        data = serializer.data

        #  Filter based on distance
        if distance_km is not None:
            data = [
                item for item in data
                if item['distance'] is not None and float(item['distance']) <= distance_km
            ]
        else:
            data = [
                item for item in data
                if item['distance'] is not None and float(item['distance']) <= distance_km_default
            ]

        #  Sort: comprehensive ranking (weighted score)
        if comprehensive_ranking:
            for item in data:
                rating = float(item.get('rating') or item.get('res_details', {}).get('average_rating') or 0)

                distance = item.get('distance', 0)
                delivery_fee = float(item.get('delivery_fee', {}).get("5.0", 0.99))

                rating_score = rating if rating else 3
                distance_score = 10 if distance == 0 else 1 / (distance + 0.1)
                delivery_fee_score = 1 / (delivery_fee + 1)

                item['comprehensive_score'] = (
                    (0.3 * rating_score) + (0.5 * distance_score) + (0.2 * delivery_fee_score)
                )

            data = sorted(data, key=lambda x: x['comprehensive_score'], reverse=True)

        else:
            #  Optional: nearest-only filtering ≤ 6 km
            if nearest:
                data = [
                    item for item in data
                    if item['distance'] is not None and float(item['distance']) <= 6
                ]

            # Multi-level sort (nearest, ldf, most_rated)
            if nearest or ldf or most_rated:
                def sort_key(item):
                    distance = float(item.get('distance') or 9999)
                    delivery_fee = float(item.get('delivery_fee', {}).get("5.0", 0.99))
                    rating = float(item.get('rating') or item.get('res_details', {}).get('average_rating') or 0)

                    return (
                        distance if nearest else 0,
                        delivery_fee if ldf else 0,
                        -rating if most_rated else 0
                    )

                data = sorted(data, key=sort_key)

        #  Paginate
        page = self.paginate_queryset(data)
        if page is not None:
                if not page:
                    # Paginated but empty result
                    message = "We’re sorry! No restaurants are available for delivery in your area right now."
                    return Response({
                        "restaurants": [],
                        "message": message,
                        "red_zone": getattr(self, "user_in_red_zone", False)
                    }, status=status.HTTP_200_OK)
                return self.get_paginated_response(page)

        return Response(data)


class BaseRemoteKitchenAllCuisinesAPIView(APIView):
    """
    Returns all unique cuisines used in remote kitchen restaurants.
    """

    def get(self, request):
        # Get cuisines from remote kitchen restaurants
        cuisine_ids = Restaurant.objects.filter(is_remote_Kitchen=True, cuisines__isnull=False)\
            .values_list("cuisines__id", flat=True)\
            .distinct()

        cuisines = RemoteKitchenCuisine.objects.filter(id__in=cuisine_ids)
        serializer = BaseRemoteKitchenCuisineSerializer(cuisines, many=True)

        return Response(serializer.data)



class BaseChatchefRestaurantList(BaseRestaurantListAPIView):
    pagination_class = StandardRemoteKitchenResultsSetPagination
    serializer_class = RemoteKitchenRestaurantSerializer
    
  
    def get_queryset(self):
      query_set = super().get_queryset().filter(is_remote_Kitchen=False)
      cuisine = self.request.query_params.get("cuisine")
      price_lte = self.request.query_params.get("price_lte")
      price_gte = self.request.query_params.get("price_gte")
      most_rated = self.request.query_params.get("most_rated")
      ldf = self.request.query_params.get("ldf")
      search = self.request.query_params.get("search")
      restaurant_search = self.request.query_params.get("restaurant_search")

      if restaurant_search:
          search_ids = Restaurant.objects.filter(
              name__icontains=restaurant_search.lower()
          ).values_list('id', flat=True).distinct()
          
          query_set = query_set.filter(id__in=search_ids)
          
          result_count = query_set.distinct().count()
          log_search_keyword(restaurant_search, user=self.request.user, result_count=result_count)
      
      if search:
          search_ids = MenuItem.objects.filter(
              name__icontains=search.lower()
          ).values_list('restaurant_id', flat=True).distinct()
          
          query_set = query_set.filter(id__in=search_ids)
          
          result_count = query_set.distinct().count()
          log_search_keyword(search, user=self.request.user, result_count=result_count)

      # Filter by cuisine
      if cuisine:
          cuisine_ids = Category.objects.filter(
              name__icontains=cuisine
          ).values_list('restaurant_id', flat=True).distinct()
          print(cuisine_ids, 'cuisine_ids ----> 47')
          query_set = query_set.filter(id__in=cuisine_ids)

      # Filter by price range
      if price_lte and price_gte:
          price_range_ids = MenuItem.objects.filter(
              base_price__gte=price_gte,
              base_price__lte=price_lte
          ).values_list('restaurant_id', flat=True).distinct()
          query_set = query_set.filter(id__in=price_range_ids)

      # Filter by most rated
      if most_rated:
          rating_ids = Rating.objects.order_by("-rating").values_list(
              'menuItem__restaurant_id', flat=True).distinct()

          unique_rating_id = []
          for i in rating_ids:
              if i not in unique_rating_id:
                  unique_rating_id.append(i)

          query_set = query_set.filter(id__in=unique_rating_id)
          

      # Order by lowest delivery fee
      if ldf:
          query_set = query_set.order_by("delivery_fee")
          
      

      return query_set


    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

      # Default values
        distance_km_default = 10
        distance_km = request.query_params.get("km", None)
        distance_min = request.query_params.get("min", None)
        nearest = request.query_params.get("nearest", None)
        comprehensive_ranking = request.query_params.get("comprehensive_ranking", None)

      # Calculate distance_km from distance_min if provided
        if distance_min:
            distance_km = int(distance_min) // 5

        if distance_km is not None:
          try:
                distance_km = float(distance_km)
          except ValueError:
              distance_km = None  # Fallback to None if conversion fails

        serializer = self.get_serializer(queryset, many=True)
        data = serializer.data

      # Filter based on distance_km or default
        if distance_km is not None:
          data = [
              item for item in data
              if item['distance'] is not None and float(item['distance']) <= distance_km
          ]
        else:
            data = [
                item for item in data
                if item['distance'] is not None and float(item['distance']) <= distance_km_default
            ]

      # Further filter for nearest if required
        if nearest:
            data = [
                item for item in data
                if item['distance'] is not None and float(item['distance']) <= 6
            ]
            data = sorted(data, key=lambda x: (x['distance'] is None, x['distance']))
            
        
        if comprehensive_ranking:
            for item in data:
                # Retrieve individual metrics
                rating = float(item.get('rating', 0))  # Assume a default rating of 0
                distance = item.get('distance', 0)
                delivery_fee = item.get('delivery_fee', {}).get("5.0", 0.99)  # Default to 0.99 if missing

                # Calculate individual scores
                rating_score = rating
                distance_score = 10 if distance == 0 else 1 / (distance + 0.1)
                delivery_fee_score = 1 / (delivery_fee + 1)

                # Adjust for missing rating (assume average rating if 0)
                if rating == 0:
                    rating_score = 3  # Default average rating

                # Calculate comprehensive score
                item['comprehensive_score'] = (
                    (0.3 * rating_score) + (0.5 * distance_score) + (0.2 * delivery_fee_score)
                )

            # Sort data by comprehensive score in descending order
            data = sorted(data, key=lambda x: x['comprehensive_score'], reverse=True)


      # Paginate the filtered data
        page = self.paginate_queryset(data)
        if page is not None:
          return self.get_paginated_response(page)

        return Response(data)



class BaseSaveFavoriteView(APIView):

    def get(self, request):
        user = request.user

        # Retrieve all favorite items for the current user
        favorites = Favorite.objects.filter(user=user)
        
        # Serialize the data to return a structured response
        data = [
            {
                "id": favorite.id,
                "menu_item_id": favorite.item.id,
                "menu_item_name": favorite.item.name,
                "restaurant_name": favorite.restaurant.name,
                "saved_at": favorite.created_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            for favorite in favorites
        ]
        
        return Response({"favorites": data}, status=status.HTTP_200_OK)


    def post(self, request):
        user = request.user
        menu_item_id = request.data.get('menu_item_id')

        try:
            menu_item = MenuItem.objects.get(id=menu_item_id)
            restaurant = menu_item.restaurant
        except MenuItem.DoesNotExist:
            return Response({"detail": "Menu item not found."}, status=status.HTTP_404_NOT_FOUND)

        # Check if the item is already in the user's favorites
        if Favorite.objects.filter(user=user, item=menu_item).exists():
            return Response({"detail": "This item is already in your favorites."}, status=status.HTTP_400_BAD_REQUEST)

        # Create a new favorite
        favorite = Favorite.objects.create(user=user, item=menu_item, restaurant=restaurant)
        return Response({"detail": "Item saved as favorite."}, status=status.HTTP_201_CREATED)


class BaseRemoveFavoriteView(APIView):
    def delete(self, request):
        user = request.user
        menu_item_id = request.data.get('menu_item_id')

        try:
            menu_item = MenuItem.objects.get(id=menu_item_id)
        except MenuItem.DoesNotExist:
            return Response({"detail": "Menu item not found."}, status=status.HTTP_404_NOT_FOUND)

        # Check if the item is in the user's favorites
        try:
            favorite = Favorite.objects.get(user=user, item=menu_item)
            favorite.delete()
            return Response({"detail": "Item removed from favorites."}, status=status.HTTP_204_NO_CONTENT)
        except Favorite.DoesNotExist:
            return Response({"detail": "This item is not in your favorites."}, status=status.HTTP_400_BAD_REQUEST)


class BaseFavoriteListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        # Check if user is authenticated (IsAuthenticated permission already checks this)
        user = request.user
        if not user.is_authenticated:
            raise NotAuthenticated("You must be logged in to view your favorites.")

        favorites = Favorite.objects.filter(user=user)
        serialized_favorites = FavoriteSerializer(favorites, many=True, context={'request': request})
        return Response(serialized_favorites.data, status=status.HTTP_200_OK)
    


class BaseFavoriteStatusView(APIView):
    permission_classes = [IsAuthenticated]  
    def get(self, request, item_id):
        user = request.user
        exists = Favorite.objects.filter(user=user, item_id=item_id).exists()
        return Response({"is_favorite": exists}, status=status.HTTP_200_OK)




# class BaseVisitHistoryView(APIView):
#     permission_classes = [IsAuthenticated]

#     def post(self, request, *args, **kwargs):
#         user = request.user
#         restaurant_id = request.data.get('restaurant_id')
#         if not restaurant_id:
#             return Response({'error': 'Restaurant ID is required'}, status=status.HTTP_400_BAD_REQUEST)

#         try:
#             restaurant = Restaurant.objects.get(id=restaurant_id)
#         except Restaurant.DoesNotExist:
#             return Response({'error': 'Restaurant does not exist'}, status=status.HTTP_404_NOT_FOUND)

#         # Check if a record exists for the user, restaurant, and today
#         today = date.today()
#         visit, created = VisitHistory.objects.get_or_create(user=user, restaurant=restaurant, date=today)

#         if created:
#             return Response({'message': 'Visit recorded successfully'}, status=status.HTTP_201_CREATED)
#         else:
#             return Response({'message': 'Visit already recorded for today'}, status=status.HTTP_200_OK)

#     def get(self, request, *args, **kwargs):
#         user = request.user
#         visits = VisitHistory.objects.filter(user=user)
#         serializer = BaseRestaurantWithItemsSerializer(visits, many=True)
#         return Response(serializer.data, status=status.HTTP_200_OK)
    




# class BaseItemVisitHistoryView(APIView):
#     permission_classes = [IsAuthenticated]

#     def post(self, request, *args, **kwargs):
#         user = request.user
#         item_id = request.data.get('item_id')
#         restaurant_id = request.data.get('restaurant_id')
#         if not item_id or not restaurant_id:
#             return Response({'error': 'Item ID and Restaurant ID are required'}, status=status.HTTP_400_BAD_REQUEST)

#         try:
#             item = MenuItem.objects.get(id=item_id)
#             restaurant = Restaurant.objects.get(id=restaurant_id)
#         except (MenuItem.DoesNotExist, Restaurant.DoesNotExist):
#             return Response({'error': 'Item or Restaurant does not exist'}, status=status.HTTP_404_NOT_FOUND)

#         # Check if a record exists for the user, item, restaurant, and today
#         today = date.today()
#         visit, created = ItemVisitHistorySingle.objects.get_or_create(user=user, item=item, restaurant=restaurant, date=today)

#         if created:
#             return Response({'message': 'Item visit recorded successfully'}, status=status.HTTP_201_CREATED)
#         else:
#             return Response({'message': 'Item visit already recorded for today'}, status=status.HTTP_200_OK)

#     def get(self, request, *args, **kwargs):
#         user = request.user
#         visits = ItemVisitHistorySingle.objects.filter(user=user)
#         serializer = BaseItemVisitHistoryDetailSerializer(visits, many=True)
#         return Response(serializer.data, status=status.HTTP_200_OK)  



class BaseVisitHistoryView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        user = request.user
        restaurant_id = request.data.get('restaurant_id')
        if not restaurant_id:
            return Response({'error': 'Restaurant ID is required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            restaurant = Restaurant.objects.get(id=restaurant_id)
        except Restaurant.DoesNotExist:
            return Response({'error': 'Restaurant does not exist'}, status=status.HTTP_404_NOT_FOUND)

        today = date.today()
        visit, created = VisitHistory.objects.get_or_create(user=user, restaurant=restaurant, date=today)

        if created:
            return Response({'message': 'Visit recorded successfully'}, status=status.HTTP_201_CREATED)
        else:
            return Response({'message': 'Visit already recorded for today'}, status=status.HTTP_200_OK)

    def get(self, request, *args, **kwargs):
        user = request.user
        # Get all visited restaurants
        visits = VisitHistory.objects.filter(user=user).select_related('restaurant')
        restaurants_with_items = []

        for visit in visits:
            restaurant = visit.restaurant
            
            # Fetch detailed restaurant data
            restaurant_data = {
                'restaurant_id': restaurant.id,
                'restaurant_name': restaurant.name,
                'address': restaurant.address,
                'phone': restaurant.phone,
                'avatar_image': restaurant.avatar_image,
                # 'banner_image':restaurant.banner_image,
                'is_store_close':restaurant.is_store_close,
                # 'logo':restaurant.logo,
                # 'company': restaurant.company,
                # 'owner':restaurant.owner

                # 'rating': restaurant.rating  # Assuming these fields exist in Restaurant model
            }

            # Fetch visited items with all item data
            items = ItemVisitHistorySingle.objects.filter(
                user=user, restaurant=restaurant
            ).select_related('item')

            visited_items = [
                {
                    'item_id': item.item.id,
                    'name': item.item.name,
                    'description': item.item.description,
                    'base_price': item.item.base_price,
                    # 'images': item.item.images,
                    # 'original_image': item.item.original_image,
                    'is_available':item.item.is_available,
                    'rating':item.item.rating,
                    'category': item.item.category.name if item.item.category else None
                }
                for item in items
            ]
            
            # Add restaurant and visited item details
            restaurant_data['visited_items'] = visited_items
            restaurants_with_items.append(restaurant_data)

        return Response(restaurants_with_items, status=status.HTTP_200_OK)
    


class BaseItemVisitHistoryView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        user = request.user
        item_id = request.data.get('item_id')
        restaurant_id = request.data.get('restaurant_id')
        if not item_id or not restaurant_id:
            return Response({'error': 'Item ID and Restaurant ID are required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            item = MenuItem.objects.get(id=item_id)
            restaurant = Restaurant.objects.get(id=restaurant_id)
        except (MenuItem.DoesNotExist, Restaurant.DoesNotExist):
            return Response({'error': 'Item or Restaurant does not exist'}, status=status.HTTP_404_NOT_FOUND)

        today = date.today()
        visit, created = ItemVisitHistorySingle.objects.get_or_create(
            user=user, item=item, restaurant=restaurant, date=today
        )

        if created:
            return Response({'message': 'Item visit recorded successfully'}, status=status.HTTP_201_CREATED)
        else:
            return Response({'message': 'Item visit already recorded for today'}, status=status.HTTP_200_OK)

    def get(self, request, *args, **kwargs):
        user = request.user
        # Get all visited items for this user
        item_visits = ItemVisitHistorySingle.objects.filter(user=user).select_related('item', 'restaurant')

        visited_items = [
            {
                'item_id': visit.item.id,
                'name': visit.item.name,
                'description': visit.item.description,
                'base_price': visit.item.base_price,
                # 'images': item.item.images,
                # 'original_image': item.item.original_image,
                'is_available':visit.item.is_available,
                'rating':visit.item.rating,
                'category': visit.item.category.name if visit.item.category else None,
                
                'item_id': visit.item.id,
                'item_name': visit.item.name,
                'restaurant_id': visit.restaurant.id,
                'restaurant_name': visit.restaurant.name,
                'date': visit.date
            }
            for visit in item_visits
        ]

        return Response(visited_items, status=status.HTTP_200_OK)
    


class BaseAddressManagementView(APIView):
    permission_classes = [IsAuthenticated]  

    def get(self, request, pk=None):
        user = request.user
        if pk:
            try:
                address = UserAddress.objects.get(pk=pk, user=user)  
                serializer = UserAddressSerializer(address)
                return Response(serializer.data, status=status.HTTP_200_OK)
            except UserAddress.DoesNotExist:
                return Response({"error": "Address not found."}, status=status.HTTP_404_NOT_FOUND)
        else:
            addresses = UserAddress.objects.filter(user=user)  # Get all addresses for the user
            serializer = UserAddressSerializer(addresses, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request, pk=None):
        user = request.user
        if pk:  # Change default address
            try:
                # Unset previous default address for the user
                UserAddress.objects.filter(user=user, is_default=True).update(is_default=False)
                # Set the new default
                address = UserAddress.objects.get(pk=pk, user=user)
                address.is_default = True
                address.save()
                return Response({"message": "Default address updated successfully."}, status=status.HTTP_200_OK)
            except UserAddress.DoesNotExist:
                return Response({"error": "Address not found."}, status=status.HTTP_404_NOT_FOUND)
        else:  # Create a new address
            data = request.data
            data["user"] = user.id  # Assign the authenticated user
            serializer = UserAddressSerializer(data=data, context={"request": request})
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def put(self, request, pk=None):
        """
        PUT /addresses/<pk>/update/ -> Update an address (user-specific).
        """
        user = request.user
        try:
            address = UserAddress.objects.get(pk=pk, user=user)
            serializer = UserAddressSerializer(address, data=request.data, partial=True, context={"request": request})
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except UserAddress.DoesNotExist:
            return Response({"error": "Address not found."}, status=status.HTTP_404_NOT_FOUND)

    def delete(self, request, pk=None):
        """
        DELETE /addresses/<pk>/delete/ -> Delete an address (user-specific).
        """
        user = request.user
        try:
            address = UserAddress.objects.get(pk=pk, user=user)
            address.delete()
            return Response({"message": "Address deleted successfully."}, status=status.HTTP_204_NO_CONTENT)
        except UserAddress.DoesNotExist:
            return Response({"error": "Address not found."}, status=status.HTTP_404_NOT_FOUND)
        


class BaseDefaultAddressView(APIView):
    permission_classes = [IsAuthenticated]  

    def get(self, request):
        user = request.user
        try:
            address = UserAddress.objects.get(user=user, is_default=True)
            serializer = UserAddressSerializer(address)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except UserAddress.DoesNotExist:
            return Response({"error": "Default address not found."}, status=status.HTTP_404_NOT_FOUND)
        



class BaseRestaurantFavoriteView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk=None):
        user = request.user
        if pk:  
            try:
                bookmark = FavoriteRestaurant.objects.get(pk=pk, user=user)
                serializer = RestaurantFavoriteSerializer(bookmark, context={'request': request})
                return Response(serializer.data, status=status.HTTP_200_OK)
            except FavoriteRestaurant.DoesNotExist:
                return Response({"error": "Bookmark not found."}, status=status.HTTP_404_NOT_FOUND)
        else:  # Get all bookmarks
            bookmarks = FavoriteRestaurant.objects.filter(user=user)
            serializer = RestaurantFavoriteSerializer(bookmarks, many=True, context={'request': request})
            return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        user = request.user
        data = request.data
        data["user"] = user.id  
        serializer = RestaurantFavoriteSerializer(data=data, context={"request": request})
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk=None):
        user = request.user
        try:
            bookmark = FavoriteRestaurant.objects.get(pk=pk, user=user)
            bookmark.delete()
            return Response({"message": "Bookmark deleted successfully."}, status=status.HTTP_204_NO_CONTENT)
        except FavoriteRestaurant.DoesNotExist:
            return Response({"error": "Bookmark not found."}, status=status.HTTP_404_NOT_FOUND)
        


class BaseCountdownView(APIView):
    END_TIME = None  

    def get(self, request):
        start_time = now()
        if not BaseCountdownView.END_TIME:
            BaseCountdownView.END_TIME = start_time + timedelta(days=60)

        delta = BaseCountdownView.END_TIME - now()
        if delta.total_seconds() > 0:  
            days, seconds = divmod(delta.total_seconds(), 86400)
            hours, seconds = divmod(seconds, 3600)
            minutes, seconds = divmod(seconds, 60)
        else:
            days, hours, minutes, seconds = 0, 0, 0, 0  

        return Response({
            "start_time": start_time.isoformat(),
            "end_time": BaseCountdownView.END_TIME.isoformat(),
            "remaining_time": {
                "days": int(days),
                "hours": int(hours),
                "minutes": int(minutes),
                "seconds": int(seconds),
            }
        }, status=status.HTTP_200_OK)
    




# google maps 

class GoogleAutocompleteService:
    """Service class for handling Google Maps API interactions"""

    @staticmethod
    def get_autocomplete_suggestions(query):
        """Fetch place autocomplete suggestions from Google Maps API"""
        params = {
            "input": query,
            "key": GOOGLE_MAPS_API_KEY,
            "types": "geocode",
            "language": "en",
            "components": "country:BD"
        }

        response = requests.get("https://maps.googleapis.com/maps/api/place/autocomplete/json", params=params)
        if response.status_code == 200:
            return response.json()
        return {"error": "Failed to fetch autocomplete suggestions"}

    @staticmethod
    def get_current_location():
        """Fetch the user's current location using Google's Geolocation API"""
        params = {
            "key": GOOGLE_MAPS_API_KEY
        }

        response = requests.post("https://www.googleapis.com/geolocation/v1/geolocate", params=params)
        if response.status_code == 200:
            return response.json()
        return {"error": "Failed to fetch current location"}

    @staticmethod
    def reverse_geocode(lat, lng):
        """Convert latitude & longitude into a human-readable address"""
        params = {
            "latlng": f"{lat},{lng}",
            "key": GOOGLE_MAPS_API_KEY
        }

        response = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=params)
        if response.status_code == 200:
            return response.json()
        return {"error": "Failed to reverse geocode"}

    @staticmethod
    def get_place_details(place_id):
        """Fetch place details using a location ID (place_id)"""
        params = {
            "place_id": place_id,
            "key": GOOGLE_MAPS_API_KEY,
            "fields": "address_component,formatted_address,geometry"

        }

        response = requests.get("https://maps.googleapis.com/maps/api/place/details/json", params=params)
        if response.status_code == 200:
            return response.json()
        return {"error": "Failed to fetch place details"}

class BaseGoogleAutocompleteView(APIView):
    """API to get autocomplete suggestions"""
    def get(self, request):
        query = request.GET.get("query", "")
        if not query:
            return Response({"error": "Query parameter is required"}, status=status.HTTP_400_BAD_REQUEST)
        
        data = GoogleAutocompleteService.get_autocomplete_suggestions(query)
        return Response(data, status=status.HTTP_200_OK)

class BaseGLocateMeView(APIView):
    """API to get the user's current location"""
    def get(self, request):
        data = GoogleAutocompleteService.get_current_location()
        return Response(data, status=status.HTTP_200_OK)

class BaseGReverseGeocodeView(APIView):
    """API to convert coordinates into a readable address"""
    def get(self, request):
        lat = request.GET.get("lat")
        lng = request.GET.get("lng")

        if not lat or not lng:
            return Response({"error": "Latitude and Longitude are required"}, status=status.HTTP_400_BAD_REQUEST)

        data = GoogleAutocompleteService.reverse_geocode(lat, lng)
        return Response(data, status=status.HTTP_200_OK)

class BaseGPlaceDetailsView(APIView):
    """API to fetch detailed location information using place_id"""
    def get(self, request):
        place_id = request.GET.get("place_id")

        if not place_id:
            return Response({"error": "place_id parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        data = GoogleAutocompleteService.get_place_details(place_id)

        return Response(data, status=status.HTTP_200_OK)




    def get_customer_order_count(self, user, restaurant):
        return Order.objects.filter(customer=user, restaurant=restaurant, status="completed").count()

    def get_delivery_fee(self, user, restaurant):
        order_count = self.get_customer_order_count(user, restaurant)

        rule = DeliveryFeeRule.objects.filter(restaurants=restaurant).first()

        if not rule:
            rule = DeliveryFeeRule.objects.filter(restaurants__isnull=True).first()

        if not rule:
            return 0 

        if order_count == 0:
            return rule.first_order_fee
        elif order_count == 1:
            return rule.second_order_fee
        else:
            return rule.third_or_more_fee

    def post(self, request, *args, **kwargs):
        user = request.user  
        restaurant_id = request.data.get("restaurant_id")
        restaurant = get_object_or_404(Restaurant, id=restaurant_id)

        delivery_fee = self.get_delivery_fee(user, restaurant)

        order = Order.objects.create(
            customer=user,
            restaurant=restaurant,
            delivery_fee=delivery_fee,
            status="pending"  
        )

        return Response({
            "order_id": order.id,
            "delivery_fee": str(delivery_fee)
        }, status=status.HTTP_201_CREATED)