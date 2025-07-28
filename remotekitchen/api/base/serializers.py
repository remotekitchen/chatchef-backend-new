import datetime

import requests
from rest_framework import serializers

from billing.models import RestaurantFee
from chatchef.settings.defaults import mapbox_api_key
from food.api.base.serializers import (BaseRestaurantGETSerializer,
                                       TimeTableModelSerializer, BaseRestaurantSerializer, BaseLocationSerializer)
from food.models import Restaurant, TimeTable, Location, VisitHistory, ItemVisitHistorySingle,RemoteKitchenCuisine
from remotekitchen.models import Cuisine
from food.api.base.serializers import (BaseMenuItemSerializer)
from remotekitchen.models import Favorite , FavoriteRestaurant
from accounts.models import UserAddress
from math import radians, cos, sin, asin, sqrt


class BaseRemoteKitchenCuisineSerializer(serializers.ModelSerializer):
    class Meta:
        model = RemoteKitchenCuisine
        fields = "__all__"

# serializers.py
class BaseCuisineSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cuisine
        fields = "__all__"
 

class RemoteKitchenRestaurantSerializer(BaseRestaurantGETSerializer):
    delivery_charge = serializers.SerializerMethodField()
    time_to_open = serializers.SerializerMethodField()
    res_details = serializers.SerializerMethodField()
    location_details = serializers.SerializerMethodField()
    average_delivery_time = serializers.SerializerMethodField()

    class Meta(BaseRestaurantGETSerializer.Meta):
        fields = [
            'id',
            'name',
            'latitude',
            'longitude',
            'avatar_image',
            'delivery_charge',
            'distance',
            'time_to_open',
            'res_details',
            'location_details',
            "average_delivery_time"
            # 'delivery_time'
        ]
        
    
  
    
    def get_res_details(self, obj: Restaurant):
        return BaseRestaurantSerializer(obj).data
    
    def get_location_details(self, obj: Restaurant):
      locations = obj.locations.all()  
      return BaseLocationSerializer(locations, many=True).data

    def get_time_to_open(self, obj: Restaurant):
        def _timezone(timezone_str):
            timezone = {
                '-12:00': -12,
                '-11:00': -11,
                '-10:00': -10,
                '-09:00': -9,
                '-08:00': -8,
                '-07:00': -7,
                '-06:00': -6,
                '-05:00': -5,
                '-04:00': -4,
                '-03:00': -3,
                '-02:00': -2,
                '-01:00': -1,
                '+00:00': +0,
                '+01:00': +1,
                '+02:00': +2,
                '+03:00': +3,
                '+04:00': +4,
                '+05:00': +5,
                '+06:00': +6,
                '+07:00': +7,
                '+08:00': +8,
                '+09:00': +9,
                '+10:00': +10,
                '+11:00': +11,
                '+12:00': +12,
            }

            current_utc = datetime.datetime.utcnow()
            utc_converted = datetime.timedelta(hours=timezone[timezone_str])
            converted_time = current_utc + utc_converted

            return converted_time

        current_day = _timezone(obj.timezone).strftime("%a").lower()

        time = None
        open_hour = obj.opening_hours.filter(day_index=current_day).first()
        if open_hour:
            times = TimeTable.objects.filter(opening_hour=open_hour.id)
            time = TimeTableModelSerializer(times, many=True).data
            return time
        return time

    def get_delivery_charge(self, obj: Restaurant):
        request = self.context.get('request')
        if not request:
            return None  
        
        # Calculate the distance
        distance = self.get_distance(obj)


        # Ensure distance is a valid float
        try:
            distance = float(distance)
        except (TypeError, ValueError):
            return None  # or return a default fee if needed

        # Delivery fee logic
        if distance < 3.00:
            return 50
        elif distance < 6.00:
            return 10
        elif distance < 9.00:
            return 20
        elif distance < 15.00:
            return 30
        elif distance < 20.00:
            return 40
        else:
            return 50
    

    def get_distance(self, obj: Restaurant):
        request = self.context['request']
        user_lat = request.query_params.get("lat")
        user_lng = request.query_params.get("lng")
        distance = self.get_distance_between_coords(
            user_lat,
            user_lng,
            obj.latitude,
            obj.longitude
        )
        return distance

    def get_average_delivery_time(self, obj):
        average_speed_kmh = 10.64  
        request = self.context.get('request')
        
        if not request:
            return None  
        
        distance = self.get_distance(obj)

        # Check if distance is None or invalid
        if distance is None or distance <= 0:
            return 0  

        estimated_time_minutes = (distance / average_speed_kmh) * 60

        return round(estimated_time_minutes, 2)  


    def get_distance_between_coords(self, lat1, lng1, lat2, lng2):
        return self.get_distance_mapbox(lat1, lng1, lat2, lng2)

    # def get_distance_mapbox(self, lat1, lng1, lat2, lng2):
    #     url = f"https://api.mapbox.com/directions/v5/mapbox/driving/{lng1},{lat1};{lng2},{lat2}"
    #     params = {
    #         "access_token": mapbox_api_key,
    #         "geometries": "geojson",
    #     }

    #     response = requests.get(url, params=params)

    #     if response.status_code == 200:
    #         data = response.json()
    #         if data["routes"]:
    #             distance_meters = data["routes"][0]["distance"]
    #             distance_km = distance_meters / 1000
    #             return float("{0:.2f}".format(distance_km))
    #         else:
    #             return None
    #     else:
    #         return None
    def get_distance_mapbox(self, lat1, lng1, lat2, lng2):
        """
        Calculates the great-circle distance (in kilometers) between two points
        using the Haversine formula. All inputs are cast to float to prevent errors.
        """
        try:
            # Cast all inputs to float
            lat1 = float(lat1)
            lng1 = float(lng1)
            lat2 = float(lat2)
            lng2 = float(lng2)

            # Radius of Earth in kilometers
            R = 6371.0

            # Convert degrees to radians
            lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])

            # Differences
            dlat = lat2 - lat1
            dlng = lng2 - lng1

            # Haversine calculation
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlng / 2)**2
            c = 2 * asin(sqrt(a))
            distance_km = R * c
            
            print("distance_km>>>>>>>>", distance_km)

            return float("{0:.2f}".format(distance_km))

        except Exception as e:
            print(f"Error calculating haversine distance: {e}")
            return None


class FavoriteSerializer(serializers.ModelSerializer):
    item = BaseMenuItemSerializer()
    restaurant = RemoteKitchenRestaurantSerializer()

    class Meta:
        model = Favorite
        fields = ['id', 'user', 'item', 'restaurant', 'created_at']



class UserAddressSerializer(serializers.ModelSerializer):
   class Meta:
         model = UserAddress
         fields = "__all__"
         extra_kwargs = {"user": {"read_only": True}}



class RestaurantFavoriteSerializer(serializers.ModelSerializer):
    items = serializers.SerializerMethodField()
    restaurant_details = serializers.SerializerMethodField()

    class Meta:
        model = FavoriteRestaurant
        fields = ['id', 'user', 'restaurant', 'created_at', 'items', 'restaurant_details']

    def get_items(self, obj):
        user = obj.user
        restaurant = obj.restaurant
        saved_items = Favorite.objects.filter(user=user, restaurant=restaurant)
        return [
            {
                'item_id': item.item.id,
                'item_name': item.item.name,
                'saved_at': item.created_at,
                'base_price': getattr(item.item, 'base_price', None),  # Fetch base_price from the related MenuItem
            }
            for item in saved_items
        ]

    def get_restaurant_details(self, obj):
        restaurant = obj.restaurant
        owner = restaurant.owner

        print("restaurant>>", restaurant)
        return {
            'name': restaurant.name,
            'owner': {
                'username': owner.username,  
                'email': owner.email,       
            },
            'location': restaurant.location,
            'avatar_image': restaurant.avatar_image,
            'phone': restaurant.phone,
            'email': restaurant.email,
        }
