from drf_writable_nested import WritableNestedModelSerializer
from rest_framework import serializers
from django.utils.timezone import now
from chatchef.settings.defaults import mapbox_api_key
import requests
from billing.models import RestaurantFee

from core.api.serializers import AddressSerializer, BaseSerializer
from core.utils import get_logger
from food.models import (Category, CuisineType, Image, Location, Menu,
                         MenuItem, ModifierGroup, ModifierGroupOrder,
                         ModifiersItemsOrder, OpeningHour, Restaurant,
                         RestaurantOMSUsagesTracker, SpecialHour, TimeTable, RemoteKitchenCuisine)
from food.utils import is_closed
from marketing.api.base.serializers import RatingSerializer
from marketing.models import Rating
from billing.models import Order
from marketing.api.base.serializers import BaseBogoSerializer
from django.db.models import Sum

from marketing.models import Bogo, BxGy

logger = get_logger()


class BaseCuisineTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = CuisineType
        fields = '__all__'


class TimeTableModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeTable
        fields = ['id', 'start_time', 'end_time']


class BaseOpeningHoursSerializer(WritableNestedModelSerializer):
    opening_hour = TimeTableModelSerializer(many=True, required=True)

    class Meta:
        model = OpeningHour
        fields = '__all__'


class BaseSpecialHoursSerializer(serializers.ModelSerializer):

    class Meta:
        model = SpecialHour
        fields = '__all__'


class BaseWritableNestedSerializer(serializers.ModelSerializer):
    """
        This serializer is to be inherited by those serializers that have m2m nested serializer fields and those need
        to be writable as well
    """
    # opening_hours = BaseOpeningHoursSerializer(many=True)
    m2m_field_name = "opening_hours"
    m2m_model = OpeningHour

    def create(self, validated_data):
        field_value = validated_data.pop(self.m2m_field_name, [])
        obj = super().create(validated_data)
        objects = []

        for val in field_value:
            objects.append(self.m2m_model(**val))
        created_m2m = self.m2m_model.objects.bulk_create(objects)

        getattr(obj, self.m2m_field_name).add(*[m2m.id for m2m in created_m2m])
        return obj


class BaseImageSerializer(serializers.ModelSerializer):
    working_url = serializers.ReadOnlyField()

    class Meta:
        model = Image
        fields = '__all__'


class BaseRemoteKitchenCuisineSerializer(serializers.ModelSerializer):
    class Meta:
        model = RemoteKitchenCuisine
        fields = ["id", "name", "icon"]  


class BaseRestaurantSerializer(WritableNestedModelSerializer):
    opening_hours = BaseOpeningHoursSerializer(many=True, required=False)
    avatar_image = BaseImageSerializer(required=False, allow_null=True)
    banner_image = BaseImageSerializer(
        many=True, read_only=True, required=False)
    location_details = serializers.SerializerMethodField()
    
    location_cnt = serializers.SerializerMethodField()
    menu_cnt = serializers.SerializerMethodField()
    category_cnt = serializers.SerializerMethodField()
    item_cnt = serializers.SerializerMethodField()
    address = AddressSerializer(required=False)
    monthly_sales_count = serializers.SerializerMethodField()  
    average_ticket_size = serializers.SerializerMethodField()
    total_gross_revenue = serializers.SerializerMethodField()
    total_sales_count = serializers.SerializerMethodField()
    total_order_count = serializers.SerializerMethodField()  
    cuisines = BaseRemoteKitchenCuisineSerializer(many=True, read_only=True)
    distance = serializers.SerializerMethodField()
    average_delivery_time = serializers.SerializerMethodField()
    has_bogo_offers = serializers.SerializerMethodField()
    has_bxgy_offers = serializers.SerializerMethodField()
    delivery_charge = serializers.SerializerMethodField()
    m2m_field_name = 'opening_hours'
    m2m_model = OpeningHour


    class Meta:
        model = Restaurant
        fields = '__all__'

    def get_distance(self, obj: Restaurant):
        request = self.context.get('request', None)  

        if not request:
            return None  
    

        
       
        user_lat = request.query_params.get("lat")
        user_lng = request.query_params.get("lng")
        print("all",  obj.latitude, obj.longitude, user_lat, user_lng)
        try:
            user_lat = float(user_lat)
            user_lng = float(user_lng)
            rest_lat = float(obj.latitude)
            rest_lng = float(obj.longitude)
        except (TypeError, ValueError):
            return None

        distance = self.get_distance_between_coords(user_lat, user_lng, rest_lat, rest_lng)
        return distance
    
    def get_distance_between_coords(self, lat1, lon1, lat2, lon2):
        import math
        R = 6371.0  # km
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return round(distance, 2)
    
    def get_has_bogo_offers(self, obj):
        return Bogo.objects.filter(restaurant=obj).exists()
    
    def get_has_bxgy_offers(self, obj):
        return BxGy.objects.filter(restaurant=obj).exists()

    # def get_distance_between_coords(self, lat1, lng1, lat2, lng2):
    #     return self.get_distance_mapbox(lat1, lng1, lat2, lng2)

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
    
    
    def get_delivery_charge(self, obj):
        request = self.context.get('request')
        if not request:
            return None  
        
        # Calculate the distance
        distance = 9999 # Call the method directly


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

    def get_average_delivery_time(self, obj):
        average_speed_kmh = 10.64  
        request = self.context.get('request')
        if not request:
            return None  

        distance =  0  # Ensure distance is at least 0

        print(type(distance), type(average_speed_kmh), 'distance and speed')

        if distance <= 0:
            return 0  

        estimated_time_minutes = (distance / average_speed_kmh) * 60

        return round(estimated_time_minutes, 2)  # Return the estimated time in minutes



    # def get_delivery_charge(self, obj: Restaurant):

    #     data = {}
    #     fees = RestaurantFee.objects.filter(restaurant=obj.id)
    #     for fee in fees:
    #         data[f"{fee.max_distance}"] = fee.delivery_fee

    #     return data    
    

    def update(self, instance, validated_data):
        cuisines = validated_data.pop('cuisine_ids', [])
        instance.cuisines.set(cuisines)
        return super().update(instance, validated_data)
    
    def get_location_details(self, obj: Restaurant):
      locations = obj.locations.all()  
      return BaseLocationSerializer(locations, many=True).data
    
    def get_location_cnt(self, obj: Restaurant):
        return obj.locations.count()

    def get_menu_cnt(self, obj: Restaurant):
        return obj.menu_set.count()

    def get_category_cnt(self, obj: Restaurant):
        return obj.category_set.count()

    def get_item_cnt(self, obj: Restaurant):
        return obj.menuitem_set.count()
    
    def get_monthly_sales_count(self, obj):

        from django.utils.timezone import now
        from django.db.models import Count

        start_of_month = now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        total_orders = Order.objects.filter(
            restaurant_id=obj.id, created_date__gte=start_of_month
        ).count()

        return total_orders


    def get_total_sales_count(self, obj):
        return Order.objects.filter(restaurant_id=obj.id).count()

    def get_total_gross_revenue(self, obj):
        total_revenue = Order.objects.filter(
            restaurant_id=obj.id
        ).aggregate(total=Sum('total'))['total'] or 0

        return float("{0:.2f}".format(total_revenue))


    def get_average_ticket_size(self, obj):
        total_revenue = Order.objects.filter(
            restaurant_id=obj.id
        ).aggregate(total=Sum('total'))['total'] or 0

        total_orders = Order.objects.filter(restaurant_id=obj.id).count()

        if total_orders == 0:
            return 0

        avg_ticket = total_revenue / total_orders

        return float("{0:.2f}".format(avg_ticket))




    def get_total_order_count(self, obj):  
        """Calculate the total number of orders for this restaurant (all-time)."""
        return Order.objects.filter(restaurant_id=obj.id).count()
    
   

class BaseRestaurantGETSerializer(BaseRestaurantSerializer):
    class Meta(BaseRestaurantSerializer.Meta):
        fields = ['id', 'name']


class BaseLocationSerializer(WritableNestedModelSerializer):
    opening_hours = BaseOpeningHoursSerializer(many=True, required=True)
    restaurant_name = serializers.SerializerMethodField()
    address = AddressSerializer(required=False)
    m2m_field_name = 'opening_hours'
    m2m_model = OpeningHour,

    class Meta:
        model = Location
        fields = '__all__'

    def get_restaurant_name(self, obj: Location):
        try:
            return obj.restaurant.name
        except:
            return ""


class ALlLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Location
        fields = ['id', 'slug', 'name', 'details']


class BaseMenuSerializer(WritableNestedModelSerializer):
    category_cnt = serializers.SerializerMethodField()
    item_cnt = serializers.SerializerMethodField()
    opening_hours = BaseOpeningHoursSerializer(many=True, required=False)
    special_hours = BaseSpecialHoursSerializer(many=True, required=False)
    is_closed = serializers.SerializerMethodField(
        read_only=True, required=False)
    restaurant_name = serializers.SerializerMethodField()
    location_names = serializers.SerializerMethodField()
    categories = serializers.PrimaryKeyRelatedField(queryset=Category.objects.all(), many=True, write_only=True,
                                                    source='category_set', required=False)

    # m2m_field_name = 'is_closed'

    class Meta:
        model = Menu
        fields = '__all__'
        extra_kwargs = {
            'company': {
                'required': False
            },
            'cuisine_types': {
                'required': False,
                'allow_empty': True
            },
            "is_closed": {
                'required': False
            }
        }

    def get_category_cnt(self, obj: Menu):
        return obj.category_set.count()

    def get_item_cnt(self, obj: Menu):
        return obj.menuitem_set.count()

    def get_restaurant_name(self, obj: Menu):
        try:
            return obj.restaurant.name
        except:
            return ""

    def get_location_names(self, obj: Menu):
        try:
            return ','.join(list(obj.locations.all().values_list('name', flat=True)))
        except:
            return ""

    def get_is_closed(self, obj: Menu):
        status = is_closed(obj)
        return status


class BaseCategorySerializer(serializers.ModelSerializer):
    menuitem_cnt = serializers.SerializerMethodField()
    menuitem_set = serializers.PrimaryKeyRelatedField(
        many=True, read_only=True)
    menu_name = serializers.SerializerMethodField()
    restaurant_name = serializers.SerializerMethodField()
    location_names = serializers.SerializerMethodField()

    class Meta:
        model = Category
        fields = '__all__'

    def get_menuitem_cnt(self, obj: Category):
        return obj.menuitem_set.count()

    def get_menu_name(self, obj: Category):
        try:
            return obj.menu.title
        except:
            return ""

    def get_restaurant_name(self, obj: Category):
        try:
            return obj.restaurant.name
        except:
            return ""

    def get_location_names(self, obj: Category):
        try:
            return ','.join(list(obj.locations.all().values_list('name', flat=True)))
        except:
            return ""


class ItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = MenuItem
        fields = ['id', 'name', 'description',
                  'base_price', 'virtual_price', 'currency', 'images', 'category', 'is_available', 'is_available_today', 'is_vegan',
                  'is_vegetarian', 'is_glutenfree']


class BaseModifiersItemsOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = ModifiersItemsOrder
        fields = '__all__'


class BaseModifiersItemsOrderGETSerializer(BaseModifiersItemsOrderSerializer):
    menu_item = ItemSerializer(read_only=True)


class ModifierGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = ModifierGroup
        fields = '__all__'


class UsedByModifiersItems(ItemSerializer):
    class Meta(ItemSerializer.Meta):
        fields = ["id", "name"]


class BaseModifierGroupSerializer(ModifierGroupSerializer):
    modifiers_item_list = BaseModifiersItemsOrderGETSerializer(many=True)
    modifier_items = ItemSerializer(many=True, read_only=True)
    # used_by = UsedByModifiersItems(many=True, read_only=True)

    # class Meta:
    #     model = ModifierGroup
    #     fields = '__all__'
    #     extra_kwargs = {
    #         'modifier_items': {
    #             'allow_empty': True
    #         },
    #         'used_by': {
    #             'allow_empty': True
    #         }
    #     }


class BaseModifierGroupOrderSerializer(serializers.ModelSerializer):
    name = serializers.SerializerMethodField()

    class Meta:
        model = ModifierGroupOrder
        fields = '__all__'

    def get_name(self, obj: ModifierGroupOrder):
        return f"{obj.modifier_group.name}"


class BaseModifierGroupOrderGETSerializer(BaseModifierGroupOrderSerializer):
    modifier_group = BaseModifierGroupSerializer()
    menu_details = serializers.SerializerMethodField()

    def get_menu_details(self, obj: ModifierGroupOrder):
        return {"id": obj.menu_item.id, "name": obj.menu_item.name, "price": obj.menu_item.base_price}


class BaseMenuItemSerializer(WritableNestedModelSerializer):
    ratings = RatingSerializer(many=True, read_only=True)
    images = BaseImageSerializer(many=True, required=False)
    modifiergroup_set = BaseModifierGroupSerializer(many=True, required=False)
    modifiergrouporder_set = BaseModifierGroupOrderGETSerializer(
        many=True, required=False)
    # imageFiles = ListField(child=FileField(), write_only=True, required=False)
    original_image = BaseImageSerializer(required=False)
    menu_name = serializers.SerializerMethodField()
    category_names = serializers.SerializerMethodField()
    m2m_field_name = 'images'
    m2m_model = Image
    like_count = serializers.SerializerMethodField()
    is_current_user_liked = serializers.SerializerMethodField()

    class Meta:
        model = MenuItem
        fields = '__all__'

    def get_menu_name(self, obj: MenuItem):
        try:
            return obj.menu.title
        except:
            return ""

    def get_category_names(self, obj: MenuItem):
        try:
            return ','.join(list(obj.category.all().values_list('name', flat=True)))
        except:
            return ""

    def get_like_count(self, obj: MenuItem):
        rating_count = 0
        if Rating.objects.filter(menuItem=obj.id).exists():
            rating_count = Rating.objects.get(menuItem=obj.id).rating
        return rating_count

    def get_is_current_user_liked(self, obj: MenuItem):
        request = self.context.get('request')

        if request.user.is_authenticated:
            if Rating.objects.filter(menuItem=obj.id).exists():
                return Rating.objects.get(
                    menuItem=obj.id).user.filter(id=request.user.id).exists()
        return False


class BaseOpeningHourSerializer(serializers.ModelSerializer):
    opening_hour = TimeTableModelSerializer(many=True, read_only=True)

    class Meta:
        model = OpeningHour
        fields = '__all__'


class BaseCategoryDetailSerializer(BaseCategorySerializer):
    menuitem_set = BaseMenuItemSerializer(many=True, read_only=True)
    location_details = BaseLocationSerializer(
        many=True, read_only=True, source='locations')


class BaseMenuDetailSerializer(BaseMenuSerializer):
    category_set = BaseCategorySerializer(many=True, read_only=True)
    # modifiergroup_set = BaseModifierGroupSerializer(many=True, read_only=True)
    # menuitem_set = BaseMenuItemSerializer(many=True, read_only=True)
    opening_hours = BaseOpeningHourSerializer(many=True, required=False)


class BaseExcelMenuUploadSerializer(BaseSerializer):
    menu_file = serializers.FileField()
    restaurant = serializers.CharField()
    locations = serializers.ListField(required=False, allow_empty=True)
    name = serializers.CharField()


class BaseRestaurantDetailSerializer(BaseRestaurantSerializer):
    locations = BaseLocationSerializer(many=True, read_only=True)


class BaseLocationDetailSerializer(BaseLocationSerializer):
    restaurant_details = BaseRestaurantSerializer(
        source='restaurant', read_only=True)


class TimeTableSerializer(serializers.Serializer):
    id = serializers.CharField(required=False)
    start_time = serializers.TimeField()
    end_time = serializers.TimeField()
    is_delete = serializers.BooleanField()


class OpeningHourSerializer(serializers.Serializer):
    opening_hour_id = serializers.CharField(required=False)
    day_index = serializers.CharField()
    is_close = serializers.BooleanField()
    times = TimeTableSerializer(many=True)


class BaseMenuUpdateOpeningHourSerializer(serializers.Serializer):
    opening_hour = OpeningHourSerializer(many=True)


class OpeningHourModelSerializer(serializers.ModelSerializer):
    opening_hour = TimeTableModelSerializer(many=True)

    class Meta:
        model = OpeningHour
        fields = ['id', 'day_index', 'is_close', 'opening_hour']


# New Serializers


class BaseMenuItemsGetForHomePageSerializer(WritableNestedModelSerializer):
    class Meta:
        model = Menu
        fields = '__all__'
        extra_kwargs = {
            'company': {
                'required': False
            },
            'cuisine_types': {
                'required': False,
                'allow_empty': True
            },
            "is_closed": {
                'required': False
            }
        }


class BaseMenuItemWithoutModifierSerializer(WritableNestedModelSerializer):
    images = BaseImageSerializer(many=True, required=False)
    original_image = BaseImageSerializer(required=False)
    menu_name = serializers.SerializerMethodField()
    category_names = serializers.SerializerMethodField()
    m2m_field_name = 'images'
    m2m_model = Image
    like_count = serializers.SerializerMethodField()
    is_current_user_liked = serializers.SerializerMethodField()
    has_modifier = serializers.SerializerMethodField()

    class Meta:
        model = MenuItem
        fields = '__all__'

    def get_modifiergroup_set(self, obj):
        return []

    def get_menu_name(self, obj: MenuItem):
        try:
            return obj.menu.title
        except:
            return ""

    def get_category_names(self, obj: MenuItem):
        try:
            return ','.join(list(obj.category.all().values_list('name', flat=True)))
        except:
            return ""

    def get_like_count(self, obj: MenuItem):
        rating_count = 0
        if Rating.objects.filter(menuItem=obj.id).exists():
            rating_count = Rating.objects.get(menuItem=obj.id).rating
        return rating_count

    def get_is_current_user_liked(self, obj: MenuItem):
        request = self.context.get('request')

        if request.user.is_authenticated:
            if Rating.objects.filter(menuItem=obj.id).exists():
                return Rating.objects.get(
                    menuItem=obj.id).user.filter(id=request.user.id).exists()
        return False

    def get_has_modifier(self, obj: MenuItem):
        has_modifier = ModifierGroup.objects.filter(used_by=obj.id).exists()
        if not has_modifier:
            has_modifier = ModifierGroupOrder.objects.filter(
                menu_item=obj.id).exists()
        return has_modifier


class BaseMenuItemDetailWithoutModifierSerializer(BaseMenuItemsGetForHomePageSerializer):
    # category_set = BaseCategorySerializer(many=True, read_only=True)
    # modifiergroup_set = BaseModifierGroupSerializer(many=True, read_only=True)
    menuitem_set = BaseMenuItemWithoutModifierSerializer(
        many=True, read_only=True)
    # opening_hours = BaseOpeningHourSerializer(many=True, required=False)


class BaseMenuItemInflationSerializer(BaseRestaurantSerializer):
    class Meta(BaseRestaurantSerializer.Meta):
        fields = ['inflation_percent']


class BaseExcelModifierUploadSerializer(BaseSerializer):
    modifiers_file = serializers.FileField()
    restaurant = serializers.CharField()
    location = serializers.CharField()


class BaseRestaurantOMSUsagesTrackerSerializer(serializers.ModelSerializer):
    restaurant_name = serializers.SerializerMethodField()
    location_name = serializers.SerializerMethodField()

    class Meta:
        model = RestaurantOMSUsagesTracker
        fields = '__all__'

    def get_restaurant_name(self, obj: RestaurantOMSUsagesTracker):
        return f"{obj.restaurant.name}"

    def get_location_name(self, obj: RestaurantOMSUsagesTracker):
        return f"{obj.location.name}"


class BaseListOfRestaurantsForOrderCallSerializer(BaseRestaurantGETSerializer):
    class Meta(BaseRestaurantGETSerializer.Meta):
        fields = ["id", "name", "receive_call_for_order", "phone"]




# new add

class BaseRestaurantSearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = Restaurant
        fields = ['id', 'name']

class BaseLocationSearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = Location
        fields = ['id', 'name', 'restaurant']
