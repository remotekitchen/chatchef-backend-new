import environ
import jwt
import rest_framework
from rest_framework.generics import get_object_or_404
from rest_framework.permissions import BasePermission
from hotel.models import Hotel, Booking
from accounts.models import User
from billing.models import Order
from food.models import Menu, Restaurant
from integration.models import Onboarding, Platform

env = environ.Env()

OAUTH_KEY = env.str('CHATCHEFS_EXTERNAL_KEY')


class IsObjectOwner(BasePermission):
    """
    Object-level permission to only allow owners of an object to edit it.
    """

    def has_object_permission(self, request, view, user):
        return user == request.user or request.user.is_superuser


class IsObjOwner(BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.user == request.user


class HasCompanyAccess(BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.user.is_superuser:
            return True  # Superuser bypass
        
        return request.method == 'GET' or (
                request.user.is_authenticated and obj.company == request.user.company)


class HasRestaurantAccess(BasePermission):
    def has_permission(self, request, view):
        if request.user.is_authenticated and request.user.is_superuser:
            return True
        
        restaurant_id = request.data.get('restaurant', None)
        if restaurant_id is not None:
            if not request.user.is_authenticated:
                return False
            restaurant = get_object_or_404(Restaurant, id=restaurant_id)
            return restaurant.company == request.user.company
        return True

    def has_object_permission(self, request, view, obj):
        if request.user.is_authenticated and request.user.is_superuser:
            return True
        return request.method == 'GET' or (
                request.user.is_authenticated and
                obj.restaurant is not None and
                obj.restaurant.company == request.user.company)


class HasMenuAccess(BasePermission):
    def has_permission(self, request, view):
        menu_id = request.data.get('menu', None)
        if menu_id is not None:
            menu = get_object_or_404(Menu, id=menu_id)
            return menu.company == request.user.company
        return True


class OrderPermission(BasePermission):
    def has_object_permission(self, request, view, obj: Order):
        if request.user.is_superuser:
            return True
        print(request.method)
        # if request.method != 'GET' and not request.user.is_authenticated:
        #     return False
        if request.user != obj.user and request.user.company != obj.company:
            return False
        if request.method != 'GET' and request.user.company != obj.company:
            # if request.data.get('status') != Order.StatusChoices.CANCELLED or obj.status !=
            # Order.StatusChoices.PENDING:
            if 'status' not in request.data or len(request.data.keys()) > 1:
                return False

                # print(request.method)
        # if request.method != "GET" and request.user.company != obj.company:
        #     print('here')
        #     return False
        return True


class HasPlatformAccess(BasePermission):
    def has_permission(self, request, view):
        bearer_token = request.headers.get('Authorization')

        try:
            if bearer_token.startswith('Bearer '):
                token = bearer_token.split(' ')[1]
            else:
                return False

            if not Platform.objects.filter(token=token).exists():
                return False

            decoded_token = jwt.decode(
                token, key=OAUTH_KEY, algorithms='HS256', verify=True
            )
            request.decoded_token = decoded_token

            return True
        except Exception as error:
            print(error)
            return False


class HasRestaurantPerformAccess(BasePermission):
    def has_permission(self, request, view):
        store_key = request.headers.get('onboarding')
        request.store_key = store_key

        client_id = request.decoded_token['client_id']
        client_secret = request.decoded_token['client_secret']

        platform = Platform.objects.get(
            client_id=client_id, client_secret=client_secret
        )

        request.platform = platform

        if not Onboarding.objects.filter(store_key=store_key, client=platform).exists():
            return False

        request.onboarding = Onboarding.objects.get(
            store_key=store_key, client=platform
        )

        return True







# for hotel permission



class IsHotelOwner(BasePermission):
    def has_permission(self, request, view):
        return (
            request.user.is_authenticated and
            request.user.role == User.RoleType.HOTEL_OWNER
        )

class IsHotelOwnerOfBooking(BasePermission):
    def has_object_permission(self, request, view, obj):
        return (
            request.user.is_authenticated and
            request.user.role == User.RoleType.HOTEL_OWNER and
            obj.hotel.owner == request.user
        )

class IsHotelOwnerOfObject(BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.hotel.owner == request.user or obj.owner == request.user
    

    
class IsStaffAndHotelAdmin(BasePermission):
    """
    Allows access only to users who are:
    - is_staff == True AND
    - hotel_admin == True
    """

    def has_permission(self, request, view):
        user = request.user
        return (
            user and
            user.is_authenticated and
            user.is_staff and
            user.hotel_admin
        )


class IsHotelOwnerOrHotelAdmin(BasePermission):
    """
    Allow access if user is either:
    - Hotel Owner
    OR
    - Hotel Admin
    """
    def has_permission(self, request, view):
        user = request.user
        return (
            user.is_authenticated and (
                user.role == User.RoleType.HOTEL_OWNER
                or (user.is_staff and user.hotel_admin)
            )
        )
