# # hotel/api/base/permissions.py

# from rest_framework.permissions import BasePermission
# from hotel.models import Hotel, Booking
# from accounts.models import User

# class IsHotelOwner(BasePermission):
#     def has_permission(self, request, view):
#         return (
#             request.user.is_authenticated and
#             request.user.role == User.RoleType.HOTEL_OWNER
#         )

# class IsHotelOwnerOfBooking(BasePermission):
#     def has_object_permission(self, request, view, obj):
#         return (
#             request.user.is_authenticated and
#             request.user.role == User.RoleType.HOTEL_OWNER and
#             obj.hotel.owner == request.user
#         )

# class IsHotelOwnerOfObject(BasePermission):
#     def has_object_permission(self, request, view, obj):
#         return obj.hotel.owner == request.user or obj.owner == request.user