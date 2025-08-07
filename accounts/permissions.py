# permissions.py
from rest_framework.permissions import BasePermission

class IsNotBlocked(BasePermission):
    message = "Your account has been blocked. Please contact support."

    def has_permission(self, request, view):
        return not (
            request.user
            and request.user.is_authenticated
            and getattr(request.user, "is_blocked", False)
        )
