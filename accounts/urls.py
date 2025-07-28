from django.urls import path, include

from accounts.views import UbereatsLoginRedirectView

try:
    from allauth.socialaccount import providers
except ImportError:
    raise ImportError("allauth needs to be added to INSTALLED_APPS.")

urlpatterns = [
    path('ubereats/login/redirect/', UbereatsLoginRedirectView.as_view(), name='ubereats-login-redirect'),
    path('', include('allauth.urls')),
    # path('api/password_reset/', include('django_rest_passwordreset.urls', namespace='password_reset')),
]
