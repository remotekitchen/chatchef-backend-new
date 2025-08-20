"""chatchef URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path

from hungrytiger.settings import ENV_TYPE

api_url_patterns = (
    [
        # V1 APIS
        path('food/v1/', include('food.api.v1.urls')),
        path('chat/v1/', include('chat.api.v1.urls')),
        path('accounts/v1/', include('accounts.api.v1.urls')),
        # path('image-generator/v1/', include('image_generator.api.v1.urls')),
        path('webhook/v1/', include('Webhook.urls')),
        path('billing/v1/', include('billing.api.v1.urls')),
        path('stats/v1/', include('stats.api.v1.urls')),
        path('wappmsg/v1/', include('communication.api.v1.urls')),
        path('marketing/v1/', include('marketing.api.v1.urls')),
        path('open-api/v1/', include('integration.api.v1.urls')),
        path('dashboard/analytics/v1/', include('analytics.api.v1.urls')),
        path('reward/v1/', include('reward.api.v1.urls')),
        path('firebase/v1/', include('firebase.api.v1.urls')),
        path('qr_code/v1/', include('QR_Code.api.v1.urls')),
        path('hotel/v1/', include('hotel.api.v1.urls')),
        # V2 APIS
        path('billing/v2/', include('billing.api.v2.urls')),
        path("food/v2/", include("food.api.v2.urls")),
        path("reward/v2/", include("reward.api.v2.urls")),



        path("design/v1/", include("dynamic_theme.api.v1.urls")),
        path("core/v1/", include("core.api.urls")),
        path("referrals/v1/", include("referral.api.v1.urls")),
<<<<<<< HEAD
        path("lark/v1/", include('lark.api.v1.urls'))
=======
>>>>>>> 8282bd5e6cbcb8cf9d0b9db03fc6269eeea3dfab
    ]
)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include(api_url_patterns)),
    path('', include('accounts.urls')),
    path("remote-kitchen/", include("remotekitchen.urls"))
]

if ENV_TYPE == 'DEVELOPMENT':
    urlpatterns.append(path('__debug__/', include('debug_toolbar.urls')))
