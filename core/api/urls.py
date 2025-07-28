from django.urls import path

from core.api.views import GetLatestAPP

urlpatterns = [
    path('app/', GetLatestAPP.as_view(), name='app'),
]
