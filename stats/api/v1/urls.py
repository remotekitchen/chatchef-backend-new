from django.urls import path

from stats.api.v1.views import SaleListAPIView

urlpatterns = [
    path('sales', SaleListAPIView.as_view(), name='sales')
]