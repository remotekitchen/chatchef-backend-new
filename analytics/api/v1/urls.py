from django.urls import include, path
from rest_framework.routers import DefaultRouter

from analytics.api.v1 import views

router = DefaultRouter()
router.register("traffic-monitor", views.TrafficMonitorModelView,
                basename="traffic-monitor")

urlpatterns = [
    path('', include(router.urls)),
    path('order-data', views.DashboardOrder.as_view(),
         name="dashboard-order-data"),
    path('business-performance', views.BusinessPerformanceAPIView.as_view()),
    path('menu-performance-data', views.MenuPerformanceAPI.as_view(),
         name="dashboard-menu-data"),
    path('traffic-monitor-dashboard', views.TrafficMonitorDashboardAPIView.as_view(),
         name="dashboard-menu-data"),
]
