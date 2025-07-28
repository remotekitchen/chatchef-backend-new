from django.urls import include, path
from rest_framework.routers import DefaultRouter

from integration.api.core.views import (allow_application, connect_with_app,
                                        stores)
from integration.api.v1 import views

router = DefaultRouter()
router.register('applications', views.PlatformReadOnlyModelView,
                basename='platforms-list')

urlpatterns = [
    path('', include(router.urls)),
    path('auth/token/', views.IntegrationTokenView.as_view(),
         name="IntegrationToken"),
    path('connections/', views.Connections.as_view(), name="Connections"),
    path('menus', views.MenuSender.as_view(), name="MenuSender"),
    path('order/all/', views.ExternalOrderCreateListView.as_view(),
         name="order-all"),
    path('order/create/', views.ExternalOrderCreateListView.as_view(),
         name="order-create"),
    path('order/update/<str:pk>/', views.ExternalOrderCreateListView.as_view(),
         name="order-update"),
    path('order-state/update/<str:pk>/', views.OrderStateUpdate.as_view(),
         name="order-update"),
    path('allow-application/', allow_application, name='allow-application'),
    path('connect/stores/', stores, name='stores'),
    path('connect/stores/<str:pk>/', connect_with_app, name='connect-app'),
    path('cost-calculation/', views.ExternalCostCalculationView.as_view(),
         name='cost-calculation'),
]
