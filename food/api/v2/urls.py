from django.urls import path

from food.api.v2.views import (CategoryListCreateAPIView,
                               MenuItemListCreateAPIView, MenuListAPIView,
                               MenuRetrieveAPIView,
                               ModifierGroupListCreateAPIView)

urlpatterns = [
    path("menu/", MenuListAPIView.as_view(), name="menu-list-v2"),
    path("menu/item/", MenuRetrieveAPIView.as_view(), name="menu-item-v2"),
    path('menu-item/', MenuItemListCreateAPIView.as_view(), name='menu-item'),
    path('category/', CategoryListCreateAPIView.as_view()),
    path('modifier-group/', ModifierGroupListCreateAPIView.as_view(),)
]
