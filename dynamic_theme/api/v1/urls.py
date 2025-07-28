from django.urls import path

from dynamic_theme.api.v1.views import (
    ThemeListCreateAPIView,
    ThemeRetrieveUpdateDestroyAPIView,
    GetThemeDataAPIView,
)


urlpatterns =[
    path('themes-list/', ThemeListCreateAPIView.as_view(), name='theme-list-create'),
    path('theme-detail/', ThemeRetrieveUpdateDestroyAPIView.as_view(), name='theme-detail'),
    path('theme-data/', GetThemeDataAPIView.as_view(), name='theme-data'),
]