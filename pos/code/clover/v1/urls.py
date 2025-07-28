from django.urls import include, path

from . import views

urlpatterns = [
    path('', views.GetCloverAuthCode.as_view()),
    path('push/<str:pk>/', views.GetCloverAuthCode.as_view()),
    path('hook/', views.CloverWebhook.as_view()),
    path('payment-hook/', views.CloverWebhook.as_view())
]
