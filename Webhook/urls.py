from django.urls import include, path

from .views import (DoordashWebhookView, OtterWebhook, RaiderAppWebhook,
                    StripeConnectWebhookView, StripeWebhookView,
                    UberWebhookView, WhatAppWebhookView, WhatsappWebhook,
                    connect_to_otter, delete_to_otter, otter_callback,
                    otter_callback_store, otter_connection_status)

urlpatterns = [
    path('doordash_wh/', DoordashWebhookView.as_view(), name='doordash-webhook'),
    path('uber_wh/', UberWebhookView.as_view(), name='uber-webhook'),
    path('whatsapp_wh/', WhatAppWebhookView.as_view(), name='whatsapp-webhook'),
    path('stripe/', StripeWebhookView.as_view(), name='stripe-webhook'),
    path('otter/', OtterWebhook.as_view(), name="otter-webhook"),
    path('otter/callback ', otter_callback, name="otter-callback"),
    path('otter/callback/stores/<str:pk>/',
         otter_callback_store, name="otter-callback-stores"),
    path('otter/callback/status/<str:pk>/',
         otter_connection_status, name="otter-callback-status"),
    path('otter/callback/connect/<str:pk>/',
         connect_to_otter, name="otter-callback-connect"),
    path('otter/callback/delete/<str:pk>/',
         delete_to_otter, name="otter-callback-delete"),
    path('stripe/connect/', StripeConnectWebhookView.as_view(),
         name="stripe-connect-webhook"),
    path('clover/', include('pos.code.clover.v1.urls')),
    path('raider/', RaiderAppWebhook.as_view()),
    path('marketing_wh/', WhatsappWebhook.as_view(), name='marketing-webhook'),
    # path('rider-status-webhook/', DeliveryStatusWebhookView.as_view(), name='delivery_status_webhook'),
]
