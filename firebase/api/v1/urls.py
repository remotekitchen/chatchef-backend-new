from django.urls import path
from firebase.api.base.views import FCMTokenViewSet, SendNotificationView
from firebase.api.v1.views import FirebasePushTokenCreateAPIView, CompanyPushTokenCreateAPIView, \
    CompanyPushTokenDestroyAPIView, FirebasePushTokenDestroyAPIView

urlpatterns = [
    path('token/', FirebasePushTokenCreateAPIView.as_view(), name='token-create'),
    path('token/company/', CompanyPushTokenCreateAPIView.as_view(), name='company-token-create'),
    path('token/delete/', FirebasePushTokenDestroyAPIView.as_view(), name='token-delete'),
    path('token/company/delete/', CompanyPushTokenDestroyAPIView.as_view(), name='company-token-delete'),

    path('fcm-tokens/', FCMTokenViewSet.as_view({'get': 'list', 'post': 'create'}), name='fcm-token-list'),
    path("api/send-notification/", SendNotificationView.as_view(), name="send-notification"),

]
