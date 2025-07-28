from django.urls import path

from referral.api.v1.views import InviteCodeAPIView, ReferralAPIView

urlpatterns = [
    path("referral", ReferralAPIView.as_view()),
    path("referral/<str:pk>/", ReferralAPIView.as_view()),
    path("get-code/", InviteCodeAPIView.as_view()),
]
