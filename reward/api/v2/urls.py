from django.urls import path

from reward.api.v2.views import RewardGroupListCreateAPIView

urlpatterns = [
    path('reward-group/', RewardGroupListCreateAPIView.as_view(),),
]
