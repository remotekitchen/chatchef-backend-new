from django.urls import path
from rest_framework.routers import DefaultRouter

from reward.api.v1.views import (AllCouponAPIView, AllCouponChatchefAPIView, RedeemRewardPointAPIView,
                                 RestaurantsRewardListCreateView,
                                 RewardGroupListCreateAPIView,
                                 RewardGroupRetrieveUpdateDestroyAPIView,
                                 RewardLevelDOGetAPIView,
                                 RewardLevelListCreateAPIView,
                                 RewardLevelRetrieveUpdateDestroyAPIView,
                                 RewardListAPIView, RewardManageListCreateView,
                                 UserRewardListCreateAPIView, LocalDealViewSet,IssueRewardAPIView,CampaignViewSet,TaskViewSet)

router = DefaultRouter()
router.register(r'campaigns', CampaignViewSet, basename='campaign')
router.register(r'tasks', TaskViewSet)


urlpatterns = [
    path('reward-group/', RewardGroupListCreateAPIView.as_view(), name='reward-group'),
    path('reward/', RewardListAPIView.as_view(), name='reward'),
    path('user-reward/', UserRewardListCreateAPIView.as_view(), name='user-reward'),
    path('reward-group/item/', RewardGroupRetrieveUpdateDestroyAPIView.as_view(),
         name='reward-group-item'),
    path('restaurant-reward/', RestaurantsRewardListCreateView.as_view(),
         name='restaurant-reward'),
    path('reward-point/offers/', RewardManageListCreateView.as_view(),
         name='restaurant-reward-point'),
    path('coupon/', AllCouponAPIView.as_view(), name='coupon'),
    path('coupon/chatchef/', AllCouponChatchefAPIView.as_view(), name='coupon'),
    path('redeem-points/', RedeemRewardPointAPIView.as_view(), name='redeem'),
    path('reward-level/', RewardLevelListCreateAPIView.as_view(), name='reward-level'),
    path('do/reward-level/', RewardLevelDOGetAPIView.as_view(),
         name='reward-level'),
    path('reward-level/item/', RewardLevelRetrieveUpdateDestroyAPIView.as_view(),
         name='reward-level-item'),
    path('local-deal/', LocalDealViewSet.as_view({'get': 'list', 'post': 'create'}), name='local-deal'),
    path('local-deal/<int:pk>/', LocalDealViewSet.as_view({'get': 'retrieve', 'put': 'update', 'patch': 'partial_update', 'delete': 'destroy'}), name='local-deal-id'),
    path('reward/issue/', IssueRewardAPIView.as_view(), name='issue-reward'),
]