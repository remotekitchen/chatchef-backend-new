from reward.api.base.views import (BaseAllCouponAPIView,
                                   BaseAllCouponChatchefAPIView,
                                   BaseCouponCreateAPIView,
                                   BaseRedeemRewardPointAPIView,
                                   BaseRestaurantsRewardListCreateView,
                                   BaseRewardGroupListCreateAPIView,
                                   BaseRewardGroupRetrieveUpdateDestroyAPIView,
                                   BaseRewardLevelDOGetAPIView,
                                   BaseRewardLevelListCreateAPIView,
                                   BaseRewardLevelRetrieveUpdateDestroyAPIView,
                                   BaseRewardListAPIView,
                                   BaseRewardManageListCreateView,
                                   BaseUserRewardListCreateAPIView, BaseLocalDealViewSet,
                                   BaseIssueRewardAPIView,BaseCampaignViewSet,BaseTaskViewSet)
from reward.api.v1.serializers import (RewardGroupSerializer,
                                       RewardLevelSerializer, RewardSerializer,
                                       UserRewardCreateSerializer,
                                       UserRewardSerializer)


class RewardGroupListCreateAPIView(BaseRewardGroupListCreateAPIView):
    serializer_class = RewardGroupSerializer


class RewardListAPIView(BaseRewardListAPIView):
    pass


class UserRewardListCreateAPIView(BaseUserRewardListCreateAPIView):
    pass


class RewardGroupRetrieveUpdateDestroyAPIView(BaseRewardGroupRetrieveUpdateDestroyAPIView):
    serializer_class = RewardGroupSerializer


class RestaurantsRewardListCreateView(BaseRestaurantsRewardListCreateView):
    serializer_class = RewardSerializer


class RewardManageListCreateView(BaseRewardManageListCreateView):
    pass


class AllCouponAPIView(BaseAllCouponAPIView):
    pass

class AllCouponChatchefAPIView(BaseAllCouponChatchefAPIView):
    pass


class RedeemRewardPointAPIView(BaseRedeemRewardPointAPIView):
    pass


class RewardLevelListCreateAPIView(BaseRewardLevelListCreateAPIView):
    serializer_class = RewardLevelSerializer


class RewardLevelDOGetAPIView(BaseRewardLevelDOGetAPIView):
    pass


class RewardLevelRetrieveUpdateDestroyAPIView(BaseRewardLevelRetrieveUpdateDestroyAPIView):
    serializer_class = RewardLevelSerializer


class CouponCreateAPIView(BaseCouponCreateAPIView):
    serializer_class = UserRewardCreateSerializer

class LocalDealViewSet(BaseLocalDealViewSet):
  pass


class IssueRewardAPIView(BaseIssueRewardAPIView):
  pass


class CampaignViewSet(BaseCampaignViewSet):
  pass


class TaskViewSet(BaseTaskViewSet):
  pass