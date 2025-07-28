from reward.api.base.serializers import BaseRewardGroupSerializer, BaseRewardSerializer, BaseUserRewardSerializer, \
    BaseRewardManageSerializer, BaseRewardGroupBriefSerializer, BaseRewardLevelSerializer, \
    BaseUserRewardCreateSerializer, BaseLocalDealSerializer


class RewardSerializer(BaseRewardSerializer):
    from food.api.v2.serializers import MenuItemPreviewSerializer
    item_details = MenuItemPreviewSerializer(many=True, source='items', read_only=True)


class RewardGroupSerializer(BaseRewardGroupSerializer):
    reward_set = RewardSerializer(many=True)


class RewardManageSerializer(BaseRewardManageSerializer):
    reward_group_details = RewardGroupSerializer(read_only=True, source='reward_group')


class ExtendedRewardSerializer(RewardSerializer):
    reward_group = BaseRewardGroupBriefSerializer()


class UserRewardCreateSerializer(BaseUserRewardCreateSerializer):
    pass


class UserRewardSerializer(BaseUserRewardSerializer):
    reward = ExtendedRewardSerializer()


class RewardLevelSerializer(BaseRewardLevelSerializer):
    reward_manages = RewardManageSerializer(many=True)

class LocalDealSerializer(BaseLocalDealSerializer):
  pass