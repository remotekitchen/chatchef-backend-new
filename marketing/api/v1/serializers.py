from marketing.api.base.serializers import (BaseBirthdayGiftSerializer,
                                            BaseBogoSerializer,
                                            BaseBxGySerializer,
                                            BaseFissionCampaignSerializer,
                                            BaseFissionPrizeSerializer,
                                            BaseGiftCardSerializer,
                                            BaseGroupPromotionSerializer,
                                            BaseLoyaltyProgramSerializer,
                                            BaseMembershipCardSerializer,
                                            BaseSpendXSaveYManagerSerializer,
                                            BaseSpendXSaveYSerializer,
                                            BaseVoucherSerializer)
from reward.api.base.serializers import BaseRewardSerializer
from reward.api.v1.serializers import RewardGroupSerializer
from marketing.api.base.serializers import ReviewSerializer
from food.api.base.serializers import BaseMenuItemSerializer
class SpendXSaveYManagerSerializer(BaseSpendXSaveYManagerSerializer):
    pass


class SpendXSaveYSerializer(BaseSpendXSaveYSerializer):
    pass


class VoucherSerializer(BaseVoucherSerializer):
    from food.api.base.serializers import BaseImageSerializer
    image = BaseImageSerializer(read_only=True, required=False)
    # reward = BaseRewardSerializer(read_only=True, required=False)




class VoucherGetSerializer(VoucherSerializer):
    reward = BaseRewardSerializer(read_only=True, required=False)

class ReviewGetSerializer(ReviewSerializer):
    menuItem_details = BaseMenuItemSerializer(source='menuItem', read_only=True) 
  

class BogoSerializer(BaseBogoSerializer):
    pass

class BxGySerializer(BaseBxGySerializer):
    pass

class GroupPromotionSerializer(BaseGroupPromotionSerializer):
    pass


class LoyaltyProgramSerializer(BaseLoyaltyProgramSerializer):
    pass


class FissionPrizeSerializer(BaseFissionPrizeSerializer):
    reward_details = RewardGroupSerializer(
        read_only=True, source="reward_group")


class FissionCampaignSerializer(BaseFissionCampaignSerializer):
    pass


class BirthdayGiftSerializer(BaseBirthdayGiftSerializer):
    pass


class GiftCardSerializer(BaseGiftCardSerializer):
    pass


class MembershipCardSerializer(BaseMembershipCardSerializer):
    pass
