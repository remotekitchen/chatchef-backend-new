from marketing.api.base.views import (
    BaseAllActivationCampaignListAPIView, BaseBirthdayGiftListCreateAPIView,
    BaseBirthdayGiftRetrieveUpdateDestroyAPIView, BaseBogoListCreateAPIView,BaseBxGyListCreateAPIView,
    BaseBogoRetrieveUpdateDestroyAPIView, BaseBxGyRetrieveUpdateDestroyAPIView, BaseContactUsDataModelView,
    BaseDemoDataModelView, BaseEmailConfigListCreateView,
    BaseEmailConfigurationRetrieveUpdateDestroyView, BaseEmailSendView,
    BaseFissionCampaignListCreateApiView,
    BaseFissionCampaignRetrieveUpdateDestroyApiView,
    BaseGiftCardListCreateAPIView, BaseGiftCardRetrieveUpdateDestroyAPIView,
    BaseGroupPromotionListCreateAPIView,
    BaseGroupPromotionRetrieveUpdateDestroyAPIView,
    BaseLoyaltyProgramListCreateAPIView,
    BaseLoyaltyProgramRetrieveUpdateDestroyAPIView,
    BaseMembershipCardListCreateAPIView,
    BaseMembershipCardRetrieveUpdateDestroyAPIView, BaseRandomPrizeAPIView,
    BaseRatingAndReviewModelView, BaseRestaurantEmailHistoryListView,
    BaseSpendXSaveYListCreateAPIView, BaseSpendXSaveYManagerListCreateAPIView,
    BaseSpendXSaveYManagerRetrieveUpdateDestroyAPIView,
    BaseSpendXSaveYPromoOptionListAPIView,
    BaseSpendXSaveYRetrieveUpdateDestroyAPIView, BaseStaffSendEmailAPI,
    BaseUserFissionCampaignListAPIView, BaseVoucherListCreateAPIView,
    BaseVoucherRetrieveUpdateDestroyAPIView, BaseReviewModelView, BaseAutoReplyToCommentsDetailView)
from marketing.api.v1.serializers import (BirthdayGiftSerializer,
                                          BogoSerializer,
                                          BxGySerializer,
                                          FissionCampaignSerializer,
                                          FissionPrizeSerializer,
                                          GiftCardSerializer,
                                          GroupPromotionSerializer,
                                          LoyaltyProgramSerializer,
                                          MembershipCardSerializer,
                                          SpendXSaveYManagerSerializer,
                                          SpendXSaveYSerializer,
                                          VoucherSerializer)


class SpendXSaveYManagerListCreateAPIView(BaseSpendXSaveYManagerListCreateAPIView):
    serializer_class = SpendXSaveYManagerSerializer


class SpendXSaveYManagerRetrieveUpdateDestroyAPIView(BaseSpendXSaveYManagerRetrieveUpdateDestroyAPIView):
    serializer_class = SpendXSaveYManagerSerializer


class SpendXSaveYListCreateAPIView(BaseSpendXSaveYListCreateAPIView):
    serializer_class = SpendXSaveYSerializer


class SpendXSaveYRetrieveUpdateDestroyAPIView(BaseSpendXSaveYRetrieveUpdateDestroyAPIView):
    serializer_class = SpendXSaveYSerializer


class SpendXSaveYPromoOptionListAPIView(BaseSpendXSaveYPromoOptionListAPIView):
    pass


class VoucherListCreateAPIView(BaseVoucherListCreateAPIView):
    serializer_class = VoucherSerializer


class VoucherRetrieveUpdateDestroyAPIView(BaseVoucherRetrieveUpdateDestroyAPIView):
    serializer_class = VoucherSerializer


class BogoListCreateAPIView(BaseBogoListCreateAPIView):
    serializer_class = BogoSerializer
class BogoRetrieveUpdateDestroyAPIView(BaseBogoRetrieveUpdateDestroyAPIView):
    pass
  


class GroupPromotionListCreateAPIView(BaseGroupPromotionListCreateAPIView):
    serializer_class = GroupPromotionSerializer


class GroupPromotionRetrieveUpdateDestroyAPIView(BaseGroupPromotionRetrieveUpdateDestroyAPIView):
    serializer_class = GroupPromotionSerializer


class LoyaltyProgramListCreateAPIView(BaseLoyaltyProgramListCreateAPIView):
    serializer_class = LoyaltyProgramSerializer


class LoyaltyProgramRetrieveUpdateDestroyAPIView(BaseLoyaltyProgramRetrieveUpdateDestroyAPIView):
    serializer_class = LoyaltyProgramSerializer


class FissionCampaignListCreateApiView(BaseFissionCampaignListCreateApiView):
    serializer_class = FissionCampaignSerializer


class FissionCampaignRetrieveUpdateDestroyApiView(BaseFissionCampaignRetrieveUpdateDestroyApiView):
    serializer_class = FissionCampaignSerializer


class BirthdayGiftListCreateAPIView(BaseBirthdayGiftListCreateAPIView):
    serializer_class = BirthdayGiftSerializer


class BirthdayGiftRetrieveUpdateDestroyAPIView(BaseBirthdayGiftRetrieveUpdateDestroyAPIView):
    serializer_class = BirthdayGiftSerializer


class RandomPrizeAPIView(BaseRandomPrizeAPIView):
    serializer_class = FissionPrizeSerializer


class GiftCardListCreateAPIView(BaseGiftCardListCreateAPIView):
    serializer_class = GiftCardSerializer


class GiftCardRetrieveUpdateDestroyAPIView(BaseGiftCardRetrieveUpdateDestroyAPIView):
    serializer_class = GiftCardSerializer


class AllActivationCampaignListAPIView(BaseAllActivationCampaignListAPIView):
    pass


class MembershipCardListCreateAPIView(BaseMembershipCardListCreateAPIView):
    serializer_class = MembershipCardSerializer


class MembershipCardRetrieveUpdateDestroyAPIView(BaseMembershipCardRetrieveUpdateDestroyAPIView):
    serializer_class = MembershipCardSerializer


class UserFissionCampaignListAPIView(BaseUserFissionCampaignListAPIView):
    serializer_class = FissionCampaignSerializer


class RatingAndReviewModelView(BaseRatingAndReviewModelView):
    pass

class ReviewModelView(BaseReviewModelView):
    pass


class EmailSendView(BaseEmailSendView):
    pass


class RestaurantEmailHistoryListView(BaseRestaurantEmailHistoryListView):
    pass


class EmailConfigListCreateView(BaseEmailConfigListCreateView):
    pass


class EmailConfigurationRetrieveUpdateDestroyView(BaseEmailConfigurationRetrieveUpdateDestroyView):
    pass


class StaffSendEmailAPI(BaseStaffSendEmailAPI):
    pass


class ContactUsDataModelView(BaseContactUsDataModelView):
    pass


class DemoDataModelView(BaseDemoDataModelView):
    pass

class AutoReplyToCommentsDetailView(BaseAutoReplyToCommentsDetailView):
    pass
  
class BxGyListCreateAPIView(BaseBxGyListCreateAPIView):
    serializer_class = BxGySerializer
  
class BxGyRetrieveUpdateDestroyAPIView(BaseBxGyRetrieveUpdateDestroyAPIView): 
    pass