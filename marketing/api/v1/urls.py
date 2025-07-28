from django.urls import include, path
from rest_framework.routers import DefaultRouter

from marketing.api.v1.views import (
    AllActivationCampaignListAPIView, BirthdayGiftListCreateAPIView,
    BirthdayGiftRetrieveUpdateDestroyAPIView, BogoListCreateAPIView,
    BogoRetrieveUpdateDestroyAPIView, ContactUsDataModelView,
    DemoDataModelView, EmailConfigListCreateView,
    EmailConfigurationRetrieveUpdateDestroyView, EmailSendView,
    FissionCampaignListCreateApiView,
    FissionCampaignRetrieveUpdateDestroyApiView, GiftCardListCreateAPIView,
    GiftCardRetrieveUpdateDestroyAPIView, GroupPromotionListCreateAPIView,
    GroupPromotionRetrieveUpdateDestroyAPIView,
    LoyaltyProgramListCreateAPIView,
    LoyaltyProgramRetrieveUpdateDestroyAPIView,
    MembershipCardListCreateAPIView,
    MembershipCardRetrieveUpdateDestroyAPIView, RandomPrizeAPIView,
    RatingAndReviewModelView, RestaurantEmailHistoryListView,
    SpendXSaveYListCreateAPIView, SpendXSaveYManagerListCreateAPIView,
    SpendXSaveYManagerRetrieveUpdateDestroyAPIView,
    SpendXSaveYPromoOptionListAPIView, SpendXSaveYRetrieveUpdateDestroyAPIView,
    StaffSendEmailAPI, UserFissionCampaignListAPIView,
    VoucherListCreateAPIView, VoucherRetrieveUpdateDestroyAPIView, ReviewModelView, AutoReplyToCommentsDetailView, BxGyListCreateAPIView, BxGyRetrieveUpdateDestroyAPIView)

router = DefaultRouter()
router.register('ratings-review', ReviewModelView,
                basename='ratings-review')
router.register('contact-us', ContactUsDataModelView, basename="contact")
router.register('get-demo', DemoDataModelView, basename="get-demo")

urlpatterns = [
    path('', include(router.urls)),
    path('spendx-savey/', SpendXSaveYListCreateAPIView.as_view(), name='spendXsaveY'),
    path('spendx-savey/manager/', SpendXSaveYManagerListCreateAPIView.as_view(),
         name='spendXsaveY-manager'),
    path('spendx-savey/promo-option/', SpendXSaveYPromoOptionListAPIView.as_view(),
         name='spendXsaveY-promo-option'),
    path('spendx-savey/item/', SpendXSaveYRetrieveUpdateDestroyAPIView.as_view(),
         name='spendXsaveY-item'),
    path('spendx-savey/manager/item/', SpendXSaveYManagerRetrieveUpdateDestroyAPIView.as_view(),
         name='spendXsaveY-item'),
    path('voucher/', VoucherListCreateAPIView.as_view(), name='voucher'),
    path('voucher/item/', VoucherRetrieveUpdateDestroyAPIView.as_view(),
         name='voucher-item'),
    path('bogo/', BogoListCreateAPIView.as_view(), name='bogo'),
    path('bxgy/', BxGyListCreateAPIView.as_view(), name='bxgy'),
    path('bogo/item/', BogoRetrieveUpdateDestroyAPIView.as_view(), name='bogo-item'),
    path('bxgy/item/', BxGyRetrieveUpdateDestroyAPIView.as_view(), name='bxgy-item'),
    path('group-ordering/', GroupPromotionListCreateAPIView.as_view(), name='group'),
    path('group-ordering/item/',
         GroupPromotionRetrieveUpdateDestroyAPIView.as_view(), name='group-item'),
    path('loyalty-program/', LoyaltyProgramListCreateAPIView.as_view(),
         name='loyalty-program'),
    path('loyalty-program/item/', LoyaltyProgramRetrieveUpdateDestroyAPIView.as_view(),
         name='loyalty-program-item'),
    path('lucky-flip/', FissionCampaignListCreateApiView.as_view(), name='lucky-flip'),
    path('lucky-flip/item/', FissionCampaignRetrieveUpdateDestroyApiView.as_view(),
         name='lucky-flip-price'),
    path('birthday-gift/', BirthdayGiftListCreateAPIView.as_view(),
         name='birthday-gift'),
    path('birthday-gift/item/', BirthdayGiftRetrieveUpdateDestroyAPIView.as_view(),
         name='birthday-gift-item'),
    path('random-prize/', RandomPrizeAPIView.as_view(), name='random-prize'),
    path('gift-card/', GiftCardListCreateAPIView.as_view(), name='gift-card'),
    path('gift-card/item/', GiftCardRetrieveUpdateDestroyAPIView.as_view(),
         name='gift-card-item'),
    path('membership-card/', MembershipCardListCreateAPIView.as_view(),
         name='membership-card'),
    path('membership-card/item/', MembershipCardRetrieveUpdateDestroyAPIView.as_view(),
         name='membership-card-item'),
    path('activation-campaign/', AllActivationCampaignListAPIView.as_view(),
         name='all-activation-campaign'),
    path('user/fission-campaign/', UserFissionCampaignListAPIView.as_view(),
         name='user-fission-campaign'),


    path('email/', EmailSendView.as_view(), name='email'),

    path('email-history/', RestaurantEmailHistoryListView.as_view(),
         name="email-history"),
    path('email-configurations/', EmailConfigListCreateView.as_view(),
         name='email-configuration-list-create'),
    # path('email-configurations/<int:restaurant>/', EmailConfigurationRetrieveUpdateDestroyView.as_view(),
    # name='email-configuration-detail'),
    path('email-configurations/restaurant/', EmailConfigurationRetrieveUpdateDestroyView.as_view(),
         name='email-configuration-detail'),
    path('email/staff-send', StaffSendEmailAPI.as_view()),
    
    # Rating and Review paths
#     path('ratings-review/', ReviewModelView.as_view({'get': 'list', 'post': 'create'}), name='ratings-review'),


    # Custom paths for ReviewModelView
    path('ratings-review/<int:pk>/like/', ReviewModelView.as_view({'post': 'like'}), name='review-like'),
    path('ratings-review/<int:pk>/dislike/', ReviewModelView.as_view({'post': 'dislike'}), name='review-dislike'),
    path('ratings-review/<int:pk>/add_comment/', ReviewModelView.as_view({'post': 'add_comment'}), name='review-add-comment'),
    path('ratings-review/<int:pk>/comments/', ReviewModelView.as_view({'get': 'get_nested_comments'}), name='review-get-comments'),
    path('ratings-review/<int:pk>/delete-reviews/', ReviewModelView.as_view({'delete': 'destroy'}), name='review-delete'),
    path('ratings-review/<int:pk>/pin-reviews/', ReviewModelView.as_view({'post': 'pin_review'}), name='review-pin'),
    path(
        "auto-reply/<int:restaurant_id>/<int:location_id>/",
        AutoReplyToCommentsDetailView.as_view(),
        name="auto_reply_detail",
    ),
    path("auto-reply/", AutoReplyToCommentsDetailView.as_view(), name="auto_reply_create"),
]
