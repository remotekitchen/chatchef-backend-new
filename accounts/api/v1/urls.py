from django.urls import include, path
from rest_framework.routers import DefaultRouter

from accounts.api.v1.views import (AppleLoginAPIView, ChangePasswordAPIView,
                                   ContactModelAPIView,
                                   EmailPasswordLoginAPIView, FacebookLogin,
                                   GoogleConnectAPIView, GoogleLoginAPIView,
                                   RestaurantUserAPIView,
                                   RestaurantUserRetrieveAPIView,
                                   SentUserVerifyOTP,
                                   SentUserVerifyOTPChatchef,
                                   UserAddressListCreateAPIView,
                                   UserAddressRetrieveUpdateDestroyAPIView,
                                   UserEmailVerifyView,
                                   VerifyEmailOTPView,
                                   UserRegistrationAPIView,
                                   UserRetrieveUpdateDestroyAPIView,

                                   UserVerifyOTP, UserRankingView, SalesUserRankingView, VerifyChatUserView,
                                   PasswordResetRequestView,PasswordResetConfirmView, Plus88UserCountView, MetricsOverviewView,UpdateMetricsView, LogUserEventView,
                                   VerifyOTPView,SendUserVerifyEmail,
                                    Plus88UserCountView,  UserRankingView,  VerifyChatUserView, Plus88UserCountView,Plus88UserCountView, 
                                    ScanQrAPIView, QRScanAnalyticsView,SessionTrackingView,FeedbackPromptView)



from accounts.api.v1.views import (
    SubscriptionCreateView,
    SubscriptionVerifyView,
    SubscriptionCancelView,
    WebhookHandlerView,
    SubscriptionListView,
    CancellationRequestListView
)

router = DefaultRouter()
router.register("contacts", ContactModelAPIView, basename="contacts")

urlpatterns = [
    path("", include(router.urls)),
    path('user/events/log/', LogUserEventView.as_view(), name='user-register'),
    path('user/metrics/overview/', MetricsOverviewView.as_view(), name='user-register'),
    path('user/metrics/update/', UpdateMetricsView.as_view(), name='user-register'),
    path("session/", SessionTrackingView.as_view(), name="track-session"),
    path("feedback/", FeedbackPromptView.as_view(), name="submit-feedback"),
    path('scan/', ScanQrAPIView.as_view(), name='user-scan'),
    path('analytics/', QRScanAnalyticsView.as_view(), name='user-analytics'),
    path('user/register/', UserRegistrationAPIView.as_view(), name='user-register'),
    path('user/count/', Plus88UserCountView.as_view(), name='user-register'),
    path('user/verify/', UserEmailVerifyView.as_view(), name='user-verify'),
    path('email/verify/', VerifyEmailOTPView.as_view(), name='user-verify'), #verify email via OTP
    path('login/email/', EmailPasswordLoginAPIView.as_view(), name='email-login'),
    path('login/google/', GoogleLoginAPIView.as_view(), name='google-login'),
    path('login/facebook/', FacebookLogin.as_view(), name='facebook-login'),
    path('login/apple/', AppleLoginAPIView.as_view(), name='apple-login'),
    path('connect/google/', GoogleConnectAPIView.as_view(), name='google-connect'),
    path('user/item/', UserRetrieveUpdateDestroyAPIView.as_view(), name='user-item'),
    path('change-password/', ChangePasswordAPIView.as_view(), name='change-password'),
    path('restaurant-users/<str:pk>/', RestaurantUserAPIView.as_view(),
         name='restaurant-user'),
    path('restaurant-user/', RestaurantUserRetrieveAPIView.as_view(),
         name='restaurant-user'),
    path('user-address/', UserAddressListCreateAPIView.as_view(), name='user-address'),
    path('user-address/item/', UserAddressRetrieveUpdateDestroyAPIView.as_view(),
         name='user-address-itemI'),
    path('password_reset/', include('django_rest_passwordreset.urls',
         namespace='password_reset')),
    path("phone/verify/", SentUserVerifyOTP.as_view()),
    path("email/verify-mail/", SendUserVerifyEmail.as_view(), name="send-email-verification"), #send verify mail

    path("phone/verify/chatchef", SentUserVerifyOTPChatchef.as_view()),
    path("phone/verify/confirm/", UserVerifyOTP.as_view()),
    path('user/get-sales-info/', UserRankingView.as_view(), name='sales-user'),
#     path("user/sales-ranking/", SalesUserRankingView.as_view(), name="sales_ranking"),
    path("user/verify-chat-user/", VerifyChatUserView.as_view(), name="verify_chat_user"),
   path('password-reset/request/', PasswordResetRequestView.as_view(), name='password_reset_request'),
    path("password-reset/verify-otp/", VerifyOTPView.as_view(), name="password_reset_verify_otp"),
    path('password-reset/confirm/', PasswordResetConfirmView.as_view(), name='password_reset_confirm'),
     path("subscription/create/", SubscriptionCreateView.as_view(), name='subscription-creation'),
     path("subscription/verify/", SubscriptionVerifyView.as_view(),  name='subscription-verify'),
     path("subscription/cancel-request/", SubscriptionCancelView.as_view(),  name='subscription-cancel-request'),

     path("webhooks/stripe/", WebhookHandlerView.as_view(),  name='subscription-webhooks'),
     
     path("subscriptions/list/", SubscriptionListView.as_view(),  name='subscription'),
     path("cancel-requests/list/", CancellationRequestListView.as_view(),  name='subscription'),
]
