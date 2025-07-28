from accounts.api.base.serializers import BaseEmailPasswordLoginSerializer
from accounts.api.base.views import (
    BaseAppleLoginAPIView,
    BaseChangePasswordAPIView,
    BaseContactModelAPIView,
    BaseEmailPasswordLoginAPIView,
    BaseFacebookLogin,
    BaseGoogleConnectAPIView,
    BaseGoogleLoginAPIView,
    BasePlus88UserCountView,
    BaseQRScanAnalyticsView,
    BaseUpdateMetricsView,
    BaseRestaurantUserAPIView,
    BaseRestaurantUserRetrieveAPIView,
    BaseSalesUserRankingView,
    BaseScanQrAPIView,
    BaseSendUserVerifyEmail,
    BaseSentUserVerifyOTP,
    BaseSentUserVerifyOTPChatchef,
    BaseUserAddressListCreateAPIView,
    BaseUserAddressRetrieveUpdateDestroyAPIView,
    BaseUserEmailVerifyView,
    BaseVerifyEmailOTPView,
    BaseUserRegistrationAPIView,
    BaseUserRetrieveUpdateDestroyAPIView,
    BaseUserVerifyOTP,
    BaseUserRankingView,
    BaseVerifyChatUserView,
    BaseMetricsOverviewView,
    BaseLogUserEventView,
    BasePasswordResetConfirmView,
    BasePasswordResetRequestView,
    BaseVerifyOTPView,
    BaseSubscriptionCreateView,
    BaseSubscriptionVerifyView,
    BaseSubscriptionCancelView,
    BaseWebhookHandlerView,
    BaseSubscriptionListView,
    BaseCancellationRequestListView,
)

  


from accounts.api.v1.serializers import (ChangePasswordSerializer,
                                         RestaurantUserSerializer,
                                         UserAddressSerializer, UserSerializer)


from accounts.api.base.views import (
    BaseSubscriptionCreateView,
    BaseSubscriptionVerifyView,
    BaseSubscriptionCancelView,
    BaseWebhookHandlerView,
    BaseSubscriptionListView,
    BaseCancellationRequestListView,BaseSessionTrackingView,BaseFeedbackPromptView
)

class UserRegistrationAPIView(BaseUserRegistrationAPIView):
    serializer_class = UserSerializer


class UserEmailVerifyView(BaseUserEmailVerifyView):
    pass

class VerifyEmailOTPView(BaseVerifyEmailOTPView):
    pass

class EmailPasswordLoginAPIView(BaseEmailPasswordLoginAPIView):
    serializer_class = BaseEmailPasswordLoginSerializer


class GoogleLoginAPIView(BaseGoogleLoginAPIView):
    pass


class GoogleConnectAPIView(BaseGoogleConnectAPIView):
    pass


class FacebookLogin(BaseFacebookLogin):
    pass


class AppleLoginAPIView(BaseAppleLoginAPIView):
    pass


class UserRetrieveUpdateDestroyAPIView(BaseUserRetrieveUpdateDestroyAPIView):
    pass


class ChangePasswordAPIView(BaseChangePasswordAPIView):
    serializer_class = ChangePasswordSerializer


class RestaurantUserAPIView(BaseRestaurantUserAPIView):
    pass


class RestaurantUserRetrieveAPIView(BaseRestaurantUserRetrieveAPIView):
    serializer_class = RestaurantUserSerializer


class UserAddressListCreateAPIView(BaseUserAddressListCreateAPIView):
    serializer_class = UserAddressSerializer


class UserAddressRetrieveUpdateDestroyAPIView(BaseUserAddressRetrieveUpdateDestroyAPIView):
    serializer_class = UserAddressSerializer


class ContactModelAPIView(BaseContactModelAPIView):
    pass


class SentUserVerifyOTP(BaseSentUserVerifyOTP):
    pass
  
class SentUserVerifyOTPChatchef(BaseSentUserVerifyOTPChatchef):
    pass

class UserVerifyOTP(BaseUserVerifyOTP):
    pass

class UserRankingView(BaseUserRankingView):
    pass

class SalesUserRankingView(BaseSalesUserRankingView):
    pass
class VerifyChatUserView(BaseVerifyChatUserView):
    pass
class SendUserVerifyEmail(BaseSendUserVerifyEmail):
    pass
class Plus88UserCountView(BasePlus88UserCountView):
    pass
class MetricsOverviewView(BaseMetricsOverviewView):
    pass
class LogUserEventView(BaseLogUserEventView):
    pass

class SessionTrackingView(BaseSessionTrackingView):
    pass

class FeedbackPromptView(BaseFeedbackPromptView):
    pass

class VerifyOTPView(BaseVerifyOTPView):
    pass

class ScanQrAPIView(BaseScanQrAPIView):
    pass

class QRScanAnalyticsView(BaseQRScanAnalyticsView):
    pass

class PasswordResetRequestView(BasePasswordResetRequestView):
    pass

class PasswordResetConfirmView(BasePasswordResetConfirmView):
    pass

class UpdateMetricsView(BaseUpdateMetricsView):
    pass

class SubscriptionCreateView(BaseSubscriptionCreateView):
    pass

class SubscriptionVerifyView(BaseSubscriptionVerifyView):
    pass

class SubscriptionCancelView(BaseSubscriptionCancelView):
    pass

class WebhookHandlerView(BaseWebhookHandlerView):
    pass

class SubscriptionListView(BaseSubscriptionListView):
    pass

class CancellationRequestListView(BaseCancellationRequestListView):
    pass