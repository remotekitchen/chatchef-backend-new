from allauth.socialaccount.providers.google.views import GoogleOAuth2Adapter as BaseGoogleOAuth2Adapter
from allauth.socialaccount.providers.apple.views import AppleOAuth2Adapter as BaseAppleOAuth2Adapter

from accounts.models import User
from core.utils import get_logger

logger = get_logger()


class PreSocialLoginMixin:
    def pre_social_login(self, request, sociallogin):
        user = sociallogin.user
        if user.id:
            return
        if not user.email:
            return

        try:
            user = User.objects.get(
                email=user.email)  # if user exists, connect the account to the existing account and login
            sociallogin.connect(request, user)
        except User.DoesNotExist:
            pass
        except Exception as e:
            logger.exception('Social adapter failure - {}'.format(e))


class GoogleOAuth2Adapter(PreSocialLoginMixin, BaseGoogleOAuth2Adapter):
    pass


class AppleOAuth2Adapter(PreSocialLoginMixin, BaseAppleOAuth2Adapter):
    pass