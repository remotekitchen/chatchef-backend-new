from firebase.api.base.views import BaseFirebasePushTokenCreateAPIView, BaseCompanyPushTokenCreateAPIView, \
    BaseFirebasePushTokenDestroyAPIView, BaseCompanyPushTokenDestroyAPIView
from firebase.api.v1.serializers import FirebasePushTokenSerializer, CompanyPushTokenSerializer


class FirebasePushTokenCreateAPIView(BaseFirebasePushTokenCreateAPIView):
    serializer_class = FirebasePushTokenSerializer


class CompanyPushTokenCreateAPIView(BaseCompanyPushTokenCreateAPIView):
    serializer_class = CompanyPushTokenSerializer


class FirebasePushTokenDestroyAPIView(BaseFirebasePushTokenDestroyAPIView):
    pass


class CompanyPushTokenDestroyAPIView(BaseCompanyPushTokenDestroyAPIView):
    pass

