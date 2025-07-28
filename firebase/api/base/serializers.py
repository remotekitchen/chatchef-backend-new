from rest_framework import serializers
from firebase.models import FirebasePushToken, CompanyPushToken, TokenFCM


class BaseFirebasePushTokenSerializer(serializers.ModelSerializer):
    class Meta:
        model = FirebasePushToken
        fields = '__all__'
        extra_kwargs = {
            'user': {
                'read_only': True
            }
        }


class BaseCompanyPushTokenSerializer(serializers.ModelSerializer):
    class Meta:
        model = CompanyPushToken
        fields = '__all__'
        extra_kwargs = {
            'company': {
                'read_only': True
            }
        }



class FCMTokenSerializer(serializers.ModelSerializer):
    class Meta:
        model = TokenFCM
        fields = ["id", "user", "token", "device_type"]
        extra_kwargs = {"user": {"read_only": True}}