
from rest_framework import serializers

from communication.models import (EmailCampaignHistory, GroupInvitationOR,
                                  whatsAppCampaignHistory)
from core.api.serializers import BaseSerializer


class BaseTwiloSerializer(BaseSerializer):
    msg_from = serializers.CharField()
    msg_to = serializers.CharField()
    body = serializers.CharField()
    restaurant = serializers.CharField(
        allow_blank=True, required=False, allow_null=True)


class BaseWhatsAppOfferSerializer(serializers.ModelSerializer):
    class Meta:
        model = whatsAppCampaignHistory
        fields = '__all__'


class GroupInvitationORSerializer(serializers.ModelSerializer):
    class Meta:
        model = GroupInvitationOR
        fields = '__all__'


class EmailCampaignSerializer(serializers.ModelSerializer):
    class Meta:
        model = EmailCampaignHistory
        fields = '__all__'
