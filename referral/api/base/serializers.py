from rest_framework import serializers

from accounts.api.base.serializers import BaseUserSerializer
from hungrytiger.settings.defaults import ENV_TYPE
from referral.models import InviteCodes, Referral


class ReferredUserSerializer(BaseUserSerializer):
    class Meta(BaseUserSerializer.Meta):
        fields = ["id", "first_name", "last_name"]


class BaseReferralSerializer(serializers.ModelSerializer):
    class Meta:
        model = Referral
        fields = '__all__'


class BaseInviteCodeSerializer(serializers.ModelSerializer):
    link = serializers.SerializerMethodField()

    class Meta:
        model = InviteCodes
        fields = '__all__'

    def get_link(self, obj: InviteCodes):
        head = 'order'
        if ENV_TYPE == "DEVELOPMENT":
            head = 'dev'

        return f"https://{head}.chatchefs.com/{obj.refer.restaurant.slug}/{obj.refer.location.slug}/account/signup?refer={obj.code}"


class BaseGetReferralSerializer(BaseReferralSerializer):
    invited_users = ReferredUserSerializer(many=True)
    joined_users = ReferredUserSerializer(many=True)
    invites = serializers.SerializerMethodField()

    def get_invites(self, obj: Referral):
        return BaseInviteCodeSerializer(InviteCodes.objects.filter(refer=obj.id), many=True).data
