from rest_framework import serializers

from analytics.models import VisitorAnalytics


class DashboardSerializer(serializers.Serializer):
    month = serializers.CharField()
    value = serializers.IntegerField()


class ParseDate(serializers.Serializer):
    start = serializers.DateField()
    end = serializers.DateField()


class BaseVisitorAnalyticsModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = VisitorAnalytics
        fields = "__all__"
