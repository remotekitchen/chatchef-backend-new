
from QR_Code.models import QrCode

import django.db
import django.db.models
import food.models
from rest_framework import serializers

class baseQRCodeSerializer(serializers.ModelSerializer):
    class Meta:
        model = QrCode
        fields = '__all__'

class baseQRScannedCountSerializer(serializers.Serializer):
    restaurant_id=serializers.IntegerField()
    location_id=serializers.IntegerField()
    qr=serializers.CharField()