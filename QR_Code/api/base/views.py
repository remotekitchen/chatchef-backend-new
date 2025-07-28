
from http.client import responses

import requests
from rest_framework.generics import (GenericAPIView, ListAPIView,
                                     ListCreateAPIView, RetrieveAPIView,
                                     RetrieveUpdateDestroyAPIView,
                                     get_object_or_404)
from rest_framework.response import Response
from rest_framework.views import APIView

from food.api.base.views import BaseLocationRetrieveUpdateDestroyAPIView
from food.models import Location, Restaurant
from QR_Code.api.base.serializers import (baseQRCodeSerializer,
                                          baseQRScannedCountSerializer)
from QR_Code.models import QrCode


class BaseQRCodeGeneratorView(GenericAPIView):

    def get(self, request, location_id):
        location_instance = get_object_or_404(Location, id=location_id)

        restaurant = location_instance.restaurant

        restaurant_slug = restaurant.slug
        location_slug = location_instance.slug
        print("res", location_instance.restaurant_id)
        qr_code_instance = QrCode.objects.filter(
            restaurant_id=restaurant.id, location=location_instance)
        if not qr_code_instance.exists():
            qr = QrCode.objects.create(
                restaurant=restaurant,
                location=location_instance,
                table_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=table&restaurant_id={restaurant.id}&location_id={location_id}",
                banner_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=banner&restaurant_id={restaurant.id}&location_id={location_id}",
                social_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=social&restaurant_id={restaurant.id}&location_id={location_id}",
                poster_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=poster&restaurant_id={restaurant.id}&location_id={location_id}",
                business_card_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=business_card&restaurant_id={restaurant.id}&location_id={location_id}",
                flyer_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=flyer&restaurant_id={restaurant.id}&location_id={location_id}",
                coupon_qrlink=f"https://order.chatchefs.com/{restaurant_slug}/{location_slug}?qr=coupon&restaurant_id={restaurant.id}&location_id={location_id}",

                table_qrlink_scanned=0,
                banner_qrlink_scanned=0,
                social_qrlink_scanned=0,
                poster_qrlink_scanned=0,
            )
            print("QR", qr)
            QR = baseQRCodeSerializer(qr)
            return Response(QR.data)
        else:
            # Use .first() to get the first instance from the queryset
            QR_instance = baseQRCodeSerializer(qr_code_instance.first())
            print("xxxxxxxx", qr_code_instance)
            return Response(QR_instance.data)
        # Instantiate the BaseLocationRetrieveUpdateDestroyAPIView


class BaseQrScanedCountApi(GenericAPIView):
    serializer_class = baseQRScannedCountSerializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)

        if serializer.is_valid(raise_exception=True):
            data = serializer.data

            restaurant_id = data.get("restaurant_id")
            location_id = data.get("location_id")
            qr = data.get('qr')
            qr_code_instance = QrCode.objects.filter(
                restaurant_id=restaurant_id, location_id=location_id).first()

            if qr_code_instance:
                if qr == "all":
                    QR_code_stauts = baseQRCodeSerializer(qr_code_instance)
                    return Response({'result': 'success', 'data': {'qr_code_instance': QR_code_stauts.data}})
                elif qr in ["table", "banner", "social", "poster", "business_card", "flyer", "coupon"]:
                    # Increment the corresponding scanned count
                    setattr(qr_code_instance, f"{qr}_qrlink_scanned", getattr(
                        qr_code_instance, f"{qr}_qrlink_scanned") + 1)

                    qr_code_instance.save()
                    QR_code_stauts = baseQRCodeSerializer(qr_code_instance)
                    return Response({'result': 'success', 'data': {'qr_code_instance': QR_code_stauts.data}})
                else:
                    return Response({'result': 'error', 'message': 'Invalid qr parameter'})
            else:
                return Response({'result': 'error', 'message': 'QR code not found for the given restaurant and location'})
