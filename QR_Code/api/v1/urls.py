
from django.urls import path

from QR_Code.api.base.views import BaseQrScanedCountApi,BaseQRCodeGeneratorView
from .views import  QrScanedCountApi



urlpatterns = [
    # Correct: using the class-based view without calling it
    path('generate_qr/<int:location_id>/', BaseQRCodeGeneratorView.as_view(), name='Generate_QRCode'),
    path ('visit_count/',QrScanedCountApi.as_view(),name="Visit_Count"),
]
