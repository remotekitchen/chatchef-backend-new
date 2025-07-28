from django.urls import path

from billing.api.v2.views import (
    CreateQuoteAPIView,
    OrderRetrieveUpdateDestroyAPIView,
    StripePaymentAPIView,
    OTPSendAPIView,
    VerifyOTPAPIView,
    RemoteKitchenCreateDeliveryAPIView,
    CostCalculationAPIView,
    RemotekitchenOrderAPIView
    
)

urlpatterns = [
    path("create-quote/", CreateQuoteAPIView.as_view(), name="create-quote"),
    # path("create-delivery/", RemoteKitchenCreateDeliveryAPIView.as_view(), name="create-delivery"), 
    path("stripe/", StripePaymentAPIView.as_view(), name="stripe"),
    path("order/item/", OrderRetrieveUpdateDestroyAPIView.as_view(), name="order-item"),
    path("otp/send/", OTPSendAPIView.as_view(), name="otp-send"),
    path("otp/verify/", VerifyOTPAPIView.as_view(), name="otp-verify"),
     path(
        "cost-calculation/",CostCalculationAPIView.as_view(), name="cost-calculation"
    ),
        path('cash/remotekitchen', RemotekitchenOrderAPIView.as_view(), name="remotekitchen-order"),

]
