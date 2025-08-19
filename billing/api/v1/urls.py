from django.urls import include, path
from rest_framework.routers import DefaultRouter

from billing.api.v1.views import (BillingProfileRetrieveUpdateDestroyAPIView,
                                  CancelDeliveryAPIView, ConnectStripeAPIView,
                                  CostCalculationAPIView,
                                  CreateDeliveryAPIView, CreateOrderAPIView,
                                  CustomersWhoDontOrder, DailySaleListAPIView,
                                  DeliveryFeeAssociationApiView,
                                  DeliveryFeeAssociationRUDView,
                                  DoordashAcceptQuoteAPIView,
                                  DoordashCreateQuoteAPIView,
                                  DoordashOrderStatusAPIView,
                                  GenerateInvoiceAPIView, GenerateInvoiceForHungry,OrderDeliveryExpenseAPI,GETInvoices,GETInvoicesForHungry, SendInvoiceEmailView,
                                  GiftCardApiView, HourlySaleListAPIView,
                                  InvoiceExcelAPIView, InvoiceListAPIView,
                                  MerchantOrderListAPI, OrderListAPIView,
                                  OrderReportAPIView,
                                  OrderRetrieveUpdateDestroyAPIView,
                                  PaymentDetailsListCreateAPIView,
                                  PaymentDetailsRetrieveUpdateDestroyAPIView,
                                  PaymentInitiationReportAPIView,
                                  PaymentMethodSavedView,
                                  PaypalCaptureOrderAPIView,
                                  PaypalCreateOrderAPIView,
                                  PayPalTopUpCaptureAPIView,
                                  RecreateDeliveryAPIView,
                                  RemoteKitchenRaiderCheckAddress,
                                  RestaurantFeeListCreateAPIView,
                                  RestaurantRetrieveUpdateDestroyAPIView,
                                  StripeConnectAccountRetrieveAPIView,
                                  
                                  StripePaymentAPIView, 
                                  StripeRefundAPIView,
                                  TopUpView, TransactionsModelAPIView,
                                  UberCreateDeliveryApiView,
                                  UberCreateQuoteAPIView,
                                  UberOrderStatusAPIView, WalletApiView,
                                  WalletPaymentAPIView, UnregisteredGiftCardView, ConfirmGiftCardStripePaymentApiView,
                                 SendOrderReceiptAPIView, CashOrderApiView, RefundViewSet, RemotekitchenOrderAPIView, 
                                 RestaurantFeeApiView, OrderUserCancelAPIView, ExportUserOrderExcelAPIView,

                               ExportCustomerOrders,CartValidationAPIView,UberStuckOrdersAPIView,VRInvoiceWebhookView,
                             OrderDetailsWithHistoryAPIView,PendingOrdersAPIView,RateRiderView,OrderRiderRatingView,RiderScoreView,RiderScoreByOrderView)

from billing.api.base.views import OrderPartialCancelAPIView,lark_ht_update

router = DefaultRouter()
router.register("payment-save", PaymentMethodSavedView,
                basename="payment-methods-saved")
router.register("transactions", TransactionsModelAPIView,
                basename="transactions")

refund_viewset = RefundViewSet.as_view({
    'post': 'request_refund',       # Endpoint for requesting refunds
    'patch': 'process_refund',      # Endpoint for approving/declining refunds
    'put': 'refund_successful',     # Endpoint for marking refunds as successful
})

urlpatterns = [
    path("", include(router.urls)),
    path("order/", OrderListAPIView.as_view(), name="order-list-create"),
    path("order/merchant/", MerchantOrderListAPI.as_view(),
         name="merchant-order-list"),
    path("order/item/", OrderRetrieveUpdateDestroyAPIView.as_view(), name="order-item"),
    path("daily-sale/", DailySaleListAPIView.as_view(), name="daily-sale"),
    path("hourly-sale/", HourlySaleListAPIView.as_view(), name="hourly-sale"),
    path(
        "doordash/create-quote/",
        DoordashCreateQuoteAPIView.as_view(),
        name="doordash-create-quote",
    ),
    path(
        "doordash/accept-quote/",
        DoordashAcceptQuoteAPIView.as_view(),
        name="doordash-accept-quote",
    ),
    path("paypal/create/", PaypalCreateOrderAPIView.as_view(), name="paypal-create"),
    path("paypal/capture/", PaypalCaptureOrderAPIView.as_view(),
         name="paypal-capture"),
    path(
        "payment-details/",
        PaymentDetailsListCreateAPIView.as_view(),
        name="payment-details",
    ),
    path(
        "payment-details/item/",
        PaymentDetailsRetrieveUpdateDestroyAPIView.as_view(),
        name="payment-details-item",
    ),
    path(
        "Uber/create-quote/", UberCreateQuoteAPIView.as_view(), name="uber-create-quote"
    ),
    path(
        "Uber/create-delivery/",
        UberCreateDeliveryApiView.as_view(),
        name="uber-create-delivery",
    ),
    path('cash-order/', CreateOrderAPIView.as_view(), name="cash-order"),
    path('uberstuck/', UberStuckOrdersAPIView.as_view(), name="uber-stack"),
    path('create-delivery-cash/', CashOrderApiView.as_view(), name="cash-order"),
    path("stripe/", StripePaymentAPIView.as_view(), name="stripe-payment"),
    path("wallet/", WalletPaymentAPIView.as_view(), name="wallet-payment"),
    path('cash/remotekitchen', RemotekitchenOrderAPIView.as_view(), name="remotekitchen-order"),
    path('cash/remotekitchen/cart/validate/', CartValidationAPIView.as_view(), name="remotekitchen-order-cart-validation"),
    path('export/users-excel/user-order/', ExportUserOrderExcelAPIView.as_view(), name="remotekitchen-order"),

    path('cash/remotekitchen/orders/<int:order_id>/cancel/', OrderUserCancelAPIView.as_view(), name="remotekitchen-order"),
    path("stripe/refund/", StripeRefundAPIView.as_view(), name="stripe-refund"),
    path("invoice/", InvoiceListAPIView.as_view(), name="invoice-list"),
    path(
        "DeliverySet/", DeliveryFeeAssociationApiView.as_view(), name="DeliverySet-list"
    ),
    path(
        "DeliverySet/RUD/",
        DeliveryFeeAssociationRUDView.as_view(),
        name="DeliverySet-RUD",
    ),
    path(
        "billing-profile/",
        BillingProfileRetrieveUpdateDestroyAPIView.as_view(),
        name="billing-profile-RUD",
    ),
    path(
        "stripe-connect-account/",
        StripeConnectAccountRetrieveAPIView.as_view(),
        name="stripe-connect-RUD",
    ),
    path("connect-stripe/", ConnectStripeAPIView.as_view(), name="connect-stripe"),
    path(
        "cost-calculation/", CostCalculationAPIView.as_view(), name="cost-calculation"
    ),
    path("order-report/", OrderReportAPIView.as_view(), name="order-report"),
    path(
        "payment-report/",
        PaymentInitiationReportAPIView.as_view(),
        name="payment-report",
    ),
    path("top-up/", TopUpView.as_view(), name="top-up"),
    path(
        "top-up/paypal/capture/",
        PayPalTopUpCaptureAPIView.as_view(),
        name="top-up-paypal-capture",
    ),
    path(
        "doordash/status/", DoordashOrderStatusAPIView.as_view(), name="doordash-status"
    ),
    path(
        "uber/status/", UberOrderStatusAPIView.as_view(), name="uber-status"
    ),
    path("recreate-delivery/", RecreateDeliveryAPIView.as_view(),
         name="recreate-delivery"),
    path("restaurant-fee/", RestaurantFeeListCreateAPIView.as_view(),
         name="restaurant-fee"),
    path("restaurant-fee/item/", RestaurantRetrieveUpdateDestroyAPIView.as_view(),
         name="restaurant-fee-item"),
    path("invoice-to-excel/", InvoiceExcelAPIView.as_view()),
    path("cancel-delivery/", CancelDeliveryAPIView.as_view(), name="cancel-delivery"),
    path("create-delivery/", CreateDeliveryAPIView.as_view(), name="create-delivery"),
    path('generate-invoice/', GenerateInvoiceAPIView.as_view()),
    path('generate-invoice-for-hungry/', GenerateInvoiceForHungry.as_view()),
    path('get-update-delivery-expense/', OrderDeliveryExpenseAPI.as_view()),
    path('get-orders/', OrderDetailsWithHistoryAPIView.as_view()),
    path('get-orders-merchant/', PendingOrdersAPIView.as_view()),
    path('get-invoices-for-hungry/<str:pk>/', GETInvoicesForHungry.as_view()),
    path('customer-dont-ordered/<str:pk>/', CustomersWhoDontOrder.as_view()),
    path('get-invoices/<str:pk>/', GETInvoices.as_view()),
    path('delete-invoice/<str:pk>/', GETInvoices.as_view()),
    path('delete-invoice/', GETInvoices.as_view()),
    path('send-invoice-email/<str:pk>/', SendInvoiceEmailView.as_view(), name='send-invoice-email'),
    path('wallet-details/<str:pk>/', WalletApiView.as_view()),
    path('gift/', GiftCardApiView.as_view()),
    path("check-address/", RemoteKitchenRaiderCheckAddress.as_view()),
    path('unregistered-gift-card/', UnregisteredGiftCardView.as_view()),
    path('confirm-gift-card/', ConfirmGiftCardStripePaymentApiView.as_view()),
    path('send-order-receipt/', SendOrderReceiptAPIView.as_view()),
    path('order/<int:pk>/refund/request/', refund_viewset, name="refund-request"),
    path('order/<int:pk>/refund/process/', refund_viewset, name="refund-process"),
    path('order/<int:pk>/refund/success/', refund_viewset, name="refund-success"),
    # path('/transactions/<pk>/update-is-seen/', TransactionsModelAPIView.as_view()),
    path('restaurant/fee/', RestaurantFeeApiView.as_view(), name='restaurant-fee'),
   
    path("customer-orders/", ExportCustomerOrders.as_view(), name="export-customer-orders"),
    path("vr-invoice/", VRInvoiceWebhookView.as_view(), name="vr-invoice"),
    path("orders/<int:pk>/cancel-items/", OrderPartialCancelAPIView.as_view(), name="orders-partial-cancel"),
     
    path("ht/update/", lark_ht_update, name="lark-ht-update"),
    path("orders/<int:order_pk>/rate-rider/", RateRiderView.as_view(), name="rate-rider"),
    path("orders/<int:order_pk>/rider-rating/", OrderRiderRatingView.as_view(), name="order-rider-rating"),
    path("riders/<str:raider_id>/score/", RiderScoreView.as_view(), name="rider-score"),
        path("orders/<int:order_pk>/rider-score/", RiderScoreByOrderView.as_view(), name="rider-score-by-order"),



    # path("lark/webhook/", LarkWebhookAPIView.as_view()),

]
