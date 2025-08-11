from rest_framework.permissions import IsAuthenticatedOrReadOnly

from billing.api.base.serializers import (
    BaseOrderGetSerializerWithModifiersDetails, BaseOrderSerializer)
from billing.api.base.views import (
    BaseOrderDeliveryExpenseAPI,BaseBillingProfileRetrieveUpdateDestroyAPIView, BaseCancelDeliveryAPIView,
    BaseConnectStripeAPIView, BaseCostCalculationAPIView,
    BaseCreateDeliveryAPIView, BaseCreateOrderAPIView,
    BaseCustomersWhoDontOrder, BaseDailySaleListAPIView,
    BaseDeliveryFeeAssociationApiView, BaseDeliveryFeeAssociationRUD,
    BaseDoordashAcceptQuoteAPIView, BaseDoordashCreateQuoteAPIView,
    BaseDoordashOrderStatusAPIView, BaseGenerateInvoiceAPIView,BaseGETInvoicesForHungry, BaseGenerateInvoiceForHungry,
    BaseGETInvoices, BaseSendInvoiceEmailView, BaseGiftCardApiView, BaseHourlySaleListAPIView,
    BaseInvoiceExcelAPIView, BaseInvoiceListAPIView, BaseMerchantOrderListAPI,
    BaseOrderListAPIView, BaseOrderReportAPIView,
    BaseOrderRetrieveUpdateDestroyAPIView, BasePaymentDetailsListCreateAPIView,
    BasePaymentDetailsRetrieveUpdateDestroyAPIView,
    BasePaymentInitiationReportAPIView, BasePaymentMethodSavedView,
    BasePaypalCaptureOrderAPIView, BasePaypalCreateOrderAPIView,
    BasePayPalTopUpCaptureAPIView, BaseRecreateDeliveryAPIView,
    BaseRemoteKitchenRaiderCheckAddress, BaseRestaurantFeeListCreateAPIView,
    BaseRestaurantRetrieveUpdateDestroyAPIView,
    BaseStripeConnectAccountRetrieveAPIView, BaseStripePaymentAPIView,
    BaseStripeRefundAPIView, 
    BaseTopUpView, BaseTransactionsModelAPIView,
    BaseUberCreatetQuoteAPIView, BaseUberDeliveryAPI,
    BaseUberOrderStatusAPIView, BaseWalletApiView, BaseWalletPaymentAPIView, 
    BaseCashOrderApiView,
    BaseUnregisteredGiftCardListView, BaseConfirmGiftCardStripePaymentApiView,
    BaseSendOrderReceiptAPIView, BaseRefundViewSet, BaseRemotekitchenOrderAPIView,
    BaseRestaurantFeeApiView, BaseOrderUserCancelAPIView, BaseExportUserOrderExcelAPIView,

    BaseExportCustomerOrders,BaseCartValidationAPIView,BaseUberStuckOrdersAPIView,BaseVRInvoiceWebhookView,

   BaseExportCustomerOrders,BaseCartValidationAPIView,BaseUberStuckOrdersAPIView,BaseOrderDetailsWithHistoryAPIView,BasePendingOrdersAPIView

    )

from billing.api.v1.serializers import (BillingProfileSerializer,
                                        DailyOrderSerializer,
                                        HourlyOrderSerializer, OrderSerializer,
                                        RestaurantFeeSerializer,
                                        StripeConnectAccountSerializer)


class OrderListAPIView(BaseOrderListAPIView):
    # serializer_class = OrderSerializer
    def get_serializer_class(self):
        if self.request.method == 'GET':
            return BaseOrderGetSerializerWithModifiersDetails
        else:
            return BaseOrderSerializer


class OrderRetrieveUpdateDestroyAPIView(BaseOrderRetrieveUpdateDestroyAPIView):
    serializer_class = OrderSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]

    def get_serializer_class(self):
        if self.request.method == 'GET':
            return BaseOrderGetSerializerWithModifiersDetails
        else:
            return BaseOrderSerializer


class DailySaleListAPIView(BaseDailySaleListAPIView):
    serializer_class = DailyOrderSerializer


class HourlySaleListAPIView(BaseHourlySaleListAPIView):
    serializer_class = HourlyOrderSerializer


class DoordashCreateQuoteAPIView(BaseDoordashCreateQuoteAPIView):
    pass


class DoordashAcceptQuoteAPIView(BaseDoordashAcceptQuoteAPIView):
    pass


class MerchantOrderListAPI(BaseMerchantOrderListAPI):

    def get_serializer_class(self):
        if self.request.method == 'GET':
            return BaseOrderGetSerializerWithModifiersDetails
        else:
            return BaseOrderSerializer


class PaypalCreateOrderAPIView(BasePaypalCreateOrderAPIView):
    pass


class PaypalCaptureOrderAPIView(BasePaypalCaptureOrderAPIView):
    pass


# Uber Delivery

class UberCreateQuoteAPIView(BaseUberCreatetQuoteAPIView):
    pass


class UberCreateDeliveryApiView(BaseUberDeliveryAPI):
    pass


class PaymentDetailsListCreateAPIView(BasePaymentDetailsListCreateAPIView):
    pass


class PaymentDetailsRetrieveUpdateDestroyAPIView(BasePaymentDetailsRetrieveUpdateDestroyAPIView):
    pass


class CreateOrderAPIView(BaseCreateOrderAPIView):
    pass


class StripePaymentAPIView(BaseStripePaymentAPIView):
    pass


class WalletPaymentAPIView(BaseWalletPaymentAPIView):
    pass

class RemotekitchenOrderAPIView(BaseRemotekitchenOrderAPIView):
    pass

class CashOrderApiView(BaseCashOrderApiView):
    pass

class StripeRefundAPIView(BaseStripeRefundAPIView):
    pass


class InvoiceListAPIView(BaseInvoiceListAPIView):
    pass


class DeliveryFeeAssociationApiView(BaseDeliveryFeeAssociationApiView):
    pass


class DeliveryFeeAssociationRUDView(BaseDeliveryFeeAssociationRUD):
    pass


class BillingProfileRetrieveUpdateDestroyAPIView(BaseBillingProfileRetrieveUpdateDestroyAPIView):
    serializer_class = BillingProfileSerializer


class StripeConnectAccountRetrieveAPIView(BaseStripeConnectAccountRetrieveAPIView):
    serializer_class = StripeConnectAccountSerializer


class ConnectStripeAPIView(BaseConnectStripeAPIView):
    pass


class CostCalculationAPIView(BaseCostCalculationAPIView):
    pass


class OrderReportAPIView(BaseOrderReportAPIView):
    pass


class PaymentInitiationReportAPIView(BasePaymentInitiationReportAPIView):
    pass


class TopUpView(BaseTopUpView):
    pass


class PayPalTopUpCaptureAPIView(BasePayPalTopUpCaptureAPIView):
    pass


class PaymentMethodSavedView(BasePaymentMethodSavedView):
    pass


class DoordashOrderStatusAPIView(BaseDoordashOrderStatusAPIView):
    pass


class UberOrderStatusAPIView(BaseUberOrderStatusAPIView):
    pass


class RecreateDeliveryAPIView(BaseRecreateDeliveryAPIView):
    pass


class RestaurantFeeListCreateAPIView(BaseRestaurantFeeListCreateAPIView):
    serializer_class = RestaurantFeeSerializer


class RestaurantRetrieveUpdateDestroyAPIView(BaseRestaurantRetrieveUpdateDestroyAPIView):
    serializer_class = RestaurantFeeSerializer


class InvoiceExcelAPIView(BaseInvoiceExcelAPIView):
    pass


class CancelDeliveryAPIView(BaseCancelDeliveryAPIView):
    pass


class CreateDeliveryAPIView(BaseCreateDeliveryAPIView):
    pass


class GenerateInvoiceAPIView(BaseGenerateInvoiceAPIView):
    pass

class GenerateInvoiceForHungry(BaseGenerateInvoiceForHungry):
    pass


class GETInvoicesForHungry(BaseGETInvoicesForHungry):
    pass

class CustomersWhoDontOrder(BaseCustomersWhoDontOrder):
    pass


class GETInvoices(BaseGETInvoices):
    pass

class SendInvoiceEmailView(BaseSendInvoiceEmailView):
    pass

class TransactionsModelAPIView(BaseTransactionsModelAPIView):
    pass


class WalletApiView(BaseWalletApiView):
    pass


class GiftCardApiView(BaseGiftCardApiView):
    pass


class RemoteKitchenRaiderCheckAddress(BaseRemoteKitchenRaiderCheckAddress):
    pass

class UnregisteredGiftCardView(BaseUnregisteredGiftCardListView): 
    pass
  
class ConfirmGiftCardStripePaymentApiView(BaseConfirmGiftCardStripePaymentApiView): 
    pass
  
class SendOrderReceiptAPIView(BaseSendOrderReceiptAPIView): 
    pass
  

class RefundViewSet(BaseRefundViewSet):
    pass

class RestaurantFeeApiView(BaseRestaurantFeeApiView):
  pass


class OrderDeliveryExpenseAPI(BaseOrderDeliveryExpenseAPI):
    pass


class OrderUserCancelAPIView(BaseOrderUserCancelAPIView):
    pass


class ExportUserOrderExcelAPIView(BaseExportUserOrderExcelAPIView):
    pass


# class GenerateVRInvoiceAPIView(BaseGenerateVRInvoiceAPIView):
#     pass

# class SendVRInvoiceAPIView(BaseSendVRInvoiceAPIView):
#     pass


class ExportCustomerOrders(BaseExportCustomerOrders):
    pass


class CartValidationAPIView(BaseCartValidationAPIView):
    pass


class UberStuckOrdersAPIView(BaseUberStuckOrdersAPIView):
    pass


class VRInvoiceWebhookView(BaseVRInvoiceWebhookView):
    pass

class OrderDetailsWithHistoryAPIView(BaseOrderDetailsWithHistoryAPIView):
    pass

class PendingOrdersAPIView(BasePendingOrdersAPIView):
    pass