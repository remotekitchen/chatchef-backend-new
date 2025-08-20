from django.urls import include, path
from rest_framework.routers import DefaultRouter


from lark.api.base.views import lark_generate_invoice,send_invoice_pdf,lark_DO_update,lark_ht_update

router = DefaultRouter()


urlpatterns = [
    path("", include(router.urls)),
   path("ht/update/", lark_ht_update, name="lark-ht-update"),
    path("do/update/", lark_DO_update, name="lark-ht-update"),
    path("invoice/generate_DO_or_HT", lark_generate_invoice, name="lark-ht-update"),
     path("invoice/send_email_do_or_HT", send_invoice_pdf, name="send_invoice_pdf"),

    # path("lark/webhook/", LarkWebhookAPIView.as_view()),

]
