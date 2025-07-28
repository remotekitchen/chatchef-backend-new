from django.urls import include, path
from rest_framework.routers import DefaultRouter

from communication.api.v1.views import (GroupInvitationORModelView,
                                        GroupInvitationORreadOnlyModelView,WhatsAppTemplateSent,WhatsAppCampaignHistory,
                                        whatsAppApiView)

router = DefaultRouter()
router.register('restaurant-access', GroupInvitationORModelView,
                basename='GroupInvitationORModelView')
router.register('public-access', GroupInvitationORreadOnlyModelView,
                basename='GroupInvitationORreadOnlyModelView')

urlpatterns = [
    path('wapp/send/', whatsAppApiView.as_view(), name='wapp-msg-send'),
    path('group-invitation/', include(router.urls)),
    path('whasApp-template/', WhatsAppTemplateSent.as_view()),
    path('whatsapp_campaign_history/',WhatsAppCampaignHistory.as_view()),
    

]
