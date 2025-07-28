from communication.api.base.views import (
    BaseGroupInvitationORModelView, BaseGroupInvitationORreadOnlyModelView,BaseWhatsAppTemplateSent,BaseWhatsAppCampaignHistory,
    BaseTwiloSendMsgView)


class whatsAppApiView(BaseTwiloSendMsgView):
    pass


class GroupInvitationORModelView(BaseGroupInvitationORModelView):
    pass


class GroupInvitationORreadOnlyModelView(BaseGroupInvitationORreadOnlyModelView):
    pass

class WhatsAppTemplateSent(BaseWhatsAppTemplateSent):
    pass

class WhatsAppCampaignHistory(BaseWhatsAppCampaignHistory):
    pass

