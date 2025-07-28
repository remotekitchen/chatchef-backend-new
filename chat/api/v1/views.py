from chat.api.base.views import BaseMessageListCreateAPIView
from chat.api.v1.serializer import MessageSerializer


class MessageListCreateAPIView(BaseMessageListCreateAPIView):
    serializer_class = MessageSerializer
