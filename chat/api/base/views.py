from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAuthenticated

from chat.api.base.serializer import BaseMessageSerializer
from chat.models import Message


class BaseMessageListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseMessageSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Message.objects.filter(user=self.request.user)

    def perform_create(self, serializer):
        return serializer.save(user=self.request.user)
