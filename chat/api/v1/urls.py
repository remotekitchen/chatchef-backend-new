from django.urls import path

from chat.api.v1.views import MessageListCreateAPIView

urlpatterns = [
    path('message/', MessageListCreateAPIView.as_view(), name='message-list-create')
]