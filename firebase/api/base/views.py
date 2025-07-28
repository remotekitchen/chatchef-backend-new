from rest_framework.exceptions import ParseError
from rest_framework.generics import CreateAPIView, DestroyAPIView, get_object_or_404

from firebase.api.base.serializers import BaseFirebasePushTokenSerializer, BaseCompanyPushTokenSerializer, FCMTokenSerializer
from firebase.models import FirebasePushToken, CompanyPushToken, TokenFCM
from rest_framework import generics, permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status, viewsets
from firebase_admin import messaging
from django.contrib.auth import get_user_model
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from firebase_admin.messaging import Message, Notification, send
from firebase.utils.fcm_helper import send_push_notification
from firebase_admin.exceptions import FirebaseError
from rest_framework.decorators import action
from firebase.utils.fcm_helper import send_push_notification

User = get_user_model()



class BaseFirebasePushTokenCreateAPIView(CreateAPIView):
    serializer_class = BaseFirebasePushTokenSerializer
    print("hello test")
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


class BaseFirebasePushTokenDestroyAPIView(DestroyAPIView):
    model = FirebasePushToken

    def get_object(self):
        token = self.request.query_params.get('push_token', None)
        if token is None:
            raise ParseError('push_token must be provided!')
        return get_object_or_404(self.model, push_token=token)


class BaseCompanyPushTokenCreateAPIView(CreateAPIView):
    serializer_class = BaseCompanyPushTokenSerializer

    def perform_create(self, serializer):
        serializer.save(company=self.request.user.company)


class BaseCompanyPushTokenDestroyAPIView(BaseFirebasePushTokenDestroyAPIView):
    model = CompanyPushToken


class FCMTokenViewSet(viewsets.ModelViewSet):
    queryset = TokenFCM.objects.all()
    serializer_class = FCMTokenSerializer
    permission_classes = [permissions.IsAuthenticated]

    def create(self, request, *args, **kwargs):
        user = request.user
        token = request.data.get("token")
        device_type = request.data.get("device_type", "web")

        # Check if this token already exists
        existing_token = TokenFCM.objects.filter(token=token).first()
        
        if existing_token:
            existing_token.user = user
            existing_token.device_type = device_type
            existing_token.save()
            return Response({"message": "Token reassigned to current user", "token_id": existing_token.id})

        # 🔴 Remove this line to keep previous tokens
        # TokenFCM.objects.filter(user=user, device_type=device_type).delete()

        # Create a new token entry without deleting the old one
        fcm_token = TokenFCM.objects.create(user=user, token=token, device_type=device_type)

        return Response({"message": "Token registered successfully", "token_id": fcm_token.id})

    @action(detail=False, methods=["GET"])
    def get_user_tokens(self, request):
        """Retrieve all FCM tokens for the authenticated user"""
        user = request.user
        tokens = TokenFCM.objects.filter(user=user).values("id", "token", "device_type")

        return Response({"tokens": list(tokens)})




class SendNotificationView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user
        title = request.data.get("title", "No Title")
        body = request.data.get("body", "No Body")
        send_to_all = request.data.get("send_to_all", False)  # Optional flag
        fcm_token = request.data.get("fcm_token")  # Specific token (optional)

        # Fetch all tokens for the current authenticated user
        tokens = list(TokenFCM.objects.filter(user=user).values_list("token", flat=True))
        print("tokens", tokens)

        # If `send_to_all` is False and `fcm_token` is provided, send to only that token
        if not send_to_all and fcm_token:
            tokens = [fcm_token] if fcm_token in tokens else []

        if not tokens:
            return Response({"error": "No valid FCM tokens found"}, status=400)

        try:
            data={
                  "campaign_title": title,
                  "campaign_message": body,
                  "screen": "restaurant",
                  "id": 100  
            }
            response = send_push_notification(tokens, data)
            return Response(response)
        except Exception as e:
            return Response({"error": str(e)}, status=500)
