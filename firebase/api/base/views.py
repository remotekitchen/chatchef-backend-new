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
        token = request.data.get("token")
        device_type = request.data.get("device_type", "web")

        if not token:
            return Response({"detail": "token is required"}, status=400)

        # If token exists → keep its original owner
        existing_token = TokenFCM.objects.select_related("user").filter(token=token).first()
        if existing_token:
            user = existing_token.user  # take from existing token
            if device_type and existing_token.device_type != device_type:
                existing_token.device_type = device_type
                existing_token.save(update_fields=["device_type"])
            return Response({
                "message": "Token already registered; using existing token owner",
                "token_id": existing_token.id,
                "user_id": user.id,
                "device_type": existing_token.device_type,
            })

        # If token is new → assign to current authenticated user
        fcm_token = TokenFCM.objects.create(
            user=request.user,
            token=token,
            device_type=device_type
        )
        return Response({
            "message": "Token registered successfully",
            "token_id": fcm_token.id,
            "user_id": request.user.id,
            "device_type": device_type,
        })

    @action(detail=False, methods=["GET"])
    def get_user_tokens(self, request):
        """Retrieve all FCM tokens for the authenticated user"""
        tokens = TokenFCM.objects.filter(user=request.user).values("id", "token", "device_type")
        return Response({"tokens": list(tokens)})

    @action(detail=False, methods=["GET"])
    def resolve_user(self, request):
        """Get user info from a given token"""
        token = request.query_params.get("token")
        if not token:
            return Response({"detail": "token is required"}, status=400)
        obj = TokenFCM.objects.select_related("user").filter(token=token).first()
        if not obj:
            return Response({"detail": "token not found"}, status=404)
        return Response({"user_id": obj.user_id, "device_type": obj.device_type})




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
        print("tokens", tokens, user.id)

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
