import random

from allauth.socialaccount.providers.apple.client import AppleOAuth2Client
from allauth.socialaccount.providers.facebook.views import \
    FacebookOAuth2Adapter
from allauth.socialaccount.providers.oauth2.client import OAuth2Client
from dj_rest_auth.registration.views import SocialConnectView, SocialLoginView
from django.db.models import Q
from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import (ParseError, PermissionDenied,
                                       ValidationError)
from rest_framework.generics import (CreateAPIView, GenericAPIView,
                                     ListCreateAPIView, RetrieveAPIView,
                                     RetrieveUpdateDestroyAPIView,
                                     UpdateAPIView)
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from datetime import timedelta
from django.db.models import Sum
from collections import defaultdict
from accounts.models import Customer, Subscription, CancellationRequest

from accounts.api.base.serializers import (
    BaseSubscriptionCreateSerializer,
    BaseSubscriptionResponseSerializer,
    BaseSubscriptionVerifySerializer,
    BaseCancellationRequestSerializer,
    BaseWebhookSerializer,
)

from accounts.api.adapters import AppleOAuth2Adapter, GoogleOAuth2Adapter
from accounts.api.base.serializers import (BaseChangePasswordSerializer,
                                           BaseContactSerializer,
                                           BaseEmailPasswordLoginSerializer,
                                           BaseRestaurantUserGETSerializer,
                                           BaseRestaurantUserSerializer,
                                           BaseUserAddressSerializer,
                                           BaseUserSerializer,
                                           SocialLoginSerializer,PhoneOtpLoginSerializer,BaseFeedbackPromptSerializer)
from accounts.api.v1.serializers import UserSerializer, SalesRankingSerializer, InvitedUserSerializer
from accounts.models import Contacts, Otp, RestaurantUser, User, UserAddress,BlockedPhoneNumber,UserSession,FeedbackPrompt
from billing.models import Order
from referral.models import Referral
from core.api.mixins import GetObjectWithParamMixin
from core.api.paginations import StandardResultsSetPagination
from core.api.permissions import (HasCompanyAccess, HasRestaurantAccess,
                                  IsObjOwner)
from core.utils import get_debug_str, get_logger
from food.models import Location
from marketing.utils.send_sms import send_sms_bd
from marketing.utils.send_sms import send_sms_twilio
from billing.utiils import (MakeTransactions)
from django.db import transaction
from billing.models import UnregisteredGiftCard
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import AllowAny
from django.db.models import Prefetch
from django.shortcuts import get_object_or_404

from django.conf import settings
from django.core.mail import send_mail
from django.utils.crypto import get_random_string

from marketing.email_sender import send_email # Your existing utility
from accounts.utils import  send_email_verification_otp ,send_password_reset_otp_email # UUID logic

from django.contrib.auth import get_user_model
from django.utils.dateparse import parse_date
from accounts.models import DAURecord, UserEvent, UserEngagementSegment, UserEngagementSegment,UserChurnStatus, CohortRetentionRecord, UserCohort, ConversionFunnelRecord
from accounts.utils import get_bdt_time, get_client_ip
from django.shortcuts import redirect
from accounts.models import QRScan
from django.http import JsonResponse
from django.db.models.functions import TruncDate
from django.db.models import  Count
from datetime import timedelta
from datetime import datetime
from django.utils.timezone import now
import pytz
from django.http import HttpResponse
import csv
import uuid
import stripe
from hungrytiger.settings import env


logger = get_logger()



class AbstractBaseLoginView(GenericAPIView):
    authentication_classes = []

    class Meta:
        abstract = True

    def post(self, request, *args, **kwargs):
        current_dt = timezone.now()
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid():
            logger.error(get_debug_str(
                request, request.user, serializer.errors))
            raise ValidationError(serializer.errors)

        user = serializer.validated_data.get("user")
        created = user.date_joined >= current_dt
        direct_order = request.query_params.get('direct_order', False)
        if not direct_order and user.role == User.RoleType.NA:
            raise PermissionDenied('User does not have any company!')

        user_serializer = UserSerializer(
            instance=user, context={"request": request})
        token, _ = Token.objects.get_or_create(user=user)

        resp = {
            "token": token.key,
            "user_info": user_serializer.data,
            "created": created,
        }

        # Get or Create a RestaurantUser instance for the logged in user
        restaurant = self.request.data.get("restaurant", None)
        location = self.request.data.get("location", None)
        if restaurant is not None:
            obj = RestaurantUser.objects.get_or_create(
                user=user, restaurant_id=restaurant
            )[0]
            resp["restaurantUser"] = BaseRestaurantUserSerializer(obj).data
            resp["is_old_user"] = user.is_old_user(restaurant=restaurant)
            resp["order_stage"] = user.order_count(restaurant=restaurant)

        return Response(resp, status=status.HTTP_200_OK)


class BaseUserRegistrationAPIView(CreateAPIView):
    serializer_class = BaseUserSerializer
    

# https://www.facebook.com/v12.0/dialog/oauth?client_id=8849698101762545&redirect_uri=http://localhost:8000/&response_type=token&scope=email


class BaseUserEmailVerifyView(APIView):
    def get(self, request, *args, **kwargs):
        uid = request.query_params.get("code", None)
        if not User.objects.filter(uid=uid).exists():
            return Response(
                {"message": "invalid request"}, status=status.HTTP_400_BAD_REQUEST
            )

        user = User.objects.get(uid=uid)
        if user.is_email_verified:
            return Response(
                {"message": "user already verified"}, status=status.HTTP_200_OK
            )

        user.is_email_verified = True
        user.save()
        return Response({"message": "user verified"}, status=status.HTTP_202_ACCEPTED)
    


class BaseEmailPasswordLoginAPIView(AbstractBaseLoginView):
    serializer_class = BaseEmailPasswordLoginSerializer


class BaseGoogleLoginAPIView(AbstractBaseLoginView):
    serializer_class = SocialLoginSerializer
    adapter_class = GoogleOAuth2Adapter
    client_class = OAuth2Client

    # def login(self):
    #     super().login()
    #     restaurant = self.request.data.get('restaurant', None)
    #     if restaurant is not None:
    #         RestaurantUser.objects.get_or_create(user=self.user, restaurant_id=restaurant)


class BaseGoogleConnectAPIView(SocialConnectView):
    adapter_class = GoogleOAuth2Adapter


class BaseFacebookLogin(AbstractBaseLoginView):
    serializer_class = SocialLoginSerializer
    adapter_class = FacebookOAuth2Adapter
    client_class = OAuth2Client


class BaseAppleLoginAPIView(AbstractBaseLoginView):
    serializer_class = SocialLoginSerializer
    adapter_class = AppleOAuth2Adapter
    client_class = AppleOAuth2Client


class BaseUserRetrieveUpdateDestroyAPIView(RetrieveUpdateDestroyAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = BaseUserSerializer

    def get_object(self):
        return self.request.user

    def get(self, request, *args, **kwargs):
        location = request.query_params.get("location", None)

        instance = self.get_object()
        serializer = self.get_serializer(instance)
        data = serializer.data
        if location:
            restaurant = Location.objects.get(id=location).restaurant.id
            data['is_old_user'] = instance.is_old_user(restaurant=restaurant)
            data['order_stage'] = instance.order_count(restaurant=restaurant)
        return Response(data)


class BaseChangePasswordAPIView(UpdateAPIView):
    serializer_class = BaseChangePasswordSerializer
    permission_classes = [IsAuthenticated]

    def get_object(self):
        return self.request.user

    def patch(self, request, *args, **kwargs):
        self.partial_update(request, *args, **kwargs)
        return Response(UserSerializer(instance=self.get_object()).data)


class BaseRestaurantUserAPIView(APIView):
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def get(self, request, pk=None):
        if pk is None:
            return Response("Invalid request", status=status.HTTP_400_BAD_REQUEST)

        data = RestaurantUser.objects.filter(restaurant=pk)
        sr = BaseRestaurantUserGETSerializer(data, many=True)

        return Response(sr.data)

    def patch(self, request, pk=None):
        if pk is None or not RestaurantUser.objects.filter(id=pk).exists() or not "is_blocked" in request.data:
            return Response("Invalid request", status=status.HTTP_400_BAD_REQUEST)
        obj = RestaurantUser.objects.get(id=pk)
        is_blocked = request.data.get("is_blocked")
        obj.is_blocked = is_blocked
        obj.save()
        return Response(BaseRestaurantUserGETSerializer(obj).data)


class BaseRestaurantUserRetrieveAPIView(RetrieveAPIView):
    serializer_class = BaseRestaurantUserSerializer
    permission_classes = [IsAuthenticated]

    def get_object(self):
        restaurant = self.request.query_params.get("restaurant", None)
        print("j")
        if restaurant is None:
            raise ParseError("restaurant id is required")

        restaurant_user = RestaurantUser.objects.get_or_create(
            user=self.request.user, restaurant_id=restaurant
        )[0]
        return restaurant_user


class BaseUserAddressListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseUserAddressSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        is_default = self.request.query_params.get('is_default', None)
        if is_default:
            return UserAddress.objects.filter(user=self.request.user, is_default=True)
        return UserAddress.objects.filter(user=self.request.user)

    def perform_create(self, serializer):
        if serializer.validated_data.get('is_default'):
            UserAddress.objects.filter(
                user=self.request.user).update(is_default=False)
        serializer.save(user=self.request.user)


class BaseUserAddressRetrieveUpdateDestroyAPIView(
    GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView
):
    serializer_class = BaseUserAddressSerializer
    permission_classes = [IsAuthenticated, IsObjOwner]
    filterset_fields = ["id"]
    model_class = UserAddress

    def patch(self, request, *args, **kwargs):
        if "is_default" in request.data and request.data["is_default"] is True:
            UserAddress.objects.filter(
                user=request.user).update(is_default=False)
        return self.partial_update(request, *args, **kwargs)


class BaseContactModelAPIView(viewsets.ModelViewSet):
    serializer_class = BaseContactSerializer
    queryset = Contacts.objects.all()
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        user = self.request.user
        queryset = Contacts.objects.filter(
            Q(sender__user=user) | Q(receiver__user=user)
        )
        return queryset


# -----------sent user verify OTP ---------------
#   verify OTP send to phone number

class BaseSentUserVerifyOTP(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user
        phone = request.data.get("phone", None)

        if not phone:
            return Response(
                {"message": "phone number required!"},
                status=status.HTTP_400_BAD_REQUEST
            )

        user.phone = phone
        user.save()
        otp_obj = Otp.objects.create(
            user=user,
            otp=random.randint(1000, 9999),
            phone=phone
        )

        text = f"Your remote kitchen account verify code is {otp_obj.otp}"
        res = send_sms_bd(user.phone, text)

        return Response(res.json())

#   verify OTP send to email

class BaseSendUserVerifyEmail(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user

        if user.is_email_verified:
            return Response({"detail": "Email is already verified."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            send_email_verification_otp(user)

            return Response({"detail": "Verification OTP sent to your email."}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"detail": f"Failed to send email: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# verify phone number via OTP
class BaseUserVerifyOTP(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user
        otp = request.data.get("otp", None)
        if not otp:
            return Response(
                {"message": "otp required!"},
                status=status.HTTP_400_BAD_REQUEST
            )

        otp_obj = Otp.objects.filter(user=user.id, otp=otp).first()
        if otp_obj is None:
            return Response(
                {"message": "otp invalid!"},
                status=status.HTTP_400_BAD_REQUEST
            )

        if f"{otp_obj.otp}" != otp:
            return Response(
                {"message": "otp invalid!"},
                status=status.HTTP_400_BAD_REQUEST
            )

        user.is_phone_verified = True
        user.save()
        otp_obj.delete()

        return Response({"message": "verification complete!"})
    

class BaseVerifyEmailOTPView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        otp = request.data.get("otp")

        if not otp:
            return Response({"message": "OTP is required"}, status=400)

        otp_obj = Otp.objects.filter(
            user=request.user,
            otp=int(otp),
            email=request.user.email,
            is_used=False,
            expires_at__gte=timezone.now()
        ).order_by('-created_date').first()

        if not otp_obj:
            return Response({"message": "Invalid or expired OTP"}, status=400)

        if request.user.is_email_verified:
            return Response({"message": "User already verified"}, status=200)

        # Mark user as verified
        request.user.is_email_verified = True
        request.user.save()

        # Mark OTP as used
        otp_obj.is_used = True
        otp_obj.save()

        return Response({"message": "Email verified successfully"}, status=202)

      
      
class BaseSentUserVerifyOTPChatchef(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user
        phone = request.data.get("phone", None)

        if not phone:
            return Response(
                {"message": "phone number required!"},
                status=status.HTTP_400_BAD_REQUEST
            )

        user.phone = phone
        user.save()
        otp_obj = Otp.objects.create(
            user=user,
            otp=random.randint(1000, 9999),
            phone=phone
        )
      
        sms_response = send_sms_twilio(user.phone, f'Your OTP is {otp_obj.otp}')

        if sms_response["status"] == "success":
            return Response(
                {"data": "An OTP has been sent to your number!"}
            )
        else:
            return Response(
                {"message": f"Error sending SMS: {sms_response.get('error')}"}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        




class BaseUserRankingView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []

    def get(self, request):
        # Prefetch referred users and their orders
        referred_users_qs = User.objects.prefetch_related(
            Prefetch('order_set', queryset=Order.objects.all(), to_attr='prefetched_orders')
        )

        sales_users = (
            User.objects.filter(is_sales=True)
            .prefetch_related(
                Prefetch('referrals', queryset=referred_users_qs, to_attr='prefetched_referred_users')
            )
        )

        response_data = {
            "sales_users": InvitedUserSerializer(
                sales_users, many=True, context={"request": request}
            ).data
        }

        return Response(response_data)
    
    def delete(self, request):
        user_id = request.query_params.get('user_id')
        if not user_id:
            return Response({"detail": "user_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        user = get_object_or_404(User, id=user_id)

        if not user.referred_by:
            return Response({"detail": "This user is not in any referral list."}, status=status.HTTP_400_BAD_REQUEST)

        user.delete()
        return Response({"detail": f"User with id {user_id} deleted successfully."}, status=status.HTTP_204_NO_CONTENT)
    


# class BaseSalesUserRankingView(APIView):
#     permission_classes = [AllowAny]  # No login required
#     authentication_classes = []
#     def get(self, request):
#         # Get all users who are sales
#         sales_users = User.objects.filter(is_sales=True)

#         # Sort by number of referrals (most referrals first)
#         # sorted_sales_users = sorted(
#         #     sales_users,
#         #     key=lambda user: User.objects.filter(referred_by=user).count(),
#         #     reverse=True
#         # )

#         # Serialize and return the data
#         # response_data = {
#         #     "sales_ranking": SalesRankingSerializer(sorted_sales_users, many=True, context={"request": request}).data
#         # }

#         return Response("response_data")
    

class BaseSalesUserRankingView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []  # <-- VERY IMPORTANT for your case

    def get(self, request):
        # your logic
        return Response("response_data")
    

class BaseVerifyChatUserView(APIView):
    """
    API view to verify authenticated users from Chatchef Backend.
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Return user info if authenticated.
        """
        return Response({
            "id": request.user.id,
            "username": request.user.first_name + " " + request.user.last_name,
            "email": request.user.email,
        })


# password reset request

class BasePasswordResetRequestView(APIView):
    permission_classes = []
    authentication_classes = []

    def post(self, request):
        print("PasswordResetRequestView is using UPDATED CODE")
        identifier = request.data.get("identifier")
        via = request.data.get("via")  # 'email' or 'phone'

        if not identifier or via not in ['email', 'phone']:
            return Response({"detail": "identifier and valid via are required."}, status=400)

        try:
            if via == "email":
                user = User.objects.filter(email__iexact=identifier).first()
            else:
                user = User.objects.filter(phone=identifier).first()

            if not user:
                return Response({"detail": "User not found."}, status=404)

            otp_code = get_random_string(length=6, allowed_chars='0123456789')

            Otp.objects.create(
                user=user,
                phone=user.phone,
                email=user.email,
                otp=int(otp_code),
                is_used=False,
                expires_at=timezone.now() + timedelta(minutes=5)
            )

           
            if via == "email":
                send_password_reset_otp_email(user, otp_code)
            else:
                sms_message = f"Your HungryTiger OTP code is: {otp_code}. It will expire in 5 minutes."
                send_sms_bd(user.phone, sms_message)


            return Response({"detail": f"OTP sent via {via}."})

        except Exception as e:
            return Response({"detail": str(e)}, status=500)
        
# verify OTP
class BaseVerifyOTPView(APIView):
    permission_classes = []
    authentication_classes = []

    def post(self, request):
        otp = request.data.get("otp")

        if not otp:
            return Response({"detail": "OTP is required."}, status=400)

        try:
            otp_obj = Otp.objects.filter(otp=int(otp), is_used=False).latest('created_date')

            if otp_obj.expires_at < timezone.now():
                return Response({"detail": "OTP has expired."}, status=400)

            # Mark OTP as used (so no reuse)
            otp_obj.is_used = True
            otp_obj.save()

            # Return the OTP record ID to confirm password reset in step 2
            return Response({
                "detail": "OTP verified successfully.",
                "otp_id": otp_obj.id,
                "user_id": otp_obj.user.id
            })

        except Otp.DoesNotExist:
            return Response({"detail": "Invalid or expired OTP."}, status=400)
        except ValueError:
            return Response({"detail": "OTP must be a valid number."}, status=400)




# password reset confirm
class BasePasswordResetConfirmView(APIView):
    permission_classes = []
    authentication_classes = []

    def post(self, request):
        otp_id = request.data.get("otp_id")
        user_id = request.data.get("user_id")
        new_password = request.data.get("new_password", "").strip()

        if not otp_id or not new_password or not user_id:
            return Response({"detail": "otp_id, user_id, and new password are required."}, status=400)

        try:
            otp_obj = Otp.objects.get(id=otp_id, is_used=True)
            if otp_obj.user.id != int(user_id):
                return Response({"detail": "Invalid OTP session."}, status=403)

            user = otp_obj.user
            user.set_password(new_password)
            user.save()

            return Response({"detail": "Password reset successful."})

        except Otp.DoesNotExist:
            return Response({"detail": "Invalid or expired OTP session."}, status=400)
        except User.DoesNotExist:
            return Response({"detail": "User not found."}, status=404)


# login by phone otp


class PhoneOtpLoginAPIView(GenericAPIView):
    serializer_class = PhoneOtpLoginSerializer  # create this serializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = serializer.validated_data["user"]
        if not user.is_phone_verified:
            return Response({"detail": "Phone number not verified."}, status=400)

        token, _ = Token.objects.get_or_create(user=user)
        user_data = UserSerializer(user).data
        return Response({"token": token.key, "user_info": user_data})



class PhoneBlockCheckAPIView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        phone = request.data.get('phone', '')
        if not phone:
            return Response({'detail': 'Phone number is required'}, status=400)

        normalized_phone = phone
        # Add normalization here if needed (same as serializer)

        is_blocked = BlockedPhoneNumber.objects.filter(phone=normalized_phone).exists()
        return Response({'is_blocked': is_blocked})

       


class BasePlus88UserCountView(APIView):
    permission_classes = []  # Public endpoint

    def get(self, request, *args, **kwargs):
        User = get_user_model()
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')

        filters = Q(phone__startswith='+88')

        if start_date:
            filters &= Q(date_joined__date__gte=parse_date(start_date))
        if end_date:
            filters &= Q(date_joined__date__lte=parse_date(end_date))

        queryset = User.objects.filter(filters)

        total_count = queryset.count()
        sales_ref_count = queryset.filter(referred_by__isnull=False).count()
        auto_signup_count = queryset.filter(referred_by__isnull=True).count()

        # Unique and duplicate phone numbers (only consider phones starting with +88)
        phone_counts = queryset.values('phone').annotate(phone_count=Count('id'))
        unique_phone_count = sum(1 for p in phone_counts if p['phone_count'] == 1)
        duplicate_phone_count = sum(1 for p in phone_counts if p['phone_count'] > 1)

        #  Commission + Sales
        user_ids_in_range = queryset.values_list('id', flat=True)

        order_filters = Q(user_id__in=user_ids_in_range, status=Order.StatusChoices.COMPLETED)
        
        if start_date:
            order_filters &= Q(created_date__date__gte=parse_date(start_date))
        if end_date:
            order_filters &= Q(created_date__date__lte=parse_date(end_date))

        order_queryset = Order.objects.filter(order_filters)

        total_commission_charged = order_queryset.aggregate(total=Sum('commission_amount'))['total'] or 0
        total_gross_sales = order_queryset.aggregate(total=Sum('subtotal'))['total'] or 0

        # Lifetime Verified +88 Users
        total_accumulative_verified_users = User.objects.filter(
            phone__startswith='+88',
            is_phone_verified=True
        ).values('phone').distinct().count()

        return Response({
            'total_users_with_+88': total_count,
            'sales_ref_users': sales_ref_count,
            'auto_signup_users': auto_signup_count,
            'unique_phone_count': unique_phone_count,
            'duplicate_phone_count': duplicate_phone_count,
            'total_commission_charged': float(total_commission_charged),
            'total_gross_sales': float(total_gross_sales),
            'total_accumulative_verified_users': total_accumulative_verified_users,
            'start_date': start_date,
            'end_date': end_date,
            'prefix': '+88',
        })
    

class BaseMetricsOverviewView(APIView):
    """
    Return all analytics data.
    """

    def get(self, request):
        today = timezone.now().date()
        days = int(request.query_params.get("days", 7))
        date_range = [today - timedelta(days=i) for i in range(days)]
        event_names = request.query_params.getlist("event_name") or ["app_open", "weekly_active", "monthly_active"]

        total_users = User.objects.filter(phone__startswith="+88").count()
        total_orders = Order.objects.filter(restaurant__is_remote_Kitchen=True).count()

        dau_records = []
        for event_name in event_names:
            counts = []
            for date_obj in date_range:
                if event_name in ["weekly_active", "monthly_active"]:
                    record = DAURecord.objects.filter(
                        event_name=event_name,
                        date=date_obj
                    ).values("count").first()
                    count = record["count"] if record else 0
                else:
                    count = (
                        UserEvent.objects.filter(
                            event_name=event_name,
                            event_time__date=date_obj
                        ).values("user").distinct().count()
                    )
                counts.append({"date": str(date_obj), "count": count})

            label_map = {
                "app_open": "DAU",
                "weekly_active": "WAU",
                "monthly_active": "MAU"
            }

            dau_records.append({
                "event_name": label_map.get(event_name, event_name),
                "daily_counts": counts
            })

        funnels = []
        for rec in ConversionFunnelRecord.objects.filter(date__in=date_range):
            funnels.append({
                "date": str(rec.date),
                "opened_app": rec.opened_app,
                "placed_order": rec.placed_order,
                "conversion_rate": rec.conversion_rate
            })

        cohorts = []
        cohort_labels = CohortRetentionRecord.objects.values_list("cohort_label", flat=True).distinct()
        for label in cohort_labels:
            records = CohortRetentionRecord.objects.filter(cohort_label=label)
            cohorts.append({
                "cohort": label,
                "day_1": sum(r.retained_users for r in records if r.day_offset == 1),
                "day_7": sum(r.retained_users for r in records if r.day_offset == 7),
                "day_30": sum(r.retained_users for r in records if r.day_offset == 30)
            })

        segments = defaultdict(list)
        for seg in UserEngagementSegment.objects.all():
            segments[seg.segment].append({"id": seg.user.id, "email": seg.user.email})

        churns = []
        for c in UserChurnStatus.objects.all():
            churns.append({
                "id": c.user.id,
                "email": c.user.email,
                "last_activity_date": str(c.last_activity_date),
                "status": c.status
            })

        return Response({
            "total_users": total_users,
            "total_orders": total_orders,
            "dau": dau_records,
            "conversion_funnel": funnels,
            "cohort_retention": cohorts,
            "frequency_segments": segments,
            "churn_statuses": churns
        })
    
from accounts.analytic.utils import update_metrics

class BaseUpdateMetricsView(APIView):
    def post(self, request):
        update_metrics()
        return Response({"message": "Metrics updated"})

class BaseSessionTrackingView(APIView):
    def post(self, request):
        action = request.data.get("action")  # "start" or "end"
        user = request.user
        now = timezone.now()

        if action == "start":
            session = UserSession.objects.create(user=user, session_start=now)
            return Response({"session_id": session.id})

        elif action == "end":
            session_id = request.data.get("session_id")
            try:
                session = UserSession.objects.get(id=session_id, user=user)
                session.session_end = now
                session.save()
                return Response({"duration": session.duration})
            except UserSession.DoesNotExist:
                return Response({"error": "Invalid session"}, status=400)

        return Response({"error": "Invalid action"}, status=400)


class BaseFeedbackPromptView(APIView):
    """
    Called from frontend after order or on home screen to submit feedback.
    """
    def post(self, request):
        serializer = BaseFeedbackPromptSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"message": "Feedback recorded."}, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class BaseLogUserEventView(APIView):
     """
    Frontend calls this to log a user event.
    """
     def post(self, request):
        user_id = request.data.get("user_id")
        event_name = request.data.get("event_name")
        metadata = request.data.get("metadata", {})
        platform = request.data.get("platform", None)

        if not event_name:
            return Response({"error": "event_name is required."}, status=status.HTTP_400_BAD_REQUEST)

        user = None
        if user_id:
            try:
                user = User.objects.get(id=user_id)
            except User.DoesNotExist:
                return Response({"error": "Invalid user_id."}, status=status.HTTP_400_BAD_REQUEST)

        UserEvent.objects.create(
            user=user,
            event_name=event_name,
            metadata=metadata,
            platform=platform
        )

        return Response({"status": "success"}, status=status.HTTP_201_CREATED)

    


class BaseScanQrAPIView(APIView):
    def get(self, request):
        device_id = request.GET.get("device_id")
        ref = request.GET.get("ref")  
        ip = get_client_ip(request)
        user_agent = request.META.get("HTTP_USER_AGENT", "")
        timestamp = get_bdt_time()

        if not device_id:
            return JsonResponse({"error": "Missing device_id"}, status=400)

        if not QRScan.objects.filter(device_id=device_id).exists():
            QRScan.objects.create(
                device_id=device_id,
                ip_address=ip,
                user_agent=user_agent,
                ref=ref,
                timestamp=timestamp
            )
            print(f"✅ New scan from {device_id}")
        else:
            print(f"⚠️ Duplicate scan from {device_id} ignoreds")

        # return redirect("https://play.google.com/store/apps/details?id=com.chatchefs.mealmingle")
        return Response("success")
    


class BaseQRScanAnalyticsView(APIView):
    def get(self, request):
        range_type = request.GET.get("range")  # If not provided, show all
        start = request.GET.get("start")
        end = request.GET.get("end")
        ref = request.GET.get("ref")
        sort_by = request.GET.get("sort_by", "timestamp")
        order = request.GET.get("order", "desc")
        export = request.GET.get("export") == "csv"

        bdt = pytz.timezone("Asia/Dhaka")
        now_bdt = now().astimezone(bdt)

        scans = QRScan.objects.all()

        # Only apply date filter if range_type is provided
        if range_type:
            if range_type == "today":
                start_date = now_bdt.date()
                end_date = start_date
            elif range_type == "yesterday":
                start_date = now_bdt.date() - timedelta(days=1)
                end_date = start_date
            elif range_type == "last7days":
                end_date = now_bdt.date()
                start_date = end_date - timedelta(days=6)
            elif range_type == "custom" and start and end:
                start_date = datetime.strptime(start, "%Y-%m-%d").date()
                end_date = datetime.strptime(end, "%Y-%m-%d").date()
            else:
                return Response({"error": "Invalid or missing date range"}, status=400)

            # Convert dates to datetimes for safer filtering
            start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=bdt)
            end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=bdt)

            scans = scans.filter(timestamp__range=(start_datetime, end_datetime))

        # Ref filter if provided
        if ref:
            scans = scans.filter(ref=ref)

        # Sort by selected field
        if sort_by not in ['timestamp', 'device_id', 'ip_address']:
            sort_by = 'timestamp'
        scans = scans.order_by(f"-{sort_by}" if order == "desc" else sort_by)

        # CSV export
        if export:
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="qr_scan_data.csv"'
            writer = csv.writer(response)
            writer.writerow(['Device ID', 'IP Address', 'Ref', 'User Agent', 'Timestamp (BDT)'])
            for scan in scans:
                writer.writerow([
                    scan.device_id,
                    scan.ip_address,
                    scan.ref or '',
                    scan.user_agent or '',
                    scan.timestamp.astimezone(bdt).strftime("%Y-%m-%d %I:%M:%S %p"),
                ])
            return response

        # Daily summary
        daily_stats = (
            scans.annotate(day=TruncDate('timestamp'))
            .values('day')
            .annotate(count=Count('id'))
            .order_by('day')
        )

        # Top IPs and Devices
        top_ips = scans.values('ip_address').annotate(count=Count('id')).order_by('-count')[:5]
        top_devices = scans.values('device_id').annotate(count=Count('id')).order_by('-count')[:5]

        # Detailed logs
        detailed_logs = []
        for scan in scans:
            detailed_logs.append({
                "device_id": scan.device_id,
                "ip_address": scan.ip_address,
                "ref": scan.ref,
                "user_agent": scan.user_agent,
                "timestamp_bdt": scan.timestamp.astimezone(bdt).strftime("%Y-%m-%d %I:%M:%S %p"),
            })

        return Response({
            "total_scans": scans.count(),
            "total_unique_devices": scans.values("device_id").distinct().count(),
            "start_date": str(start_date) if range_type else None,
            "end_date": str(end_date) if range_type else None,
            "daily_counts": list(daily_stats),
            "top_ips": list(top_ips),
            "top_devices": list(top_devices),
            "detailed_logs": detailed_logs,
        })




class BaseSubscriptionCreateView(APIView):
    def post(self, request):
        serializer = BaseSubscriptionCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        STRIPE_KEY ="sk_test_51RgkRbQExPhyhAvqjoDbdtZjoE9veSmPeP31hZ6YkfczkwYazMqKzzzedVzHqCuHU9FNEcX62xuHeXaPqNbpOSqP00xAYkcdbW"

        # Create Stripe Customer
        customer = stripe.Customer.create(
            email=data["email"],
            name=data["name"],
            payment_method=data["payment_method_id"],
            invoice_settings={
                "default_payment_method": data["payment_method_id"],
            },
            api_key=STRIPE_KEY
        )

        Customer.objects.get_or_create(
            email=data["email"],
            defaults={
                "name": data["name"],
                "stripe_customer_id": customer.id
            }
        )


        # Create Subscription with dynamic plan_id as the price
        subscription = stripe.Subscription.create(
            customer=customer.id,
            items=[
                {"price": data["plan_id"]}
            ],
            trial_period_days=30,  # 👈 1 month free trial
            expand=["latest_invoice.payment_intent"],
            api_key=STRIPE_KEY
        )

        payment_intent = getattr(subscription.latest_invoice, "payment_intent", None)
        client_secret = payment_intent.client_secret if payment_intent else None

        return Response({
            "success": True,
            "data": {
                "subscription_id": subscription.id,
                "client_secret": client_secret,  
                "trial_end": datetime.fromtimestamp(subscription.trial_end).isoformat() if subscription.trial_end else None,
                "next_billing_date": datetime.fromtimestamp(subscription.current_period_end).isoformat() if subscription.current_period_end else None,
                "amount_due_next": 38
            }
        })

    
    
class BaseSubscriptionVerifyView(APIView):
    def post(self, request):
        serializer = BaseSubscriptionVerifySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        email = serializer.validated_data["email"]

        try:
            customer = Customer.objects.get(email=email)
            subscription = Subscription.objects.get(customer=customer)
        except (Customer.DoesNotExist, Subscription.DoesNotExist):
            return Response({
                "success": False,
                "error": "Subscription not found."
            }, status=404)

        current_period_start = subscription.current_period_start.isoformat() if subscription.current_period_start else None
        current_period_end = subscription.current_period_end.isoformat() if subscription.current_period_end else None

        response = {
            "success": True,
            "data": {
                "subscription": {
                    "id": f"sub_{subscription.id}",
                    "status": subscription.status,
                    "customer": {
                        "id": f"cus_{customer.id}",
                        "email": customer.email,
                        "name": customer.name
                    },
                    "plan": {
                        "id": subscription.plan_id,
                        "name": "Chatchef Subscription",
                        "price": 35,
                        "originalPrice": 99,
                        "interval": "month"
                    },
                    "current_period_start": current_period_start,
                    "current_period_end": current_period_end,
                    "created": current_period_start,
                }
            }
        }
        return Response(response)


class BaseSubscriptionCancelView(APIView):
    def post(self, request):
        serializer = BaseCancellationRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            customer = Customer.objects.get(email=data["email"])
            subscription = Subscription.objects.get(
                customer=customer,
                id=data["subscription_id"].replace("sub_", "")
            )
        except (Customer.DoesNotExist, Subscription.DoesNotExist):
            return Response({"success": False, "message": "Subscription not found."}, status=404)

        cancel_request = CancellationRequest.objects.create(
            subscription=subscription,
            reason=data["reason"],
            details=data["details"]
        )

        response = {
            "success": True,
            "request": {
                "id": f"req_{cancel_request.id}",
                "status": cancel_request.status,
                "created": cancel_request.created.isoformat(),
                "message": "Cancellation request submitted. Support will review."
            }
        }
        return Response(response)


class BaseWebhookHandlerView(APIView):
    def post(self, request):
        sig_header = request.META.get("HTTP_STRIPE_SIGNATURE")
        payload = request.body
        endpoint_secret = "whsec_JT3ni5j9RTav5edqOeLrxy02cG4GjMUS"

        try:
            event = stripe.Webhook.construct_event(
                payload=payload,
                sig_header=sig_header,
                secret=endpoint_secret
            )
        except ValueError:
            return Response({"error": "Invalid payload"}, status=status.HTTP_400_BAD_REQUEST)
        except stripe.error.SignatureVerificationError:
            return Response({"error": "Invalid signature"}, status=status.HTTP_400_BAD_REQUEST)

        event_type = event.get("type")
        data = event.get("data", {}).get("object", {})

        print(f"[Webhook] Received event: {event_type}")

        try:
            # 🎯 Handle subscription creation
            if event_type == "customer.subscription.created":
                stripe_customer_id = data.get("customer")
                stripe_subscription_id = data.get("id")
                items = data.get("items", {}).get("data", [])
                plan_id = None

                if items:
                    price_info = items[0].get("price")
                    if price_info and isinstance(price_info, dict):
                        plan_id = price_info.get("id")

                customer = Customer.objects.filter(stripe_customer_id=stripe_customer_id).first()
                if not customer:
                    print(f"[Webhook] Customer not found: {stripe_customer_id}")
                    # Don't block the webhook with 404, just return success
                    return Response({"message": "Customer not found"}, status=status.HTTP_200_OK)

                Subscription.objects.create(
                    customer=customer,
                    plan_id=plan_id,
                    stripe_subscription_id=stripe_subscription_id,
                    status=data.get("status", "inactive"),
                    current_period_start=datetime.fromtimestamp(data["current_period_start"]) if data.get("current_period_start") else None,
                    current_period_end=datetime.fromtimestamp(data["current_period_end"]) if data.get("current_period_end") else None,
                )
                print(f"[Webhook] Subscription created for customer {stripe_customer_id}")

            # 🎯 Handle payment success
            elif event_type == "invoice.payment_succeeded":
                stripe_subscription_id = data.get("subscription")
                if stripe_subscription_id:
                    Subscription.objects.filter(stripe_subscription_id=stripe_subscription_id).update(status="active")
                    print(f"[Webhook] Payment succeeded → Subscription {stripe_subscription_id} set to active")

            # 🎯 Handle payment failure
            elif event_type == "invoice.payment_failed":
                stripe_subscription_id = data.get("subscription")
                if stripe_subscription_id:
                    Subscription.objects.filter(stripe_subscription_id=stripe_subscription_id).update(status="inactive")
                    print(f"[Webhook] Payment failed → Subscription {stripe_subscription_id} set to inactive")

            # 🎯 Handle subscription cancellation
            elif event_type == "customer.subscription.deleted":
                stripe_subscription_id = data.get("id")
                if stripe_subscription_id:
                    Subscription.objects.filter(stripe_subscription_id=stripe_subscription_id).update(status="canceled")
                    print(f"[Webhook] Subscription {stripe_subscription_id} canceled")

        except Exception as e:
            print(f"[Webhook ERROR] {event_type}: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"success": True, "message": "Webhook processed"}, status=status.HTTP_200_OK)
class BaseSubscriptionListView(APIView):
    def get(self, request):
        subscriptions = Subscription.objects.select_related("customer").order_by("-current_period_start")
        data = []

        for sub in subscriptions:
            current_period_start = sub.current_period_start.isoformat() if sub.current_period_start else None
            current_period_end = sub.current_period_end.isoformat() if sub.current_period_end else None

            data.append({
                "id": f"sub_{sub.id}",
                "status": sub.status,
                "customer": {
                    "id": f"cus_{sub.customer.id}",
                    "email": sub.customer.email,
                    "name": sub.customer.name
                },
                "plan": {
                    "id": sub.plan_id,
                    "name": "Chatchef Subscription",
                    "price": 35,
                    "originalPrice": 99,
                    "interval": "month"
                },
                "current_period_start": current_period_start,
                "current_period_end": current_period_end,
                "created": current_period_start,
            })

        return Response({
            "success": True,
            "data": data
        })




class BaseCancellationRequestListView(APIView):
    def get(self, request):
        cancellations = CancellationRequest.objects.select_related("subscription__customer").order_by("-created")
        data = []

        for cancel in cancellations:
            data.append({
                "id": cancel.id,
                "subscription_id": f"sub_{cancel.subscription.id}",
                "customer_email": cancel.subscription.customer.email,
                "reason": cancel.reason,
                "details": cancel.details,
                "status": cancel.status,
                "created": cancel.created.isoformat()
            })

        return Response({
            "success": True,
            "data": data
        })





from firebase.models import TokenFCM

class BaseConversionRateAPIView(APIView):
    """
    Returns all Conversion Rate metrics for HungryTiger app.
    """

    def get(self, request):
        # Onboarding CVR
        installs = TokenFCM.objects.values("token").distinct().count()
        onboarded = TokenFCM.objects.filter(user__isnull=False).values("user").distinct().count()
        onboarding_cvr = (onboarded / installs * 100) if installs > 0 else 0

        # Referral CVR
        referral_shared = UserEvent.objects.filter(event_name="referral_shared").count()
        total_referral_signup = UserEvent.objects.filter(event_name="referral_signup").count()
        referral_cvr = (total_referral_signup / referral_shared * 100) if referral_shared > 0 else 0

        # Order CVR
        total_registered_users = User.objects.count()

        first_order_users = (
            Order.objects
            .filter(status="completed",restaurant__is_remote_Kitchen=True)
            .values("user")
            .distinct()
            .count()
        )
        order_cvr = (
            (first_order_users / total_registered_users * 100)
            if total_registered_users > 0 else 0
        )

        #  → First Order CVR
        signup_to_first_order_cvr = (
            (first_order_users / onboarded * 100)
            if onboarded > 0 else 0
        )

        # Repeat Order CVR
        repeat_order_users = (
            Order.objects
            .filter(status="completed",restaurant__is_remote_Kitchen=True)
            .values("user")
            .annotate(order_count=Count("id"))
            .filter(order_count__gt=1)
            .count()
        )
        repeat_order_cvr = (
            (repeat_order_users / first_order_users * 100)
            if first_order_users > 0 else 0
        )

        # Push Notification CVR
        push_sent = UserEvent.objects.filter(event_name="push_sent").count()
        push_clicked = UserEvent.objects.filter(event_name="push_clicked").count()
        push_cvr = (push_clicked / push_sent * 100) if push_sent > 0 else 0

        # Spin-to-Install CVR
        spin_invite_sent = UserEvent.objects.filter(event_name="spin_invite_sent").count()
        spin_clicked = UserEvent.objects.filter(event_name="spin_invite_clicked").count()
        spin_installed = UserEvent.objects.filter(event_name="spin_install").count()
        spin_cvr = (spin_installed / spin_clicked * 100) if spin_clicked > 0 else 0

        # Merchant Deal CVR
        deal_clicked = UserEvent.objects.filter(event_name="deal_clicked").count()
        total_deal_ordered = (
            Order.objects
            .filter(local_deal__isnull=False)
            .distinct()
            .count()
        )
        deal_cvr = (total_deal_ordered / deal_clicked * 100) if deal_clicked > 0 else 0

        # Top Referrers
        top_referrers = (
            Referral.objects.annotate(
                referral_count=Count("joined_users")
            )
            .order_by("-referral_count")[:5]
        )
        top_referrers_data = [
            {
                "user_id": r.user.id,
                "user_name": f"{r.user.first_name} {r.user.last_name}",
                "referral_count": r.referral_count
            }
            for r in top_referrers
        ]

        #  Unique User Counts
        unique_push_sent_users = UserEvent.objects.filter(event_name="push_sent").values("user").distinct().count()
        unique_push_clicked_users = UserEvent.objects.filter(event_name="push_clicked").values("user").distinct().count()
        unique_spin_clicked_users = UserEvent.objects.filter(event_name="spin_invite_clicked").values("user").distinct().count()
        unique_spin_installed_users = UserEvent.objects.filter(event_name="spin_install").values("user").distinct().count()
        unique_deal_clicked_users = UserEvent.objects.filter(event_name="deal_clicked").values("user").distinct().count()
        unique_deal_ordered_users = (
            Order.objects
            .filter(local_deal__isnull=False)
            .values("user")
            .distinct()
            .count()
        )

        return Response({
            #  Onboarding
            "onboarding_cvr": round(onboarding_cvr, 2),
            "total_installs": installs,
            "total_onboarded": onboarded,

            #  Referral
            "referral_cvr": round(referral_cvr, 2),
            "total_referral_shared": referral_shared,
            "total_referral_signup": total_referral_signup,
            "top_referrers": top_referrers_data,

            #  Order
            "order_cvr": round(order_cvr, 2),
            "signup_to_first_order_cvr": round(signup_to_first_order_cvr, 2),
            "repeat_order_cvr": round(repeat_order_cvr, 2),
            "total_registered_users": total_registered_users,
            "total_first_order_users": first_order_users,
            "total_repeat_order_users": repeat_order_users,

            #  Push Notifications
            "push_cvr": round(push_cvr, 2),
            "total_push_sent": push_sent,
            "total_push_clicked": push_clicked,
            "unique_push_sent_users": unique_push_sent_users,
            "unique_push_clicked_users": unique_push_clicked_users,

            #  Spin
            "spin_cvr": round(spin_cvr, 2),
            "total_spin_sent": spin_invite_sent,
            "total_spin_clicked": spin_clicked,
            "total_spin_installed": spin_installed,
            "unique_spin_clicked_users": unique_spin_clicked_users,
            "unique_spin_installed_users": unique_spin_installed_users,

            #  Deals
            "deal_cvr": round(deal_cvr, 2),
            "total_deal_clicked": deal_clicked,
            "total_deal_ordered": total_deal_ordered,
            "unique_deal_clicked_users": unique_deal_clicked_users,
            "unique_deal_ordered_users": unique_deal_ordered_users,
        })