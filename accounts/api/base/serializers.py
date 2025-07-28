from allauth.socialaccount.models import SocialAccount
from allauth.socialaccount.providers.base import AuthProcess
from dj_rest_auth.registration.serializers import \
    SocialLoginSerializer as BaseSocialLoginSerializer
from django.contrib.auth import authenticate
from django.contrib.auth.password_validation import validate_password
from django.db.models import Sum
from django.utils.translation import gettext_lazy as _
from rest_framework import serializers
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import ValidationError

from accounts.models import (Company, Contacts, RestaurantUser, User,
                             UserAddress,BlockedPhoneNumber)
from accounts.utils import send_verify_email
from billing.models import Order
from referral.models import InviteCodes, Referral
import uuid
import logging
from accounts.models import Customer, Subscription, CancellationRequest,FeedbackPrompt


logger = logging.getLogger(__name__)


class SocialLoginSerializer(BaseSocialLoginSerializer):
    def get_social_login(self, *args, **kwargs):
        """
        Set the social login process state to connect rather than login
        Refer to the implementation of get_social_login in base class and to the
        allauth.socialaccount.helpers module complete_social_login function.
        """
        social_login = super().get_social_login(*args, **kwargs)
        social_login.state["process"] = AuthProcess.CONNECT
        return social_login


class BaseUserSerializer(serializers.ModelSerializer):
    company_name = serializers.CharField(
        max_length=255, write_only=True, required=False
    )
    code = serializers.CharField(
        max_length=25, write_only=True, required=False
    )

    refer_code = serializers.CharField(
        max_length=25, write_only=True, required=False
    )
    password = serializers.CharField(
        write_only=True, required=True, validators=[validate_password]
    )
    order_stage = serializers.SerializerMethodField()


    hotel_count = serializers.SerializerMethodField()



    # token = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id",
            "first_name",
            "last_name",
            "email",
            "password",
            "phone",
            "company_name",
            "reward_points",
            "code",
            "refer_code",
            "date_of_birth",
            "is_phone_verified",
            "agree",
            "is_sales",
            "order_stage",
            "referred_by",
            "super_power",
            "is_get_600",
            "hotel_admin",
            "hotel_count",
        ]
        extra_kwargs = {
            "reward_points": {
                "read_only": True,
            }
        }
    
    def get_hotel_count(self, obj):
        return obj.hotel_count
    
    def get_order_stage(self, obj: User):
        # Access restaurant from request context
        request = self.context.get("request")
        restaurant_id = request.data.get("restaurant") if request else None
        if restaurant_id:
            return obj.order_count(restaurant=restaurant_id)
        return None 

    def get_name(self, obj: User):
        return f"{obj.first_name} {obj.last_name}"

    # def get_token(self, obj: User):
    #     return Token.objects.get(user=obj).key

    def to_representation(self, instance: User):
        representation = super(
            BaseUserSerializer, self).to_representation(instance)
        company = instance.company
        if company is not None:
            representation["company_name"] = company.name
        return representation

    

    def validate(self, attrs):
        request = self.context.get("request")

        allowed_existing_phones = ["01670338890", "01789141408", "01771700545", "01980796731"]

        def normalize_phone(phone):
            if phone.startswith("+880"):
                phone = "0" + phone[4:]
            elif phone.startswith("880"):
                phone = "0" + phone[3:]
            elif phone.startswith("1") and len(phone) == 10:
                phone = "0" + phone
            return phone

        if request and request.method == "POST":
            phone = attrs.get("phone")
            if phone:
                normalized_phone = normalize_phone(phone)

                # Add this phone block check here:
                if BlockedPhoneNumber.objects.filter(phone=normalized_phone).exists():
                    raise serializers.ValidationError({
                        "phone": "This phone number is blocked and cannot be used for registration."
                    })
                
                if (
                    normalized_phone not in allowed_existing_phones and
                    User.objects.filter(phone=normalized_phone).exists()
                ):
                    raise serializers.ValidationError({
                        "phone": "This phone number is already registered. Please login or recover your account."
                    })
                attrs["phone"] = normalized_phone  # Optional: store the normalized version

        if request and request.method == "PATCH" and "password" in attrs:
            attrs.pop("password")

        return attrs



    def create(self, validated_data):
        password = validated_data.pop("password")
    
        invite_obj = None

        
                
        if "refer_code" in validated_data:
            invite_code = validated_data.pop("refer_code")

            if not InviteCodes.objects.filter(code=invite_code).exists():
                raise ValidationError(
                    {'invite_code': _('This invite code invalid!')}
                )

            invite_obj = InviteCodes.objects.get(code=invite_code)

        if "company_name" in validated_data:
            company_name = validated_data.pop("company_name")
            company = Company.objects.create(name=company_name)
            role = User.RoleType.OWNER
            validated_data["company"] = company
            validated_data["role"] = role

        if "code" in validated_data:
            company_code = validated_data.pop("code")
            if not Company.objects.filter(register_code=company_code).exists():
                raise ValidationError(
                    {'register_code': _('This register code invalid!')}
                )

            company = Company.objects.get(register_code=company_code)
            role = User.RoleType.EMPLOYEE
            validated_data["company"] = company
            validated_data["role"] = role
        

        user = User.objects.create(**validated_data)
        user.set_password(password)
        user.save(update_fields=["password"])
        
        # for sending verification mail to user
        if not user.uid:
            user.uid = uuid.uuid4()
            user.save(update_fields=["uid"])


        Token.objects.create(user=user)



        if invite_obj:
            invite_obj.status = InviteCodes.STATUS.ACCEPTED

            refer_obj = invite_obj.refer
            refer_obj.joined_users.add(user)
            if not refer_obj.invited_users.filter(id=user.id).exists():
                refer_obj.invited_users.add(user)
            invite_obj.save()

        # send_verify_email(user)

        return user
    



class InvitedUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = "__all__"  # This will include all fields from the User model

class OrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = "__all__"  # Include all fields of Order


class InvitedUserSerializer(serializers.ModelSerializer):
    orders = serializers.SerializerMethodField()  # Get orders of this user

    class Meta:
        model = User
        fields = "__all__"  # Include all user fields + orders

    def get_orders(self, obj):
        """Get all orders placed by this referred user."""
        orders = Order.objects.filter(user=obj)
        return OrderSerializer(orders, many=True).data  # Serialize the full order objects


class BaseSalesRankingSerializer(serializers.ModelSerializer):
    referral_count = serializers.SerializerMethodField()
    referred_users = serializers.SerializerMethodField()
    orders_by_referred_users = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = "__all__"  # You said you want full A to Z information

    def get_referral_count(self, obj):
        """Number of users referred by this sales user"""
        return User.objects.filter(referred_by=obj).count()

    def get_referred_users(self, obj):
        """List of users referred by this sales user"""
        referred_users = User.objects.filter(referred_by=obj)
        return InvitedUserSerializer(referred_users, many=True, context=self.context).data

    def get_orders_by_referred_users(self, obj):
        """All orders placed by referred users"""
        referred_users = User.objects.filter(referred_by=obj)
        orders = Order.objects.filter(user__in=referred_users)
        return OrderSerializer(orders, many=True, context=self.context).data



class BaseOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = "__all__"  # Full order info


class BaseInvitedUserSerializer(serializers.ModelSerializer):
    order_count = serializers.SerializerMethodField()
    orders = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = "__all__"

    def get_order_count(self, obj):
        return len(getattr(obj, 'prefetched_orders', []))

    def get_orders(self, obj):
        orders = getattr(obj, 'prefetched_orders', [])
        return BaseOrderSerializer(orders, many=True, context=self.context).data



class BaseSalesUserWithInvitedSerializer(serializers.ModelSerializer):
    referred_users = serializers.SerializerMethodField()
    unique_phone_count = serializers.SerializerMethodField()
    duplicate_phone_count = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = "__all__"

    def get_referred_users(self, obj):
        referred_users = getattr(obj, 'prefetched_referred_users', [])
        return BaseInvitedUserSerializer(referred_users, many=True, context=self.context).data

    def get_unique_phone_count(self, obj):
        referred_users = getattr(obj, 'prefetched_referred_users', [])
        phones = [user.phone for user in referred_users if user.phone]
        counts = {}
        for phone in phones:
            counts[phone] = counts.get(phone, 0) + 1
        return sum(1 for c in counts.values() if c == 1)

    def get_duplicate_phone_count(self, obj):
        referred_users = getattr(obj, 'prefetched_referred_users', [])
        phones = [user.phone for user in referred_users if user.phone]
        counts = {}
        for phone in phones:
            counts[phone] = counts.get(phone, 0) + 1
        return sum(1 for c in counts.values() if c > 1)



class BaseEmailPasswordLoginSerializer(serializers.Serializer):
    email = serializers.CharField(label=_("Email"), write_only=True)
    password = serializers.CharField(
        label=_("Password"),
        style={"input_type": "password"},
        trim_whitespace=False,
        write_only=True,
    )
    token = serializers.CharField(label=_("Token"), read_only=True)

    def validate(self, attrs):
        email = attrs.get("email")
        password = attrs.get("password")

        if email and password:
            user = authenticate(
                request=self.context.get("request"), email=email, password=password
            )

            # The authenticate call simply returns None for is_active=False
            # users. (Assuming the default ModelBackend authentication
            # backend.)
            if not user:
                msg = _("Unable to log in with provided credentials.")
                raise serializers.ValidationError(msg, code="authorization")
            # if not user.is_email_verified:
            #     msg = _(
            #         'Error: Please verify your email address to proceed with the login.')
            #     raise serializers.ValidationError(msg, code='authorization')
        else:
            msg = _('Must include "username" and "password".')
            raise serializers.ValidationError(msg, code="authorization")

        attrs["user"] = user
        return attrs

    def create(self, validated_data):
        pass

    def update(self, instance, validated_data):
        pass


class BaseChangePasswordSerializer(serializers.ModelSerializer):
    password = serializers.CharField(
        write_only=True, required=True, validators=[validate_password]
    )
    old_password = serializers.CharField(write_only=True, required=False)

    class Meta:
        model = User
        fields = ("old_password", "password")

    def validate_old_password(self, value):
        user = self.context["request"].user
        if SocialAccount.objects.filter(user=user).exists():
            return value
        if not user.check_password(value):
            raise serializers.ValidationError(
                {"old_password": "Old password is not correct"}
            )
        return value

    def update(self, instance, validated_data):
        instance.set_password(validated_data["password"])
        instance.save()

        return instance


class BaseRestaurantUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = RestaurantUser
        fields = "__all__"


class BaseRestaurantUserGETSerializer(serializers.ModelSerializer):
    user = BaseUserSerializer()

    class Meta:
        model = RestaurantUser
        fields = "__all__"


class RestaurantUserGETSerializer(BaseRestaurantUserGETSerializer):
    total_ordered = serializers.SerializerMethodField()
    ordered_value = serializers.SerializerMethodField()
    last_time_order = serializers.SerializerMethodField()

    def get_total_ordered(self, obj: RestaurantUser):
        return Order.objects.filter(user=obj.user.id).count()

    def get_ordered_value(self, obj: RestaurantUser):
        return Order.objects.filter(user=obj.user.id).aggregate(Sum('total'))['total__sum'] or 0

    def get_last_time_order(self, obj: RestaurantUser):
        return Order.objects.filter(user=obj.user.id).order_by('id').last().receive_date if Order.objects.filter(user=obj.user.id) else None


class BaseUserAddressSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserAddress
        fields = "__all__"
        extra_kwargs = {"user": {"read_only": True}}


class BaseContactSerializer(serializers.ModelSerializer):
    class Meta:
        model = Contacts
        fields = "__all__"



from accounts.models import User, Otp

class PhoneOtpLoginSerializer(serializers.Serializer):
    phone = serializers.CharField(write_only=True)
    otp = serializers.CharField(write_only=True)
    token = serializers.CharField(read_only=True)
    user = serializers.PrimaryKeyRelatedField(read_only=True)

    def validate(self, attrs):
        phone = attrs.get('phone')
        otp = attrs.get('otp')

        if not phone or not otp:
            raise serializers.ValidationError("Phone and OTP are required.")

        # Normalize phone (same logic as in your BaseUserSerializer)
        if phone.startswith("+880"):
            phone = "0" + phone[4:]
        elif phone.startswith("880"):
            phone = "0" + phone[3:]
        elif phone.startswith("1") and len(phone) == 10:
            phone = "0" + phone

        # Check if user with this phone exists
        try:
            user = User.objects.get(phone=phone)
        except User.DoesNotExist:
            raise serializers.ValidationError("User with this phone does not exist.")

        # Check if OTP matches and is not used
        otp_obj = Otp.objects.filter(user=user, phone=phone, otp=otp, is_used=False).last()
        if not otp_obj:
            raise serializers.ValidationError("Invalid or expired OTP.")

        # Mark OTP as used
        otp_obj.is_used = True
        otp_obj.save(update_fields=["is_used"])

        if not user.is_phone_verified:
            raise serializers.ValidationError("Phone number not verified yet.")

        attrs['user'] = user
        return attrs




# Hotel owner user serializer

class HotelOwnerUserSerializer(BaseUserSerializer):
    class Meta(BaseUserSerializer.Meta):
        fields = [
            "id",
            "first_name",
            "last_name",
            "email",
            "password",
            "phone",
            "hotel_admin",
            "hotel_count",
        ]
class BaseSubscriptionCreateSerializer(serializers.Serializer):
    email = serializers.EmailField()
    name = serializers.CharField()
    plan_id = serializers.CharField()
    payment_method_id = serializers.CharField()


class BaseSubscriptionResponseSerializer(serializers.ModelSerializer):
    customer_id = serializers.SerializerMethodField()
    plan = serializers.SerializerMethodField()

    class Meta:
        model = Subscription
        fields = [
            "id",
            "customer_id",
            "status",
            "current_period_start",
            "current_period_end",
            "plan",
        ]

    def get_customer_id(self, obj):
        return f"cus_{obj.customer.id}"

    def get_plan(self, obj):
        return {
            "id": obj.plan_id,
            "name": "Chatchef Subscription",
            "price": 35,
            "interval": "month",
        }


class BaseSubscriptionVerifySerializer(serializers.Serializer):
    email = serializers.EmailField()


class BaseCancellationRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()
    subscription_id = serializers.CharField()
    reason = serializers.CharField()
    details = serializers.CharField()


class BaseWebhookSerializer(serializers.Serializer):
    id = serializers.CharField()
    type = serializers.CharField()
    data = serializers.JSONField()



class BaseFeedbackPromptSerializer(serializers.ModelSerializer):
    class Meta:
        model = FeedbackPrompt
        fields = "__all__"