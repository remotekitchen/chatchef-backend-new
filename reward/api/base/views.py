from django.db.models import Q
from decimal import Decimal
from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.exceptions import ParseError
from rest_framework.generics import (CreateAPIView, ListAPIView,
                                     ListCreateAPIView,
                                     RetrieveUpdateDestroyAPIView,
                                     get_object_or_404)
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from food.models import Restaurant
from chatchef.settings.defaults import mapbox_api_key
from django.db.models import Prefetch, Q
from accounts.api.base.serializers import BaseRestaurantUserSerializer
from accounts.models import RestaurantUser
from core.api.mixins import GetObjectWithParamMixin, UserCompanyListCreateMixin
from core.api.paginations import StandardResultsSetPagination,CustomPageSizePagination
from core.api.permissions import HasRestaurantAccess
from marketing.api.v1.serializers import (VoucherGetSerializer,
                                          VoucherSerializer)
from marketing.models import Voucher
from reward.api.base.serializers import (BaseRewardGroupSerializer,
                                         BaseRewardLevelSerializer,
                                         BaseRewardManageSerializer,
                                         BaseRewardSerializer,
                                         BaseUserRewardCreateSerializer,
                                         BaseUserRewardSerializer,BaseCampaignSerializer,BaseTaskSerializer,BaseRewardRedemptionSerializer)
from reward.api.v1.serializers import UserRewardSerializer, LocalDealSerializer
from reward.models import (Reward, RewardGroup, RewardLevel, RewardManage,
                           UserReward, LocalDeal,Campaign,Task, Cut, RewardRedemption)
from referral.models import InviteCodes,Referral
from remotekitchen.api.base.serializers import RemoteKitchenRestaurantSerializer
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta
from accounts.models import  User
from django.contrib.auth import get_user_model
from reward.tasks import send_on_time_reward_notification
from reward.models import RewardGroup, Reward, UserReward, AdditionalCondition
from django.db import transaction
from django.utils import timezone
from rest_framework.decorators import action
from firebase.models import TokenFCM
from django.core.mail import send_mail
from django.conf import settings
from marketing.email_sender import send_email


import random
import string
from billing.models import Order

User = get_user_model()

def get_distance_km(lat1, lng1, lat2, lng2):
    """
    Haversine formula to calculate the distance between two coordinates in KM.
    """
    try:
        lat1, lng1, lat2, lng2 = map(float, [lat1, lng1, lat2, lng2])
    except (TypeError, ValueError):
        return None

    # Radius of earth in kilometers
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2)**2
    c = 2 * asin(sqrt(a))
    return round(R * c, 2)

class BaseRewardGroupListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    model_class = RewardGroup
    serializer_class = BaseRewardGroupSerializer
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ["restaurant"]
    pagination_class = StandardResultsSetPagination
    search_fields = ['name']

    def get_queryset(self):
        exclude_expired = self.request.query_params.get('exclude_expired', False)
        queryset = (
            super()
            .get_queryset()
            .filter(deleted=False)
            .prefetch_related(
                Prefetch(
                    'reward_set',
                    queryset=Reward.objects.select_related('reward_group').prefetch_related('items')
                ),
                'additionalcondition_set'
            )
        )
        if exclude_expired is not False:
            q_exp = ~Q(validity_type=RewardGroup.ValidityType.SPECIFIC_DATE) | Q(
                validity_date__gte=timezone.now().date()
            )
            queryset = queryset.filter(q_exp)
        return queryset



class BaseRewardGroupRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseRewardGroupSerializer
    filterset_fields = ["id"]
    permission_classes = [HasRestaurantAccess]
    model_class = RewardGroup


class BaseRewardListAPIView(ListAPIView):
    serializer_class = BaseRewardSerializer
    model_class = Reward
    filterset_fields = ["reward_group", "restaurant"]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        query = self.request.query_params
        reward_group, restaurant = query.get(
            'reward_group'), query.get('restaurant')
        if reward_group is None and restaurant is None:
            raise ParseError('reward_group or restaurant is required!')
        q_exp = Q()
        if reward_group is not None:
            q_exp &= Q(reward_group=reward_group)
        if restaurant is not None:
            q_exp &= Q(restaurant=restaurant)
        return Reward.objects.filter(q_exp)


class BaseUserRewardListCreateAPIView(ListCreateAPIView):
    serializer_class = BaseUserRewardSerializer
    permission_classes = [IsAuthenticated]
    filterset_fields = ["restaurant", "location"]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        q_exp = Q(user=self.request.user,
                  is_claimed=False, reward__isnull=False)
        # & (Q(expiry_date__gte=timezone.now().date()) | Q(expiry_date__isnull=True))
        queryset = UserReward.objects.filter(q_exp)
        return queryset

    def get_serializer_class(self):
        if self.request.method == "POST":
            return BaseUserRewardCreateSerializer
        return BaseUserRewardSerializer

    # Allow restaurants to create rewards based on reward points


class BaseRestaurantsRewardListCreateView(ListCreateAPIView):
    model_class = Reward
    serializer_class = BaseRewardSerializer
    permission_classes = [HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        restaurant_id = self.kwargs.get('restaurant_id')
        queryset = Reward.objects.filter(restaurant=restaurant_id)
        return queryset


# Allow consumers to use their reward points to get coupons


class BaseRewardManageListCreateView(ListCreateAPIView):
    model_class = RewardManage
    serializer_class = BaseRewardManageSerializer
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination
    filterset_fields = ['restaurant']

    def get_queryset(self):
        """
           Reward manages are filtered based on query param filter:
           available -> All rewards based on user's reward point
           upcoming -> Others
        """
        qs = RewardManage.objects.all()
        query = self.request.query_params
        filter_base = query.get('filter', None)
        restaurant_user = RestaurantUser.objects.get_or_create(
            restaurant_id=query.get('restaurant'),
            user=self.request.user
        )[0]
        q_exp = Q()
        if filter_base == 'available':
            q_exp &= Q(points_required__lte=restaurant_user.reward_points)

        elif filter_base == 'upcoming':
            q_exp &= Q(points_required__gt=restaurant_user.reward_points)

        return qs.filter(q_exp)




from django.db.models import Q, Exists, OuterRef
from rest_framework.response import Response
from django.utils import timezone

class BaseAllCouponAPIView(APIView):
    # permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        query_params = request.query_params
        restaurant = query_params.get('restaurant')
        location = query_params.get('location')
        is_ht_voucher_param = query_params.get('is_ht_voucher')

        current_time = timezone.now()

        if is_ht_voucher_param and is_ht_voucher_param.lower() == 'true':
            # Return ONLY HT vouchers
            ht_q_exp = Q(is_ht_voucher=True)
            if restaurant:
                ht_q_exp &= Q(restaurant_id=restaurant)
            if location is not None:
                ht_q_exp &= (Q(location_id=location) | Q(location__isnull=True))

            ht_voucher_qs = Voucher.objects.filter(ht_q_exp)
            if request.user.is_authenticated:
                from django.db.models import Count, OuterRef, Subquery, IntegerField, Value, Case, When, F

                # Subquery: count how many times this voucher was used by this user
                usage_subquery = (
                    Order.objects.filter(
                        voucher=OuterRef("pk"),
                        user=request.user
                    )
                    .order_by()
                    .values("voucher")
                    .annotate(voucher_count=Count("id"))
                    .values("voucher_count")
                )

                # Annotate with usage count (default to 0)
                ht_voucher_qs = ht_voucher_qs.annotate(
                    user_usage_count=Subquery(usage_subquery, output_field=IntegerField())
                ).annotate(
                    user_usage_count=Case(
                        When(user_usage_count__isnull=True, then=Value(0)),
                        default="user_usage_count",
                        output_field=IntegerField(),
                    )
                )

                # Exclude one-time-use vouchers if used >= 1
                ht_voucher_qs = ht_voucher_qs.exclude(
                    Q(is_one_time_use=True) & Q(user_usage_count__gte=1)
                )

                # Exclude any voucher where usage >= max_uses
                ht_voucher_qs = ht_voucher_qs.exclude(
                    Q(max_uses__gt=0) & Q(user_usage_count__gte=F("max_uses"))
                )

            ht_voucher_data = VoucherGetSerializer(ht_voucher_qs, many=True).data

            return Response(ht_voucher_data)

        # Build filter for normal vouchers
        q_exp = Q()
        if restaurant:
            q_exp &= Q(restaurant_id=restaurant)
        if location is not None:
            q_exp &= (Q(location_id=location) | Q(location__isnull=True))

        # If is_ht_voucher=false, exclude HT vouchers
        if is_ht_voucher_param and is_ht_voucher_param.lower() == 'false':
            q_exp &= Q(is_ht_voucher=False)

        voucher_qs = Voucher.objects.filter(
            q_exp & Q(
                durations__start_date__lte=current_time,
                durations__end_date__gte=current_time
            )
        )
        print("hello bangladesh")
        voucher_data = VoucherGetSerializer(voucher_qs, many=True).data

        if request.user.is_authenticated and restaurant:
            reward_qs = UserReward.objects.filter(
                Q(user=request.user, is_claimed=False, reward__isnull=False) & (
                    Q(expiry_date__gte=current_time.date()) | Q(expiry_date__isnull=True)
                ) & q_exp
            )

            try:
                accept_first_second_third_reward = Restaurant.objects.get(id=restaurant).accept_first_second_third_user_reward
            except Restaurant.DoesNotExist:
                accept_first_second_third_reward = False

            if not accept_first_second_third_reward:
                reward_qs = reward_qs.exclude(platform=UserReward.PlatformChoices.REMOTEKITCHEN)

            reward_data = UserRewardSerializer(reward_qs, many=True).data
            voucher_data.extend(reward_data)

        return Response(voucher_data)

from rest_framework.response import Response
from django.db.models import Q
from django.utils import timezone

class BaseAllCouponChatchefAPIView(APIView):
    # permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        query_params = request.query_params
        location = query_params.get('location')
        current_time = timezone.now()

        # Fetch restaurants data and serialize it
        restaurants = Restaurant.objects.all()
        if location is not None:
            restaurants = restaurants.filter(Q(location_id=location) | Q(location__isnull=True))
        serialized_restaurants = RemoteKitchenRestaurantSerializer(
            restaurants,
            many=True,
            context={'request': request}
        ).data

        # Extract restaurant IDs from serialized data
        restaurant_ids = [restaurant['id'] for restaurant in serialized_restaurants]

        # Fetch vouchers data based on restaurant IDs
        voucher_qs = Voucher.objects.filter(
            Q(restaurant_id__in=restaurant_ids) &
            Q(durations__start_date__lte=current_time, durations__end_date__gte=current_time)
        )
        voucher_data = VoucherGetSerializer(voucher_qs, many=True).data

        # Add rewards if the user is authenticated
        if request.user.is_authenticated:
            reward_qs = UserReward.objects.filter(
                Q(user=request.user, is_claimed=False, reward__isnull=False) & (
                    Q(expiry_date__gte=current_time.date()) | Q(expiry_date__isnull=True)) &
                Q(restaurant_id__in=restaurant_ids)
            )
            reward_data = UserRewardSerializer(reward_qs, many=True).data
            voucher_data.extend(reward_data)

        # Combine the serialized restaurant and voucher data
        response_data = {
            'vouchers': voucher_data
        }

        return Response(response_data)


# Redemption or Redeem generator

class BaseRedeemRewardPointAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            query = request.data
            user = self.request.user
            restaurant = query.get('restaurant', None)
            location = query.get('location', None)
            reward_manage = query.get('reward', None)
            if restaurant is None or location is None or reward_manage is None:
                raise ParseError(
                    'restaurant, location and reward_manage required!')
            reward_manage = RewardManage.objects.get(id=reward_manage)
            restaurant_user = RestaurantUser.objects.get_or_create(
                user=user, restaurant=restaurant)[0]
            user_rewards = restaurant_user.redeem_reward_points(
                reward_manage=reward_manage, location=location)

            return Response(
                BaseUserRewardSerializer(user_rewards, many=True).data,
                status=status.HTTP_200_OK
            )

        except RewardManage.DoesNotExist:
            return Response({'error': 'Reward Manage not found'}, status=status.HTTP_404_NOT_FOUND)


class BaseRewardLevelListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    model_class = RewardLevel
    serializer_class = BaseRewardLevelSerializer
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination
    filterset_fields = ['restaurant']

    def get_queryset(self):
        return super().get_queryset().prefetch_related(
            "reward_manages",
            "reward_manages__reward_group",
            "reward_manages__reward_group__reward_set",
            "reward_manages__reward_group__additionalcondition_set"
        )



class BaseRewardLevelDOGetAPIView(ListAPIView):
    model_class = RewardLevel
    queryset = RewardLevel.objects.all()
    serializer_class = BaseRewardLevelSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    filterset_fields = ['restaurant']


class BaseRewardLevelRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = RewardLevel
    serializer_class = BaseRewardLevelSerializer
    permission_classes = [IsAuthenticated, HasRestaurantAccess]
    filterset_fields = ['id']


class BaseCouponCreateAPIView(CreateAPIView):
    serializer_class = BaseUserRewardCreateSerializer
    permission_classes = [IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(user=self.request.user)

class BaseLocalDealViewSet(viewsets.ModelViewSet):
    queryset = LocalDeal.objects.all()
    serializer_class = LocalDealSerializer

    def get_permissions(self):
        if self.action in ['list', 'retrieve']:
            return [AllowAny()]
        return [IsAuthenticated(), HasRestaurantAccess()]

    def list(self, request, *args, **kwargs):
        user_lat = request.query_params.get('lat')
        user_lng = request.query_params.get('lng')
        restaurant_id = request.query_params.get('restaurant')
        sort_by = request.query_params.get('sort_by', 'relevance')  # relevance, location, discount, rating

        if restaurant_id:
            queryset = LocalDeal.objects.filter(restaurant_id=restaurant_id)
        else:
            queryset = LocalDeal.objects.select_related('restaurant', 'menu_item').all()

            if user_lat and user_lng:
                filtered_deals = []
                for deal in queryset:
                    rest = deal.restaurant
                    if rest and rest.latitude and rest.longitude:
                        distance = get_distance_km(user_lat, user_lng, rest.latitude, rest.longitude)
                        if distance is not None and distance <= 20:
                            deal.distance = distance
                            deal.discount_percent = self._get_discount_percent(deal)
                            deal.restaurant_rating = getattr(rest, 'average_rating', 0)
                            filtered_deals.append(deal)
                queryset = filtered_deals

                # Sort
                if sort_by == 'location':
                    queryset = sorted(queryset, key=lambda d: d.distance)
                elif sort_by == 'discount':
                    queryset = sorted(queryset, key=lambda d: d.discount_percent, reverse=True)
                elif sort_by == 'rating':
                    queryset = sorted(queryset, key=lambda d: d.restaurant_rating, reverse=True)

            else:
                # No location filter, just sort by discount or rating if requested
                for deal in queryset:
                    deal.discount_percent = self._get_discount_percent(deal)
                    deal.restaurant_rating = getattr(deal.restaurant, 'average_rating', 0)

                if sort_by == 'discount':
                    queryset = sorted(queryset, key=lambda d: d.discount_percent, reverse=True)
                elif sort_by == 'rating':
                    queryset = sorted(queryset, key=lambda d: d.restaurant_rating, reverse=True)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def _get_discount_percent(self, deal):
        try:
            base = Decimal(deal.main_price or deal.menu_item.base_price)
            deal_price = Decimal(deal.deal_price)
            if base <= 0:
                return 0
            return round((base - deal_price) / base * 100, 2)
        except:
            return 0

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)





from django.db import transaction
# Chatchef: Endpoint to handle On-Time Guarantee reward
# class OnTimeGuaranteeRewardAPIView(APIView):
#     def post(self, request, *args, **kwargs):
#         # Extract the data from the request
#         data = request.data
#         user_id = data.get("user_id")
#         reward_amount = data.get("reward_amount")
#         delivery_id = data.get("delivery_id")
#         delivery_time = data.get("delivery_time")

#         # Validate the incoming data
#         if not user_id or not reward_amount or not delivery_id or not delivery_time:
#             return Response({"error": "Missing required data"}, status=status.HTTP_400_BAD_REQUEST)

#         try:
#             # Fetch the user from the database
#             user = User.objects.get(id=user_id)

#             # Start a database transaction to ensure consistency
#             with transaction.atomic():
#                 # Create the reward object
#                 reward = Reward.objects.create(
#                     reward_type="coupon",  # Set the type to coupon for the reward
#                     amount=reward_amount,
#                     offer_type="flat",  # You can adjust based on your reward system
#                 )

#                 # Create a UserReward object which is linked to the user and reward
#                 user_reward = UserReward.objects.create(
#                     user=user,
#                     reward=reward,
#                     is_claimed=False,
#                     expiry_date=timezone.now() + timedelta(days=7)  # Set expiry to 7 days
#                 )

#             # Respond with success
#             return Response({
#                 "message": "Reward issued successfully",
#                 "reward_id": user_reward.id
#             }, status=status.HTTP_201_CREATED)

#         except User.DoesNotExist:
#             return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
#         except Exception as e:
            # return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



def generate_random_string(length=8, include_numbers=True, include_punctuations=False):
    chars = string.ascii_letters
    if include_numbers:
        chars += string.digits
    if include_punctuations:
        chars += string.punctuation
    return ''.join(random.choices(chars, k=length))


class BaseIssueRewardAPIView(APIView):
    def post(self, request, *args, **kwargs):
        print("call reward")
        data = request.data
        user_id = data.get("user_id")
        reward_amount = data.get("reward_amount")
        reward_type = data.get("reward_type", "coupon")  # Default to 'coupon'
        expiry_date = data.get("expiry_date", None)
        order_id = data.get("order_id")  # ✅ Step 1: Receive order_id

        if not user_id or not reward_amount:
            return Response({"error": "Missing required data"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            reward_amount = float(reward_amount)
        except ValueError:
            return Response({"error": "Invalid reward amount"}, status=status.HTTP_400_BAD_REQUEST)

        if not expiry_date:
            expiry_date = (timezone.now() + timedelta(days=7)).date()

        try:
            user = User.objects.get(id=user_id)

            # ✅ Create or reuse reward group
            reward_group, _ = RewardGroup.objects.get_or_create(
                name="On-Time Delivery Guarantee",
                applies_for=[RewardGroup.AppliesFor.DELIVERY],
                validity_type=RewardGroup.ValidityType.DAYS_AFTER_REWARDED,
                validity_days=7,
            )

            # ✅ Generate tag from order_id
            extra_tag = f"on_time_reward_order_{order_id}" if order_id else None

            # ✅ Prevent duplicate reward creation
            existing_user_reward = UserReward.objects.filter(
                user=user,
                reward__reward_group=reward_group,
                reward__amount=reward_amount,
                reward__reward_type=Reward.RewardType.COUPON,
                is_claimed=False,
                expiry_date__gte=timezone.now().date(),
                code__icontains=extra_tag if extra_tag else "",
            ).first()

            if existing_user_reward:
                return Response({
                    "message": "Reward already issued for this order.",
                    "reward_id": existing_user_reward.id
                }, status=status.HTTP_200_OK)

            with transaction.atomic():
                # ✅ Create reward
                reward = Reward.objects.create(
                    reward_group=reward_group,
                    reward_type=Reward.RewardType.COUPON,
                    offer_type=Reward.OfferType.FLAT,
                    amount=reward_amount,
                )

                # ✅ Attach AdditionalCondition
                AdditionalCondition.objects.create(
                    reward_group=reward_group,
                    condition_type=AdditionalCondition.ConditionType.MINIMUM_AMOUNT,
                    amount=50
                )

                # ✅ Generate reward code
                base_code = generate_random_string(include_numbers=False, include_punctuations=False)
                full_code = f"{base_code}_{extra_tag}" if extra_tag else base_code

                # ✅ Create user reward
                user_reward = UserReward.objects.create(
                    user=user,
                    reward=reward,
                    amount=reward_amount,
                    expiry_date=expiry_date,
                    is_claimed=False,
                    code=full_code,
                )
                # ✅ Create corresponding Voucher for order usage
                # Voucher.objects.create(
                #     reward=reward,
                #     voucher_code=full_code,
                #     amount=reward_amount,
                #     minimum_spend=50,
                #     max_redeem_value=reward_amount,
                #     is_one_time_use=True,
                #     is_global=False,
                #     is_ht_voucher=True,
                #     ht_voucher_percentage_borne_by_restaurant=0,
                #     max_uses=1,
                
                # )

                # ✅ Assign reward_coupon to Order if provided
                if order_id:
                    try:
                        order = Order.objects.get(id=order_id)
                        order.reward_coupon = user_reward  # ✅ Fix: assign FK instance, not string
                        order.save(update_fields=["reward_coupon"])
                    except Order.DoesNotExist:
                        print(f"⚠️ Order with ID {order_id} not found")

                # ✅ Trigger notification
                def notify():
                    print("📨 Calling send_on_time_reward_notification task...")
                    send_on_time_reward_notification.delay(
                        user_id=user.id,
                        reward_amount=reward_amount,
                        code=user_reward.code,
                        expiry_date=str(user_reward.expiry_date)
                    )

                transaction.on_commit(notify)

            return Response({
                "message": "Reward issued successfully",
                "reward_id": user_reward.id
            }, status=status.HTTP_201_CREATED)

        except User.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        



# lucky spin campaign
# lucky spin campaign

class BaseCampaignViewSet(viewsets.ModelViewSet):
    queryset = Campaign.objects.all().order_by('-created_at')
    serializer_class = BaseCampaignSerializer


# views/task_views.py


class BaseTaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.all()
    serializer_class = BaseTaskSerializer

    @action(detail=True, methods=['post'])
    def cut(self, request, pk=None):
        task = self.get_object()
        result = perform_cut(task, request.user, request.data)
        return Response(result)

    @action(detail=True, methods=['post'])
    def redeem(self, request, pk=None):
        task = self.get_object()
        result, status_code = redeem_reward(task, request.user)
        return Response(result, status=status_code)

    @action(detail=False, methods=['post'])
    def start(self, request):
        user = request.user
        campaign_id = request.data.get("campaign_id")
        invite_code = request.data.get("invite_code")

        try:
            campaign = Campaign.objects.get(id=campaign_id)
        except Campaign.DoesNotExist:
            return Response({"detail": "Invalid campaign"}, status=400)

        task = Task.objects.create(
            user=user,
            campaign=campaign,
            current_drop_item=campaign.drop_items[0]
        )

        if invite_code:
            try:
                invite = InviteCodes.objects.get(code=invite_code, status="pending")
                invite.status = "accepted"
                invite.save()
                invite.refer.invited_users.add(user)
                invite.refer.joined_users.add(user)

                # Notify inviter
                inviter = invite.refer.user
                context = {
                    "user": inviter,
                    "friend_name": user.first_name,
                }
                send_mail(
                    "Your friend joined!",
                    f"🎉 {user.first_name} joined your Lucky Spin! You're closer to a reward!",
                    settings.DEFAULT_HUNGRY_TIGER_EMAIL,
                    [inviter.email]
                )
                send_push_notification(inviter, {
                    "title": "🎉 Friend Joined",
                    "message": f"{user.first_name} joined your Lucky Spin! You're closer to a reward!",
                    "screen": "lucky_spin"
                })
            except InviteCodes.DoesNotExist:
                pass

        return Response(BaseTaskSerializer(task).data)

    @action(detail=True, methods=['post'])
    def generate_invite_code(self, request, pk=None):
        task = self.get_object()
        referral, _ = Referral.objects.get_or_create(user=task.user)

        code = str(uuid.uuid4())[:8]
        invite = InviteCodes.objects.create(
            refer=referral,
            code=code
        )

        # Firebase Dynamic Link (update domain to your own Firebase one)
        base_dynamic_link = "https://hungrytiger.page.link"
        deep_link = f"https://www.hungry-tiger.com/spin?invite={code}"

        params = {
            "link": deep_link,
            "apn": "com.chatchefs.mealmingle",  # Android package
            "afl": "https://play.google.com/store/apps/details?id=com.chatchefs.mealmingle",
            "st": "Join Lucky Spin 🎉",
            "sd": f"{task.user.first_name} invited you to spin and win rewards!",
        }

        firebase_link = f"{base_dynamic_link}/?{urlencode(params)}"

        return Response({
            "invite_code": code,
            "invite_link": firebase_link,
            "message": f"{task.user.first_name} invited you to win prizes! Join now using this code: {code}"
        })

    @action(detail=True, methods=['post'])
    def exchange(self, request, pk=None):
        task = self.get_object()
        from_item = request.data.get("from")
        to_item = request.data.get("to")
        amount = request.data.get("amount")

        result, status_code = perform_exchange(task, from_item, to_item, amount)
        return Response(result, status=status_code)


# services.py


def send_push_notification(user, data):
    tokens = list(TokenFCM.objects.filter(user=user).values_list("token", flat=True))
    if tokens:
        from core.utils import send_push_notification as fcm_send
        fcm_send(tokens, data)


def perform_cut(task, user, data):
    is_new_user = data.get('is_new_user', False)
    real_contribution = 10 if is_new_user else 2

    task.task_progress_real += real_contribution
    task.task_progress_fake = min(task.task_progress_real * 0.9 + 3, 99)

    drop_item = task.current_drop_item or task.campaign.drop_items[0]
    drop_amount = 2 ** (task.drop_counter % 5)

    inventory = task.item_inventory
    inventory[drop_item] = inventory.get(drop_item, 0) + drop_amount
    task.item_inventory = inventory

    task.drop_counter += 1
    if task.drop_counter % task.campaign.drop_cycle == 0:
        next_index = (task.campaign.drop_items.index(drop_item) + 1) % len(task.campaign.drop_items)
        task.current_drop_item = task.campaign.drop_items[next_index]

    task.save()

    Cut.objects.create(
        task=task,
        cutter=user,
        contribution=real_contribution,
        is_new_user=is_new_user,
        item_dropped=drop_item,
        item_quantity=drop_amount
    )

    # Trigger push/email notification if big drop or finished
    if drop_amount >= 16 or task.task_progress_real >= 100:
        message = f"🎉 {user.first_name} just got {drop_amount} {drop_item}!"
        send_mail("Big Drop!", message, settings.DEFAULT_HUNGRY_TIGER_EMAIL, [user.email])
        send_push_notification(user, {
            "title": "🎉 Big Drop!",
            "message": message,
            "screen": "lucky_spin"
        })

    return BaseTaskSerializer(task).data


def redeem_reward(task, user):
    inventory = task.item_inventory or {}
    campaign = task.campaign

    for item, threshold in campaign.item_thresholds.items():
        if inventory.get(item, 0) >= threshold:
            with transaction.atomic():
                inventory[item] -= threshold
                task.item_inventory = inventory
                task.save()

                reward = RewardRedemption.objects.create(
                    task=task,
                    user=user,
                    reward_value=campaign.reward_amount,
                    item_used=item
                )
                return BaseRewardRedemptionSerializer(reward).data, status.HTTP_200_OK

    return {"detail": "Reward threshold not met"}, status.HTTP_400_BAD_REQUEST


def perform_exchange(task, from_item, to_item, amount):
    try:
        amount = int(amount)
        rate_key = f"{from_item}_to_{to_item}"
        exchange_rate = task.campaign.exchange_rates.get(rate_key)

        if not exchange_rate:
            return {"detail": "Invalid exchange pair"}, status.HTTP_400_BAD_REQUEST

        if task.item_inventory.get(from_item, 0) < amount:
            return {"detail": f"Not enough {from_item}"}, status.HTTP_400_BAD_REQUEST

        to_amount = int(amount * exchange_rate)
        task.item_inventory[from_item] -= amount
        task.item_inventory[to_item] = task.item_inventory.get(to_item, 0) + to_amount
        task.save()

        return BaseTaskSerializer(task).data, status.HTTP_200_OK

    except Exception:
        return {"detail": "Invalid input"}, status.HTTP_400_BAD_REQUEST
