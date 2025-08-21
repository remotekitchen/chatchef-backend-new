from django.db.models import Q
from decimal import Decimal
from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.exceptions import (NotFound, ParseError,
                                       )
from rest_framework.generics import (CreateAPIView, ListAPIView,
                                     ListCreateAPIView,
                                     RetrieveUpdateDestroyAPIView,
                                     get_object_or_404)
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from food.models import Restaurant
from hungrytiger.settings.defaults import mapbox_api_key
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
                           UserReward, LocalDeal,Campaign,Task, Cut, RewardRedemption,Spin,CoinWallet,CoinTransactionLog,LuckyReferral,LuckyInviteCodes,CampaignProgressLog)
from referral.models import InviteCodes,Referral
from reward.utils.event import log_user_event
from firebase.utils.fcm_helper import send_push_notification

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

from django.db.models import Sum

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


class BaseCampaignViewSet(viewsets.ModelViewSet):
    queryset = Campaign.objects.all().order_by('-created_at')
    serializer_class = BaseCampaignSerializer

    @action(detail=True, methods=["patch"])
    def config(self, request, pk=None):
        campaign = self.get_object()
        fields = [
            "reward_amount", "target_cac", "completion_rate_base",
            "cut_decay_alpha", "cut_decay_beta", "drop_items",
            "drop_cycle", "item_thresholds", "exchange_rates",
        ]
        updated = False

        for field in fields:
            if field in request.data:
                setattr(campaign, field, request.data[field])
                updated = True

        if updated:
            campaign.save()
            return Response({"message": "Campaign config updated."})
        return Response({"message": "No valid fields provided."}, status=400)
    
    @action(detail=True, methods=["get"])
    def cac(self, request, pk=None):
        campaign = self.get_object()
        
        total_rewards = RewardRedemption.objects.filter(task__campaign=campaign).aggregate(
            total=Sum('reward_value')
        )['total'] or 0

        total_completed = Task.objects.filter(campaign=campaign, is_completed=True).count()
        new_users = LuckyReferral.objects.filter(restaurant__campaign=campaign).annotate(
            count=Count('joined_users')
        ).aggregate(total=Sum('count'))['total'] or 0

        if new_users == 0:
            return Response({"cac": None, "message": "No new users yet"})

        completion_rate = total_completed / Task.objects.filter(campaign=campaign).count()
        cac_value = (total_rewards * completion_rate) / new_users

        return Response({
            "cac": round(cac_value, 2),
            "rewards_paid": total_rewards,
            "completion_rate": round(completion_rate, 2),
            "new_users": new_users
        })


import secrets

CAMPAIGN_BOOST_CONFIG = {
    "campaign_1": [
        (900_000, 0.09),
        (1_100_000, 0.11),
        (25_000, 0.0025),
        (10_000, 0.002),
        (5_000, 0.001),
        (2_500, 0.0005),
        (1_500, 0.0003),
    ]
}


def apply_campaign_boost(self, task, config):
        total_coins = 0
        progress_before = Decimal(task.task_progress_fake)

        for coins, progress in config:
            total_coins += coins
            task.task_progress_fake = Decimal(task.task_progress_fake) + Decimal(progress)
            if task.task_progress_fake >= Decimal("0.9998"):
                task.task_progress_fake = Decimal("0.9998")
                break

        task.stage = 1
        task.save()

        CampaignProgressLog.objects.create(
            task=task,
            coin_amount=total_coins,
            progress_before=progress_before,
            progress_after=task.task_progress_fake,
            source="system"
        )

        return total_coins, float(task.task_progress_fake)



def generate_branch_link(code):
    # branch_key = "key_live_xxx"  # Your Branch.io live key
    # headers = {"Content-Type": "application/json"}

    # data = {
    #     "branch_key": branch_key,
    #     "channel": "referral",
    #     "feature": "invite",
    #     "campaign": "lucky_spin",
    #     "data": {
    #         "$canonical_url": f"https://www.hungry-tiger.com/spin?invite={code}",
    #         "invite_code": code,
    #         "$deeplink_path": f"spin",
    #         "$android_deeplink_path": f"spin",
    #         "$ios_deeplink_path": f"spin",
    #         "$desktop_url": f"https://www.hungry-tiger.com/spin?invite={code}"
    #     }
    # }

    # response = requests.post("https://api2.branch.io/v1/url", headers=headers, json=data)
    # return response.json().get("url", f"https://www.hungry-tiger.com/spin?invite={code}")
    return f"https://www.hungry-tiger.com/spin?invite={code}"



class BaseTaskViewSet(viewsets.ModelViewSet):
    queryset = Task.objects.all()
    serializer_class = BaseTaskSerializer
    permission_classes = [IsAuthenticated]

    @action(detail=False, methods=['post'])
    def start(self, request):
        user = request.user
        campaign_id = request.data.get("campaign_id")
        invite_code = request.data.get("invite_code")

        try:
            campaign = Campaign.objects.get(id=campaign_id)
        except Campaign.DoesNotExist:
            return Response({"detail": "Invalid campaign"}, status=400)

        existing_task = Task.objects.filter(user=user, campaign=campaign, is_completed=False).first()
        if existing_task:
            return Response(BaseTaskSerializer(existing_task).data, status=200)

        task = Task.objects.create(
            user=user,
            campaign=campaign,
            current_drop_item=campaign.drop_items[0] if campaign.drop_items else None
        )

        # Welcome Bonus
        wallet, _ = CoinWallet.objects.get_or_create(user=user, campaign=campaign)

        welcome_bonus_given = CoinTransactionLog.objects.filter(user=user, source="welcome_bonus").exists()
        if not welcome_bonus_given:
            wallet.coins += 50
            wallet.save()
            CoinTransactionLog.objects.create(
                user=user,
                amount=50,
                balance_after=wallet.coins,
                source="welcome_bonus",
                description="Welcome bonus for first-time Lucky Spin"
            )

        log_user_event(user=user, event_name="spin_install")

        # Referral Logic
        if invite_code:
            try:
                invite = LuckyInviteCodes.objects.get(code=invite_code, status="pending")
                inviter = invite.refer.user

                if inviter == user:
                    return Response({"detail": "You cannot invite yourself."}, status=400)

                if user in invite.refer.invited_users.all():
                    return Response({"detail": "You have already used this invite code."}, status=400)

                invite.status = "accepted"
                invite.save()
                invite.refer.invited_users.add(user)
                invite.refer.joined_users.add(user)

                send_mail(
                    subject="Your friend joined!",
                    message=f"🎉 {user.first_name} joined your Lucky Spin! You're closer to a reward of {campaign.reward_amount}৳!",
                    from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL,
                    recipient_list=[inviter.email],
                )

                tokens = list(TokenFCM.objects.filter(user=inviter).values_list("token", flat=True))
                if tokens:
                    push_data = {
                        "campaign_title": "🎉 Friend Joined!",
                        "campaign_message": f"{user.first_name} joined your Lucky Spin! You're closer to a reward of {campaign.reward_amount}৳!",
                        "screen": "lucky_spin",
                        "type": "referral_joined",
                        "id": user.id,
                    }
                    send_push_notification(tokens, push_data)

                inviter_task = Task.objects.filter(user=inviter, campaign=campaign).first()
                if inviter_task and not inviter_task.is_completed:
                    perform_cut(inviter_task, user, {"is_new_user": True})

                log_user_event(user=user, event_name="spin_invite_clicked", metadata={"invite_code": invite_code})
                log_user_event(user=inviter, event_name="referral_signup", metadata={"invited_user": user.id})

            except LuckyInviteCodes.DoesNotExist:
                return Response({"detail": "Invalid invite code."}, status=400)

        # ✅ Apply Boost Automatically (Campaign 1 logic)
        campaign_key = "campaign_1"
        boost_config = CAMPAIGN_BOOST_CONFIG.get(campaign_key)

        total_coins, new_progress = 0, 0
        if boost_config:
            from decimal import Decimal
            progress_before = Decimal(task.task_progress_fake)

            for coins, progress in boost_config:
                total_coins += coins
                task.task_progress_fake += Decimal(progress)
                if task.task_progress_fake >= Decimal("0.9998"):
                    task.task_progress_fake = Decimal("0.9998")
                    break

            task.is_boosted = True
            task.stage = 1
            task.save()

            CampaignProgressLog.objects.create(
                task=task,
                coin_amount=total_coins,
                progress_before=progress_before,
                progress_after=task.task_progress_fake,
                source="system"
            )

            log_user_event(user, "boost_started", {
                "task_id": task.id,
                "coin_total": total_coins
            })

        # Response
        response_data = BaseTaskSerializer(task).data
        response_data.update({
            "real_wallet_coins": wallet.coins,
            "welcome_bonus": 50 if not welcome_bonus_given else 0,
            "is_boosted": task.is_boosted,
            "stage": task.stage,
            "fake_progress_added": round(float(task.task_progress_fake - progress_before), 4),
            "fake_progress_total": round(float(task.task_progress_fake), 4),
            "fake_coins_simulated": total_coins,
            "next_stage_available": float(task.task_progress_fake) >= 0.9998,
            "ui_message": (
                f"🎉 Boost applied with {total_coins:,} fake coins! "
                f"Your progress is now {round(float(task.task_progress_fake * 100), 2)}%.\n"
                f"🎁 You've also received {wallet.coins} real coins to spin and win!"
            )
        })


        return Response(response_data, status=201)
    
    @action(detail=True, methods=['post'], permission_classes=[IsAuthenticated])
    def update_progress(self, request, pk=None):
        task = self.get_task(pk)

        if Decimal(task.task_progress_fake) >= Decimal("0.9998"):
            return Response({"detail": "Progress already at max"}, status=400)

        config = CAMPAIGN_BOOST_CONFIG["campaign_1"]

        used_steps = set(
            CampaignProgressLog.objects.filter(
                task=task,
                source="system-fake-step"
            ).values_list("coin_amount", "progress_after")
        )

        coins, progress = None, None

        for c, p in config:
            next_progress = Decimal(task.task_progress_fake) + Decimal(p)
            if (c, float(next_progress)) not in used_steps and next_progress <= Decimal("0.9998"):
                coins, progress = c, p
                break

        if not progress:
            if Decimal(task.task_progress_fake) >= Decimal("0.9996"):
                progress_before = Decimal(task.task_progress_fake)
                task.task_progress_fake = Decimal("0.9998")
                task.save(update_fields=["task_progress_fake"])

                CampaignProgressLog.objects.create(
                    task=task,
                    coin_amount=0,
                    progress_before=progress_before,
                    progress_after=task.task_progress_fake,
                    source="system-fake-step"
                )

                return Response({
                    "progress_before": round(float(progress_before), 6),
                    "progress_after": 0.9998,
                    "coins_simulated": 0,
                    "message": "🎯 Boost complete! Progress is now 99.98%",
                    "next_stage_available": True
                })

            return Response({"detail": "No valid progress step found"}, status=400)

        # Valid step found — apply it
        progress_before = Decimal(task.task_progress_fake)
        task.task_progress_fake = min(Decimal("0.9998"), progress_before + Decimal(progress))
        task.save(update_fields=["task_progress_fake"])

        CampaignProgressLog.objects.create(
            task=task,
            coin_amount=coins,
            progress_before=progress_before,
            progress_after=task.task_progress_fake,
            source="system-fake-step"
        )

        return Response({
            "progress_before": round(float(progress_before), 6),
            "progress_added": round(float(Decimal(progress)), 6),
            "progress_after": round(float(task.task_progress_fake), 6),
            "coins_simulated": coins,
            "next_stage_available": float(task.task_progress_fake) >= 0.9998,
            "ui_message": (
                f"✨ {coins:,} fake coins added! Progress increased by "
                f"{round(float(progress) * 100, 2)}% to "
                f"{round(float(task.task_progress_fake * 100), 2)}%"
            )
        })
    
    @action(detail=True, methods=['post'], permission_classes=[IsAuthenticated])
    def next_stage_trigger(self, request, pk=None):
        task = self.get_task(pk)

        if Decimal(task.task_progress_fake) < Decimal("0.9998"):
            return Response({"detail": "Progress not yet eligible for next stage."}, status=400)

        if task.stage >= 2:
            return Response({"detail": "Already in Stage 2 or beyond."}, status=400)

        task.stage = 2
        task.save()

        log_user_event(task.user, "stage_2_unlocked", {
            "task_id": task.id,
            "progress": float(task.task_progress_fake)
        })

        return Response({
            "message": "Stage 2 unlocked",
            "new_stage": task.stage
        })


    @action(detail=False, methods=['post'])
    def spin(self, request):
        user = request.user
        campaign_id = request.data.get("campaign_id")

        if not campaign_id:
            return Response({"detail": "campaign_id is required"}, status=400)

        try:
            campaign = Campaign.objects.get(id=campaign_id, is_active=True)
        except Campaign.DoesNotExist:
            return Response({"detail": "Invalid or inactive campaign"}, status=400)

        wallet, _ = CoinWallet.objects.get_or_create(user=user, campaign=campaign)

        task = Task.objects.filter(user=user, campaign=campaign).first()
        if not task or task.stage < 2:
            return Response({"detail": "You must unlock Stage 2 before spinning."}, status=400)

        SPIN_COST = 5
        before_balance = wallet.coins

        if wallet.coins < SPIN_COST:
            return Response({"detail": "Not enough coins to spin"}, status=400)

        # Daily limit per campaign
        today = timezone.now().date()
        today_spin_count = Spin.objects.filter(user=user, campaign=campaign, created_at__date=today).count()
        if today_spin_count >= 30:
            return Response({"detail": "You’ve reached today’s spin limit."}, status=400)

        # Weighted reward
        REWARD_DISTRIBUTION = [
            (2, 0.30),
            (3, 0.25),
            (5, 0.20),
            (7, 0.10),
            (10, 0.08),
            (15, 0.05),
            (20, 0.02),
        ]
        def weighted_random_choice(choices):
            values, weights = zip(*choices)
            return random.choices(values, weights=weights, k=1)[0]

        earned = weighted_random_choice(REWARD_DISTRIBUTION)

        # Jackpot
        is_jackpot = False
        if random.random() < 0.01:
            earned = 50
            is_jackpot = True
            tokens = TokenFCM.objects.filter(user=user).values_list("token", flat=True)
            if tokens:
                push_data = {
                    "campaign_title": "🎉 Jackpot!",
                    "campaign_message": "You hit the 50-coin jackpot!",
                    "screen": "lucky_spin",
                    "type": "jackpot",
                    "id": user.id,
                }
                send_push_notification(tokens, push_data)

        # Double card
        double_used = False
        if wallet.double_card_available:
            earned *= 2
            double_used = True
            wallet.double_card_available = False

        # Deduct spin cost
        wallet.coins -= SPIN_COST
        wallet.save()

        CoinTransactionLog.objects.create(
            user=user,
            amount=-SPIN_COST,
            balance_after=wallet.coins,
            campaign=campaign,
            source="spin_cost",
            description="Deducted 5 coins for spinning"
        )

        # Add reward
        wallet.coins += earned
        wallet.save()

        CoinTransactionLog.objects.create(
            user=user,
            amount=earned,
            balance_after=wallet.coins,
            campaign=campaign,
            source="jackpot" if is_jackpot else "spin_reward",
            description=f"Earned {earned} coins from Lucky Spin{' (doubled)' if double_used else ''}"
        )

        # Log spin
        Spin.objects.create(
            user=user,
            campaign=campaign,
            coins_earned=earned,
            doubled=double_used,
            double_card_used=double_used,
        )

        # Epic spin push
        if earned >= 15:
            tokens = TokenFCM.objects.filter(user=user).values_list("token", flat=True)
            if tokens:
                push_data = {
                    "campaign_title": "🔥 Epic Spin!",
                    "campaign_message": f"You earned {earned} coins in one spin!",
                    "screen": "lucky_spin",
                    "type": "epic_spin",
                    "id": user.id,
                }
                send_push_notification(tokens, push_data)

        return Response({
            "earned": earned,
            "double_used": double_used,
            "spin_cost": SPIN_COST,
            "before_balance": before_balance,
            "after_balance": wallet.coins,
            "net_change": wallet.coins - before_balance,
            "new_balance": wallet.coins,
            "spins_today": today_spin_count + 1,
            "spins_remaining": max(0, 30 - (today_spin_count + 1)),
            "is_jackpot": is_jackpot,
        })



        

    @action(detail=True, methods=['post'])
    def cut(self, request, pk=None):
        task = self.get_object()
        result = perform_cut(task, request.user, request.data)

        if isinstance(result, tuple):
            data, status_code = result
        else:
            data, status_code = result, 200

        log_user_event(user=request.user, event_name="cut", metadata={"task_id": task.id})
        return Response(data, status=status_code)
    
    @action(detail=True, methods=['post'])
    def redeem(self, request, pk=None):
        task = self.get_object()
        result, status_code = redeem_reward(task, request.user)
        return Response(result, status=status_code)



    @action(detail=True, methods=['post'])
    def generate_invite_code(self, request, pk=None):
        task = self.get_object()

        # Get or create the referral for the user + campaign
        referral, _ = LuckyReferral.objects.get_or_create(user=task.user, campaign=task.campaign)

        # Generate unique code
        code = str(uuid.uuid4())[:8]

        # Create invite with the referral instance
        invite = LuckyInviteCodes.objects.create(
            refer=referral,  # Use the instance, not the class
            code=code,
            status=LuckyInviteCodes.STATUS.PENDING
        )

        # Generate and attach Firebase/Branch link (your custom function)
        invite_link = generate_branch_link(code)
        invite.firebase_link = invite_link
        invite.save(update_fields=["firebase_link"])

        # Log the referral event
        log_user_event(user=task.user, event_name="referral_shared", metadata={"code": code})

        return Response({
            "invite_code": code,
            "invite_link": invite_link,
            "message": f"{task.user.first_name} invited you to win prizes! Use the link below to join.",
        })
    

    @action(detail=True, methods=["get"])
    def status(self, request, pk=None):
        task = self.get_object()
        user = task.user

        # Total coins used in system boost
        total_coins = CampaignProgressLog.objects.filter(task=task).aggregate(
            total=Sum("coin_amount")
        )["total"] or 0

        # Determine progress state
        boost_complete = task.task_progress_fake >= 0.9998
        current_stage = "invite" if boost_complete else "system_boost"
        almost_there = 0.9990 <= task.task_progress_fake < 0.9998

        # Last progress update time
        last_log = CampaignProgressLog.objects.filter(task=task).order_by("-created_at").first()
        last_progress_time = last_log.created_at if last_log else None

        # Wallet
        wallet, _ = CoinWallet.objects.get_or_create(user=user, campaign=task.campaign)


        # Remaining manual cuts (limit: 20/day)
        today = timezone.now().date()
        cut_count = Cut.objects.filter(task=task, created_at__date=today).count()
        cuts_remaining = max(0, 20 - cut_count)

        return Response({
            "task_id": task.id,
            "campaign_id": task.campaign.id,
            "user_id": user.id,

            # Progress info
            "current_stage": current_stage,
            "progress_percentage": round(task.task_progress_fake * 100, 4),
            "boost_complete": boost_complete,
            "show_almost_there_message": almost_there,

            # Action permissions
            "can_cut": boost_complete and cuts_remaining > 0,
            "can_exchange": boost_complete,
            # "cuts_remaining": cuts_remaining,

            # Wallet info
            "wallet_balance": wallet.coins,
            "double_card_available": wallet.double_card_available,

            # Items (from JSONField)
            "item_inventory": task.item_inventory,

            # Log info
            "total_coins_used_for_boost": total_coins,
            "progress_last_updated_at": last_progress_time,
        })


    @action(detail=True, methods=['post'])
    def exchange(self, request, pk=None):
        task = self.get_object()
        from_item = request.data.get("from_item")
        to_item = request.data.get("to_item")
        amount = request.data.get("amount")

        result, status_code = perform_exchange(task, from_item, to_item, amount)

        if status_code == 200:
            log_user_event(user=task.user, event_name="item_exchanged", metadata={
                "from": from_item,
                "to": to_item,
                "amount": amount
            })

        
        return Response(result, status=status_code)
    


    @action(detail=True, methods=["get"])
    def check_ready(self, request, pk=None):
        task = self.get_object()
        campaign = task.campaign

        # Step 1: Check item thresholds
        inventory = task.item_inventory or {}
        missing_items = {}
        has_all_items = True

        redeemed_items = task.redeemed_items or []


        for item, threshold in campaign.item_thresholds.items():
            # If item already redeemed, skip checking
            if item in redeemed_items:
                continue

            current = inventory.get(item, 0)
            if current < threshold:
                has_all_items = False
                missing_items[item] = threshold - current


        # Step 2: Check referral requirement
        required_referrals = getattr(campaign, 'required_referrals', 1)
        referral = LuckyReferral.objects.filter(user=task.user).first()
        joined_count = referral.joined_users.count() if referral else 0
        real_progress_done = joined_count >= required_referrals

        # Step 3: Final eligibility
        can_redeem = has_all_items and real_progress_done

        if can_redeem:
            message = "You're ready to redeem!"
        elif not has_all_items:
            message = "Collect all required items to redeem."
        else:
            message = f"Invite {required_referrals - joined_count} more friend(s) to redeem!"

        return Response({
            "has_all_items": has_all_items,
            "real_progress_done": real_progress_done,
            "can_redeem": can_redeem,
            "joined_count": joined_count,
            "required_referrals": required_referrals,
            "redeemed_items": task.redeemed_items or [],

            "message": message,
            "missing_items": missing_items
        })

    @action(detail=True, methods=["patch"])
    def force_complete(self, request, pk=None):
        task = self.get_object()
        task.task_progress_real = 100
        task.task_progress_fake = 0.9999
        task.is_completed = True
        task.save()

        log_user_event(user=task.user, event_name="admin_force_completed", metadata={
            "task_id": task.id
        })

        return Response({"message": "Task marked as completed"})

    def get_task(self, pk):
        try:
            return Task.objects.select_related("campaign", "user").get(pk=pk)
        except Task.DoesNotExist:
            raise NotFound("Task not found")


def perform_cut(task, user, data):
    print(f"🚀 Cut triggered by {user.email} on task {task.id} for campaign {task.campaign.id}")

    is_manual = data.get('is_manual', False)
    is_new_user = data.get('is_new_user', False)
    inviter = task.user

    # 1. Manual cut limit check (with coin extension)
    MAX_DAILY_MANUAL_CUTS = 5
    MAX_EXTRA_MANUAL_CUTS = 2
    EXTRA_MANUAL_CUT_COST = 10
    MAX_TOTAL_MANUAL_CUTS = MAX_DAILY_MANUAL_CUTS + MAX_EXTRA_MANUAL_CUTS

    start_of_day = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)
    manual_cuts_today = Cut.objects.filter(
        cutter=user,
        is_manual=True,
        created_at__gte=start_of_day
    ).count()

    if is_manual and manual_cuts_today >= MAX_TOTAL_MANUAL_CUTS:
        return {
            "detail": "You've used all manual cuts for today, including 2 extra ones.",
            "max_manual_cuts": MAX_TOTAL_MANUAL_CUTS,
            "manual_cuts_used": manual_cuts_today,
            "manual_cuts_left": 0
        }, 429

    # Use coins if extra cut
    if is_manual and manual_cuts_today >= MAX_DAILY_MANUAL_CUTS:
        wallet, _ = CoinWallet.objects.get_or_create(user=user, campaign=task.campaign)
        if wallet.coins < EXTRA_MANUAL_CUT_COST:
            return {"detail": "Not enough coins to unlock extra manual cut."}, 400

        wallet.coins -= EXTRA_MANUAL_CUT_COST
        wallet.save()

        CoinTransactionLog.objects.create(
            user=user,
            amount=-EXTRA_MANUAL_CUT_COST,
            balance_after=wallet.coins,
            campaign=task.campaign,
            source="extra_manual_cut",
            description="Spent 10 coins for extra manual cut"
        )

    # 2. Prevent any further cuts after task complete
    if task.task_progress_real >= 1.0:
        return {"detail": "Task already completed."}, 400

    # 3. Block self-cut unless manual
    if inviter == user and not is_manual:
        return {"detail": "Self-cut is not allowed."}, 400

    # 4. Prevent multiple real cuts by same friend
    if not is_manual and Cut.objects.filter(task=task, cutter=user, is_new_user=True).exists():
        return {"detail": "You've already contributed to this task."}, 400

    # 5. Progress Calculation
    invite_goal = task.campaign.required_referrals or 30
    real_contribution = Decimal("1.0") / Decimal(invite_goal) if is_new_user else Decimal("0.0")
    fake_contribution = Decimal("0.02")

    FAKE_PROGRESS_MAX = Decimal("0.9998")
    REAL_PROGRESS_MAX = Decimal("1.0")

    if is_manual:
        task.task_progress_fake = min(task.task_progress_fake + fake_contribution, FAKE_PROGRESS_MAX)
    else:
        task.task_progress_real = min(task.task_progress_real + real_contribution, REAL_PROGRESS_MAX)
        task.task_progress_fake = min(task.task_progress_real * Decimal("0.9") + Decimal("0.03"), FAKE_PROGRESS_MAX)

    task.task_progress_real = min(task.task_progress_real, REAL_PROGRESS_MAX)
    task.task_progress_fake = min(task.task_progress_fake, FAKE_PROGRESS_MAX)

    # 6. Drop logic
    if not task.campaign.drop_items:
        return {"detail": "No drop items configured for this campaign"}, 400

    drop_item = task.current_drop_item or task.campaign.drop_items[0]
    drop_amount = 2 ** (task.drop_counter % 5)

    inventory = task.item_inventory or {}
    inventory[drop_item] = inventory.get(drop_item, 0) + drop_amount
    task.item_inventory = inventory

    # 7. Drop cycle switching
    drop_cycle = task.campaign.drop_cycle or 5
    task.drop_counter += 1

    if drop_cycle > 0 and task.drop_counter % drop_cycle == 0:
        try:
            current_index = task.campaign.drop_items.index(drop_item)
        except ValueError:
            current_index = 0
        next_index = (current_index + 1) % len(task.campaign.drop_items)
        task.current_drop_item = task.campaign.drop_items[next_index]

    task.save()

    # 8. Log the cut
    Cut.objects.create(
        task=task,
        cutter=user,
        contribution=real_contribution if not is_manual else 0,
        is_new_user=is_new_user,
        is_manual=is_manual,
        item_dropped=drop_item,
        item_quantity=drop_amount
    )

    print(f"✅ Cut saved: user={user.email}, task={task.id}, contribution={real_contribution}, drop={drop_item}:{drop_amount}")

    # 9. Log user event
    log_user_event(user=user, event_name="item_dropped", metadata={
        "task_id": task.id,
        "item": drop_item,
        "quantity": drop_amount
    })

    # 10. Manual cut stats
    manual_cuts_used = manual_cuts_today + (1 if is_manual else 0)

    response = {
        "detail": "Cut successful",
        "progress_real": float(round(task.task_progress_real * 100, 2)),
        "progress_fake": float(round(task.task_progress_fake * 100, 2)),
        "item_dropped": drop_item,
        "quantity": drop_amount,
        "new_inventory": task.item_inventory,
        "task": BaseTaskSerializer(task).data,
        "max_manual_cuts": MAX_DAILY_MANUAL_CUTS,
        "manual_cuts_used": manual_cuts_used,
        "manual_cuts_left": max(0, MAX_DAILY_MANUAL_CUTS - manual_cuts_used)
    }

    # 11. Send mail + push notification on big drop or 100%
    if drop_amount >= 16 or task.task_progress_real >= 1.0:
        message = f"🎉 {user.first_name} just got {drop_amount} {drop_item}!"
        send_mail(
            subject="Big Drop!",
            message=message,
            from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL,
            recipient_list=[user.email],
        )
        tokens = list(TokenFCM.objects.filter(user=user).values_list("token", flat=True))
        if tokens:
            send_push_notification(tokens, {
                "campaign_title": "🎁 Lucky Drop!",
                "campaign_message": message,
                "screen": "lucky_spin",
                "type": "item_drop",
                "id": user.id,
            })

    return response


def redeem_reward(task, user):
    inventory = task.item_inventory or {}
    campaign = task.campaign

    # ✅ Check referral progress
    required_referrals = getattr(campaign, 'required_referrals', 1)
    referral = LuckyReferral.objects.filter(user=user).first()
    joined_count = referral.joined_users.count() if referral else 0

    if joined_count < required_referrals:
        return {
            "detail": f"You must invite at least {required_referrals} friend(s) who sign up to unlock the reward.",
            "joined_count": joined_count,
            "required_referrals": required_referrals,
        }, status.HTTP_400_BAD_REQUEST

    # ✅ Check item thresholds
    missing_items = {}
    for item, threshold in campaign.item_thresholds.items():
        current = inventory.get(item, 0)
        if current < threshold:
            missing_items[item] = threshold - current

    if missing_items:
        return {
            "detail": "You have not collected all required items.",
            "missing_items": missing_items,
        }, status.HTTP_400_BAD_REQUEST

    # ✅ Deduct items and issue reward (atomic)
    with transaction.atomic():
        for item, threshold in campaign.item_thresholds.items():
            inventory[item] -= threshold

        task.item_inventory = inventory
        task.save()

        reward = RewardRedemption.objects.create(
            task=task,
            user=user,
            reward_value=campaign.reward_amount,
            item_used=", ".join(campaign.item_thresholds.keys())
        )

        log_user_event(user=user, event_name="reward_redeemed", metadata={
            "task_id": task.id,
            "reward": str(campaign.reward_amount),
            "items_used": campaign.item_thresholds,
        })
        log_user_event(user=user, event_name="reward_claimed", metadata={
            "task_id": task.id,
            "reward": str(campaign.reward_amount),
        })

        return {
            "detail": "Reward redeemed successfully",
            "items_used": campaign.item_thresholds,
            "reward_value": campaign.reward_amount,
            "remaining_inventory": inventory,
            "reward": BaseRewardRedemptionSerializer(reward).data
        }, status.HTTP_200_OK


from math import ceil
from math import ceil

def perform_exchange(task, from_item, to_item, amount):
    campaign = task.campaign
    inventory = task.item_inventory or {}

    # Validate input
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return {"detail": "Invalid amount."}, 400

    if from_item == to_item:
        return {"detail": "Cannot exchange the same item."}, 400

    if amount <= 0:
        return {"detail": "Amount must be greater than 0."}, 400

    if from_item not in inventory or inventory[from_item] < amount:
        return {"detail": f"Not enough {from_item} to exchange."}, 400

    # Get thresholds and exchange rate
    threshold = campaign.item_thresholds.get(to_item)
    if threshold is None:
        return {"detail": f"No threshold set for item {to_item}."}, 400

    current_to_count = inventory.get(to_item, 0)
    delta = threshold - current_to_count

    if delta <= 0:
        return {"detail": f"{to_item} already meets or exceeds the threshold."}, 400

    exchange_rates = campaign.exchange_rates or {}
    key = f"{from_item}_to_{to_item}"
    exchange_rate = exchange_rates.get(key)

    if not exchange_rate:
        return {"detail": f"No exchange rate defined for {from_item} → {to_item}."}, 400

    needed_from = ceil(delta / exchange_rate)

    if amount < needed_from:
        return {"detail": f"You need at least {needed_from} {from_item} to complete exchange."}, 400

    # Perform exchange
    received_amount = int(amount * exchange_rate)
    inventory[from_item] -= amount
    inventory[to_item] = inventory.get(to_item, 0) + received_amount

    # Reward unlock logic
    unlocked = False
    task.redeemed_items = task.redeemed_items or []
    
    if inventory[to_item] >= threshold and to_item not in task.redeemed_items:
        unlocked = True
        task.redeemed_items.append(to_item)

    task.item_inventory = inventory
    task.save()

    return {
        "detail": "Exchange successful",
        "from_item": from_item,
        "to_item": to_item,
        "amount_spent": amount,
        "amount_received": received_amount,
        "inventory": inventory,
        "reward_unlocked": unlocked,
        "unlocked_item": to_item if unlocked else None
    }, 200


