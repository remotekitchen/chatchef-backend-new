import hashlib
import json
import random
import string
import threading
import time
from datetime import datetime
from django.db.models import Count, OuterRef, Subquery, IntegerField, Value, Case, When, F, Q
from django.db.models import Q
from django.template.loader import render_to_string
from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.exceptions import (MethodNotAllowed, ParseError,
                                       PermissionDenied)
from rest_framework.generics import (GenericAPIView, ListAPIView,
                                     ListCreateAPIView, RetrieveAPIView,
                                     RetrieveUpdateDestroyAPIView,
                                     get_object_or_404)
from rest_framework.permissions import (IsAuthenticated,
                                        IsAuthenticatedOrReadOnly)
from rest_framework.response import Response
from rest_framework.views import APIView

from accounts.models import RestaurantUser
from billing.models import Order
from communication.models import CustomerInfo
from core.api.mixins import GetObjectWithParamMixin, UserCompanyListCreateMixin
from core.api.paginations import StandardResultsSetPagination
from core.api.permissions import HasRestaurantAccess
from core.utils import get_logger
from Event_logger.models import Action_logs
from Event_logger.utils import action_saver
from food.models import Location, MenuItem, Restaurant
from marketing.api.base.mixins import ActivationCampaignListCreateMixin
from marketing.api.base.serializers import (
    BaseBirthdayGiftSerializer, BaseBogoDetailSerializer, BaseBxGyDetailSerializer, BaseBogoSerializer, BaseBxGySerializer,
    BaseContactUsDataSerializer, BaseDemoDataSerializer,
    BaseEmailConfigSerializer, BaseEmailHistorySerializers,
    BaseEmailSendSerializers, BaseFissionCampaignSerializer,
    BaseFissionPrizeSerializer, BaseGiftCardSerializer,
    BaseGroupPromotionSerializer, BaseLoyaltyProgramSerializer,
    BaseMembershipCardSerializer, BaseReviewRatingSerializer,
    BaseSpendXSaveYManagerSerializer, BaseSpendXSaveYPromoOptionSerializer,
    BaseSpendXSaveYSerializer, BaseStaffSendEmailSerializer,
    BaseVoucherSerializer, RatingSerializer, ReviewSerializer, CommentSerializer, UserSerializer, AutoReplyToCommentsSerializer)
from marketing.email_sender import send_email
from marketing.api.v1.serializers import ReviewGetSerializer
from marketing.models import (BirthdayGift, Bogo, BxGy, ContactUsData, DemoData,
                              EmailConfiguration, EmailHistory,
                              FissionCampaign, GiftCard, GroupPromotion,
                              LoyaltyProgram, MembershipCard, Rating, Review,
                              SpendXSaveY, SpendXSaveYManager,
                              SpendXSaveYPromoOption, Voucher, Comment, AutoReplyToComments)
from marketing.utils.check_weekday import is_current_week
from marketing.utils.inflat_bogo_item import inflate_items_after_set_bogo
from referral.api.base.serializers import BaseReferralSerializer
from referral.models import Referral, StaffReferral
from reward.models import UserReward


logger = get_logger()


class BaseSpendXSaveYListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    serializer_class = BaseSpendXSaveYSerializer
    model_class = SpendXSaveY
    filterset_fields = ['restaurant', 'location']
    permission_classes = [HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination


class BaseSpendXSaveYRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseSpendXSaveYSerializer
    model_class = SpendXSaveY
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ['id']


class BaseSpendXSaveYManagerListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    serializer_class = BaseSpendXSaveYManagerSerializer
    model_class = SpendXSaveYManager
    filterset_fields = ['restaurant', 'location']
    permission_classes = [HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination


class BaseSpendXSaveYManagerRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseSpendXSaveYManagerSerializer
    model_class = SpendXSaveYManager
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ['id', 'restaurant', 'location']

    def get_object(self):
        restaurant, location = self.request.query_params.get('restaurant', None), \
            self.request.query_params.get('location', None)
        q_exp = Q()
        if restaurant is not None:
            q_exp &= Q(restaurant=restaurant)
        if location is not None:
            q_exp &= Q(location=location)
        return SpendXSaveYManager.objects.filter(q_exp).first()


class BaseSpendXSaveYPromoOptionListAPIView(ListAPIView):
    serializer_class = BaseSpendXSaveYPromoOptionSerializer
    queryset = SpendXSaveYPromoOption.objects.all()
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination


class BaseVoucherListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    pagination_class = StandardResultsSetPagination
    serializer_class = BaseVoucherSerializer
    model_class = Voucher
    filterset_fields = ['restaurant', 'location']
    permission_classes = [HasRestaurantAccess]

    def get_queryset(self):
        qs = super().get_queryset()
        direct_order = self.request.query_params.get('direct_order', False)
        filter_param = self.request.query_params.get('filter', None)

        if direct_order:
            if not self.request.user.is_authenticated:
                # Anonymous users can only access vouchers available to ALL
                qs = qs.filter(available_to=Voucher.Audience.ALL)
            else:
                user = self.request.user

                #  Annotate how many times this user used each voucher
                usage_subquery = (
                    Order.objects.filter(
                        voucher=OuterRef('pk'),
                        user=user
                    )
                    .values('voucher')
                    .annotate(voucher_count=Count('id'))
                    .values('voucher_count')
                )

                qs = qs.annotate(
                    user_usage_count=Subquery(usage_subquery, output_field=IntegerField())
                ).annotate(
                    user_usage_count=Case(
                        When(user_usage_count__isnull=True, then=Value(0)),
                        default=F("user_usage_count"),
                        output_field=IntegerField()
                    )
                )

                #  Apply filter if provided
                if filter_param == 'used':
                    qs = qs.filter(user_usage_count__gte=1)
                elif filter_param == 'unused':
                    qs = qs.filter(user_usage_count__lt=1)

                #  Order-based exclusions
                first_order_exclusion = ~Q(available_to=Voucher.Audience.FIRST_ORDER)
                second_order_exclusion = ~Q(available_to=Voucher.Audience.SECOND_ORDER)
                third_order_exclusion = ~Q(available_to=Voucher.Audience.THIRD_ORDER)

                order_count = Order.objects.filter(user=user, is_paid=True).count()

                if order_count >= 1:
                    qs = qs.filter(first_order_exclusion)
                if order_count >= 2:
                    qs = qs.filter(second_order_exclusion)
                if order_count >= 3:
                    qs = qs.filter(third_order_exclusion)
                    
        qs = qs.order_by('-id')
        return qs


# class BaseVoucherListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
#     pagination_class = StandardResultsSetPagination
#     serializer_class = BaseVoucherSerializer
#     model_class = Voucher
#     filterset_fields = ['restaurant', 'location']
#     permission_classes = [HasRestaurantAccess]

#     def get_queryset(self):
#       qs = super().get_queryset()
#       direct_order = self.request.query_params.get('direct_order', False)
    
#       if direct_order:
#           if not self.request.user.is_authenticated:
#             # Anonymous users can only access vouchers available to ALL
#               qs = qs.filter(available_to=Voucher.Audience.ALL)
#           else:
#               user = self.request.user
#               # Filter vouchers based on user's order history
#               first_order_exclusion = ~Q(available_to=Voucher.Audience.FIRST_ORDER)
#               second_order_exclusion = ~Q(available_to=Voucher.Audience.SECOND_ORDER)
#               third_order_exclusion = ~Q(available_to=Voucher.Audience.THIRD_ORDER)

#               # Determine if the user qualifies for certain order-based vouchers
#               order_count = Order.objects.filter(user=user, is_paid=True).count()

#               if order_count >= 1:
#                   qs = qs.filter(first_order_exclusion)  # Exclude first-order vouchers if user has 1+ orders
#               if order_count >= 2:
#                   qs = qs.filter(second_order_exclusion)  # Exclude second-order vouchers if user has 2+ orders
#               if order_count >= 3:
#                   qs = qs.filter(third_order_exclusion)  # Exclude third-order vouchers if user has 3+ orders
#       print("im calling",qs)
#       return qs
    

class BaseVoucherRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseVoucherSerializer
    model_class = Voucher
    filterset_fields = ['id']
    permission_classes = [HasRestaurantAccess]


class BaseBogoListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    serializer_class = BaseBogoSerializer
    model_class = Bogo
    filterset_fields = ['restaurant', 'location']
    pagination_class = StandardResultsSetPagination
    permission_classes = [HasRestaurantAccess]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        inflate_items_after_set_bogo(serializer.instance)

        return Response(self.get_serializer(serializer.instance).data, status=status.HTTP_201_CREATED)
      
class BaseBxGyListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    serializer_class = BaseBxGySerializer
    model_class = BxGy
    filterset_fields = ['restaurant', 'location']
    permission_classes = [HasRestaurantAccess]
    pagination_class = StandardResultsSetPagination
    
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)

        return Response(self.get_serializer(serializer.instance).data, status=status.HTTP_201_CREATED)


class BaseBogoRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseBogoDetailSerializer
    model_class = Bogo
    filterset_fields = ['id']
    permission_classes = [HasRestaurantAccess]

class BaseBxGyRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseBxGyDetailSerializer
    model_class = BxGy
    filterset_fields = ['id']
    permission_classes = [HasRestaurantAccess]

class BaseGroupPromotionListCreateAPIView(ActivationCampaignListCreateMixin, ListCreateAPIView):
    pagination_class = StandardResultsSetPagination
    model_class = GroupPromotion
    filterset_fields = ['restaurant', 'location']
    permission_classes = [HasRestaurantAccess]
    serializer_class = BaseGroupPromotionSerializer


class BaseGroupPromotionRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = GroupPromotion
    filterset_fields = ['id']
    permission_classes = [HasRestaurantAccess]
    serializer_class = BaseGroupPromotionSerializer


class BaseLoyaltyProgramListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    serializer_class = BaseLoyaltyProgramSerializer
    model_class = LoyaltyProgram
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ['restaurant']
    pagination_class = StandardResultsSetPagination


class BaseLoyaltyProgramRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseLoyaltyProgramSerializer
    model_class = LoyaltyProgram
    permission_classes = [HasRestaurantAccess]
    filterset_fields = ['id']


# Fission campaign apis
class BaseFissionCampaignListCreateApiView(UserCompanyListCreateMixin, ListCreateAPIView):
    pagination_class = StandardResultsSetPagination
    model_class = FissionCampaign
    filterset_fields = ['restaurant']
    permission_classes = [HasRestaurantAccess]
    serializer_class = BaseFissionCampaignSerializer

    def get_queryset(self):
        return super().get_queryset().prefetch_related(
            'prizes',
            'durations',
            'restaurant',
        )


class BaseFissionCampaignRetrieveUpdateDestroyApiView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = FissionCampaign
    filterset_fields = ['id']
    permission_classes = [HasRestaurantAccess]
    serializer_class = BaseFissionCampaignSerializer


class BaseRandomPrizeAPIView(RetrieveAPIView):
    serializer_class = BaseFissionPrizeSerializer

    # permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        query = self.request.query_params
        fission_id, restaurant, location = query.get(
            'fission_id', None
        ), query.get('restaurant', None), query.get('location', None)
        if fission_id is None or restaurant is None:
            raise ParseError('fission id and restaurant is required!')

        fission: FissionCampaign = get_object_or_404(
            FissionCampaign, id=fission_id
        )

        # Getting random prize
        prize = fission.get_random_prize()
        response = BaseFissionPrizeSerializer(prize).data
        if self.request.user.is_authenticated:
            self._handle_user_campaigns(restaurant=restaurant, fission=fission)
            # if fission is None:
            #     raise ParseError('Restaurant does not have any Fission campaign!')
        try:
            user_rewards = UserReward.objects.create_from_reward_group(
                user=self.request.user if self.request.user.is_authenticated else None,
                reward_group=prize.reward_group,
                restaurant=restaurant,
                location=location
            )
            response.update(
                {
                    'coupon': [user_reward.code for user_reward in user_rewards]
                }
            )
        except Exception as e:
            logger.error(f'user reward create error {e}')

        return Response(response)

    def _handle_user_campaigns(self, restaurant, fission):
        """ Check if user can use this fission """
        if fission.availability != FissionCampaign.Availability.ONCE_EVERY_WEEK:
            """
                Time limit campaigns are eligible for users who have fulfilled some conditions
            """
            self._handle_non_once_in_week_campaigns(
                restaurant=restaurant, fission=fission)
        else:
            """
                Repeating ones can be used by every user once a week on specified weekday
            """
            self._handle_once_in_week_campaigns(fission=fission)

    def _handle_non_once_in_week_campaigns(self, restaurant, fission):
        restaurant_user: RestaurantUser = RestaurantUser.objects.get_or_create(
            user=self.request.user, restaurant_id=restaurant
        )[0]
        active_campaigns = restaurant_user.available_lucky_draws.get_non_once_in_week_campaigns()
        if fission not in active_campaigns:
            if fission.availability == FissionCampaign.Availability.AFTER_EVERY_ORDER:
                raise PermissionDenied(
                    'You have to place an order in order to use this lucky flip!')
            if fission.availability == FissionCampaign.Availability.ONCE_EVERY_USER:
                raise PermissionDenied(
                    'You already have used this lucky flip!')
            if fission.availability == FissionCampaign.Availability.AFTER_SIGN_UP:
                raise PermissionDenied(
                    'This lucky flip is for new users only!')
            raise PermissionDenied('You can\'t use this lucky flip!')
        restaurant_user.available_lucky_draws.remove(fission)

    def _handle_once_in_week_campaigns(self, fission):
        current_week = is_current_week(fission.last_used_time)
        logger.info(
            f'current week {current_week} {self.request.user in fission.last_week_users.all()}')
        """
            If user used the lucky flip this week, raise exception
        """
        if current_week and self.request.user in fission.last_week_users.all():
            raise PermissionDenied("Your limit for this week has reached.")

        """
            If the campaign was not used this week, clear the user list
        """
        if not current_week:
            fission.last_week_users.clear()

        fission.last_week_users.add(self.request.user)
        fission.last_used_time = timezone.now()
        fission.save(update_fields=['last_used_time'])


class BaseUserFissionCampaignListAPIView(ListAPIView):
    serializer_class = BaseFissionCampaignSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        restaurant = self.request.query_params.get('restaurant', None)
        if restaurant is None:
            raise ParseError('restaurant id required!')
        restaurant_obj = get_object_or_404(Restaurant, id=restaurant)
        restaurant_user = RestaurantUser.objects.get_or_create(
            restaurant=restaurant_obj, user=self.request.user
        )[0]
        # current_time = timezone.now()
        return FissionCampaign.objects.get_available_campaigns(
            user=self.request.user, restaurant=restaurant, restaurant_user=restaurant_user
        )


class BaseBirthdayGiftListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    model_class = BirthdayGift
    filterset_fields = ['restaurant', 'location']
    serializer_class = BaseBirthdayGiftSerializer
    permission_classes = [HasRestaurantAccess]


class BaseBirthdayGiftRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = BirthdayGift
    filterset_fields = ['id']
    serializer_class = BaseBirthdayGiftSerializer
    permission_classes = [HasRestaurantAccess]


class BaseGiftCardListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    model_class = GiftCard
    serializer_class = BaseGiftCardSerializer
    # permission_classes = [HasRestaurantAccess]


class BaseGiftCardRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = GiftCard
    serializer_class = BaseGiftCardSerializer
    # permission_classes = [HasRestaurantAccess]
    filterset_fields = ['id']


class BaseAllActivationCampaignListAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        restaurant = request.query_params.get('restaurant', None)
        q_exp = Q(company=request.user.company)
        if restaurant is not None:
            q_exp &= Q(restaurant_id=restaurant)
        spendx_savey = SpendXSaveY.objects.filter(q_exp)
        bogo = Bogo.objects.filter(q_exp)
        bxgy = BxGy.objects.filter(q_exp)
        voucher = Voucher.objects.filter(q_exp)
        group = GroupPromotion.objects.filter(q_exp)
        response = {
            'SpendXSaveY': BaseSpendXSaveYSerializer(spendx_savey, many=True).data,
            'Bogo': BaseBogoSerializer(bogo, many=True).data,
            'BxGy': BaseBxGySerializer(bxgy, many=True).data,
            'Voucher': BaseVoucherSerializer(voucher, many=True).data,
            'GroupPromotion': BaseGroupPromotionSerializer(group, many=True).data
        }
        return Response(response)


class BaseMembershipCardListCreateAPIView(UserCompanyListCreateMixin, ListCreateAPIView):
    permission_classes = [HasRestaurantAccess]
    model_class = MembershipCard
    filterset_fields = ['restaurant']
    serializer_class = BaseMembershipCardSerializer


class BaseMembershipCardRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    model_class = MembershipCard
    filterset_fields = ['id']
    serializer_class = BaseMembershipCardSerializer
    permission_classes = [HasRestaurantAccess]


class BaseRatingAndReviewModelView(viewsets.ModelViewSet):
    serializer_class = RatingSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [IsAuthenticatedOrReadOnly]

    def get_queryset(self, *args, **kwargs):
        queryset = Rating.objects.all()
        item = self.request.query_params.get('item')
        if item:
            queryset = Rating.objects.filter(menuItem=item)
        return queryset

    def create(self, request):
        serializer = BaseReviewRatingSerializer(data=request.data)
        action = Action_logs.objects.create(
            action='Review And Rating', logs='creating action'
        )
        if serializer.is_valid():
            action_saver(action, f'valid serializer data found')
            rating = None
            if 'rating' in serializer.data and serializer.data['rating']:
                id_item = serializer.data['menuItem']
                menu = MenuItem.objects.get(id=id_item)
                action_saver(
                    action, f'line 281 --> menu item {menu.id} --> serializer id -->{id_item}'
                )
                if Rating.objects.filter(menuItem=serializer.data['menuItem']).exists():
                    rating = Rating.objects.get(
                        menuItem=serializer.data['menuItem']
                    )
                    if rating.user.filter(id=request.user.id).exists():
                        rating.rating -= 1
                        rating.user.remove(request.user)
                        rating.save()
                        action_saver(action, f'rating removed')
                    else:
                        rating.rating += 1
                        rating.user.add(request.user)
                        rating.save()
                        action_saver(action, f'rating added')
                else:
                    rating = Rating.objects.create(
                        menuItem=menu
                    )
                    rating.rating += 1
                    rating.user.add(request.user)
                    rating.save()
                    action_saver(action, f'new rating object created')

            if rating is None:
                id_item = serializer.data['menuItem']
                menu = MenuItem.objects.get(id=id_item)
                action_saver(
                    action, f'line 306 --> rating none --> serializer id {id_item}'
                )
                if Rating.objects.filter(menuItem=id_item).exists():
                    action_saver(action, f'line 310 --> rating found')
                    rating = Rating.objects.get(menuItem=id_item)
                    action_saver(action, f'rating found')
                else:
                    rating = Rating.objects.create(
                        menuItem=menu
                    )
                    action_saver(action, f'creating new rating object')

            if rating:
                menu_item = rating.menuItem
                menu_item.rating = rating.rating
                menu_item.save()

            if rating is not None and 'review' in serializer.data:
                if Review.objects.filter(rating=rating, user=request.user).exists():
                    review = Review.objects.get(
                        rating=rating, user=request.user
                    )
                    review.review = serializer.data['review']
                    review.save()
                    action_saver(action, f'updating review')
                else:
                    Review.objects.create(
                        rating=rating, user=request.user, review=serializer.data['review']
                    )
                    action_saver(action, f'creating review')

            sr = self.get_serializer(rating)
            return Response(sr.data, status=status.HTTP_202_ACCEPTED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class BaseReviewModelView(viewsets.ModelViewSet):
    serializer_class = ReviewGetSerializer
    # queryset = Review.objects.all()
    user = UserSerializer(read_only=True)
    # restaurant_id = self.request.query_params.get('restaurant_id')  

    def get_queryset(self):
        queryset = Review.objects.all().order_by('-is_pinned')
        restaurant_id = self.request.query_params.get('restaurant_id')  
        if restaurant_id:
            queryset = queryset.filter(restaurant_id=restaurant_id)  
        return queryset

    def perform_create(self, serializer):
        # Save the review and ensure user is set
        serializer.save(user=self.request.user)

    def like(self, request, pk=None):
        review = self.get_object()  # Get the specific review
        if not request.user.is_authenticated:
            raise PermissionDenied("You must be logged in to like a review.")

        review.like(request.user)  # Add the like
        review.save()

        return Response({
            'status': 'review liked',
            'likes_count': review.likes.count(),
            'liked_users': UserSerializer(review.likes.all(), many=True).data
        }, status=status.HTTP_200_OK)

    def dislike(self, request, pk=None):
        review = self.get_object()  # Get the specific review
        if not request.user.is_authenticated:
            raise PermissionDenied("You must be logged in to dislike a review.")

        review.dislike(request.user)  # Add the dislike
        review.save()

        return Response({
            'status': 'review disliked',
            'dislikes_count': review.dislikes.count(),
            'disliked_users': UserSerializer(review.dislikes.all(), many=True).data
        }, status=status.HTTP_200_OK)

    def add_comment(self, request, pk=None):
      review = self.get_object()  # Get the review to comment on
      if not request.user.is_authenticated:
        raise PermissionDenied("You must be logged in to add a comment.")

      parent_comment = None

      # Check if this is a reply (i.e., has a parent comment)
      if 'parent' in request.data and request.data['parent'] is not None:
        try:
            parent_comment = Comment.objects.get(id=request.data['parent'])
        except Comment.DoesNotExist:
            return Response({'error': 'Parent comment does not exist'}, status=status.HTTP_400_BAD_REQUEST)

      # Create the comment
      serializer = CommentSerializer(data=request.data)
      if serializer.is_valid():
        serializer.save(user=request.user, review=review, parent=parent_comment)
        return Response(serializer.data, status=status.HTTP_201_CREATED)
      return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def get_nested_comments(self, request, pk=None):
        review = self.get_object()  # Get the specific review
        comments = review.comments.filter(parent= None)  # Fetch only top-level comments
        serializer = CommentSerializer(comments, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
      
    def delete_reviews(self, request, pk=None):
        if not request.user.is_authenticated:
            raise PermissionDenied("You must be logged in to delete a comment.")
          
        try:
            review = Review.objects.get(id=pk)
        except Review.DoesNotExist:
            return Response({'error': 'Review does not exist'}, status=status.HTTP_400_BAD_REQUEST)
        
        review.delete()
        return Response({'status': 'Review deleted'}, status=status.HTTP_200_OK)
      
    def pin_review(self, request, pk=None):
        if not request.user.is_authenticated:
            raise PermissionDenied("You must be logged in to pin a review.")

        try:
            review = Review.objects.get(id=pk)
        except Review.DoesNotExist:
            return Response({'error': 'Review does not exist'}, status=status.HTTP_404_NOT_FOUND)

        review.is_pinned = True
        review.save()

        return Response({
            'status': 'Review pinned',
            'review_id': review.id
        }, status=status.HTTP_200_OK)

class BaseEmailSendView(GenericAPIView):
    serializer_class = BaseEmailSendSerializers

    def post(self, request):
        serializer = self.get_serializer(data=request.data)

        if serializer.is_valid(raise_exception=True):
            data = serializer.data
            program = data.get("Program")
            audience = data.get("audience")
            subject = data.get('subject')
            html_path = data['html_path']
            context = data['context']
            to_emails = data['to_emails']
            restaurant = data.get('restaurant')
            schedule_time_str = data.get('schedule_time')

            if schedule_time_str is not None and schedule_time_str != "":
                schedule_time = datetime.strptime(
                    schedule_time_str, '%Y-%m-%dT%H:%M:%SZ')

                print(schedule_time)
            #   schedule_time = datetime.fromisoformat(schedule_time_str.replace(" ", "T"))
            #   print(schedule_time)
            else:
                schedule_time = schedule_time_str
            html_message = render_to_string(html_path, context)
            restaurant_instance = get_object_or_404(Restaurant, id=restaurant)
            sender_email = restaurant_instance.email
            if EmailConfiguration.objects.filter(restaurant=restaurant).exists():
                email_configs = EmailConfiguration.objects.get(
                    restaurant=restaurant
                )
                print(email_configs)
                sender_email = email_configs.email_host_user

            if schedule_time_str is not None and schedule_time_str != "" and schedule_time > datetime.now():
                email_history = EmailHistory(
                    program=program,
                    audience=audience,
                    restaurant=restaurant_instance,
                    subject=subject,
                    message=context.get("body", ""),
                    html_content=html_message,
                    scheduled_time=schedule_time,  # Replace with the desired scheduled time
                    sender_email=sender_email,
                )
                print(email_history)
                email_history.save()
                return Response({'message': 'Email Saved successfully!'})
            if audience == 'all':
                result_email = CustomerInfo.objects.filter(
                    restaurant=restaurant).values('email')
                print('line 525', result_email)
                for entry in result_email:
                    print(entry["email"])
                    if entry != "":
                        email_thread = threading.Thread(
                            target=send_email,
                            args=(subject, html_path, context,
                                  [entry["email"]], restaurant)
                        )
                        email_thread.start()

            if audience == "member":
                result_email = CustomerInfo.objects.filter(restaurant=data.get("restaurant"), is_member=True).values(
                    'email'
                )
                for entry in result_email:
                    if entry != "":
                        send_email(
                            subject,
                            html_path,
                            context,
                            [entry["email"]],
                            restaurant
                        )

            if audience == "custom":
                send_email(subject, html_path, context, to_emails, restaurant)

            email_history = EmailHistory(
                program=program,
                audience=audience,
                restaurant=restaurant_instance,
                subject=subject,
                message=context.get("body", ""),
                html_content=html_message,
                scheduled_time=timezone.now(),  # Replace with the desired scheduled time
                sender_email=sender_email,
            )
            email_history.save()
            return Response({'message': 'Email sent successfully!'})

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class BaseRestaurantEmailHistoryListView(ListAPIView):
    serializer_class = BaseEmailHistorySerializers
    filterset_fields = ['restaurant']

    def get_queryset(self):
        restaurant_id = self.kwargs['restaurant']

        return EmailHistory.objects.filter(restaurant=restaurant_id)


class BaseEmailConfigListCreateView(ListCreateAPIView):
    queryset = EmailConfiguration.objects.all()
    serializer_class = BaseEmailConfigSerializer


class BaseEmailConfigurationRetrieveUpdateDestroyView(GetObjectWithParamMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = BaseEmailConfigSerializer
    model_class = EmailConfiguration
    filterset_fields = ['id', 'restaurant']

    # def get_object(self):
    #     print(self.kwargs)
    #     restaurant_id = self.kwargs['restaurant']
    #     print(restaurant_id)

    #     obj = get_object_or_404(EmailConfiguration, restaurant=restaurant_id)
    #     print(obj)
    #     self.check_object_permissions(self.request, obj)
    #     return obj


class BaseStaffSendEmailAPI(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user
        sr = BaseStaffSendEmailSerializer(data=request.data)
        sr.is_valid(raise_exception=True)
        restaurant = get_object_or_404(
            Restaurant, id=sr.data.get("restaurant"))
        location = get_object_or_404(Location, id=sr.data.get("location"))
        subject = sr.data.get("subject")
        html_path = "email/staff.html"
        to_emails = sr.data.get("to_emails")
        code, obj = self.__get__code(request, user, restaurant, location)
        context = {
            "restaurant": restaurant, "location": location, "code": code
        }
        send_email(subject, html_path, context, to_emails, restaurant)
        self.__update_tracking__(obj, len(to_emails))
        return Response("email sent")

    def __update_tracking__(self, obj, num):
        obj.total += num
        obj.month += num
        obj.today += num
        obj.save()
        return

    def __get__code(self, request, user, restaurant, location):
        refer = Referral.objects.filter(user=user.id).first()
        if not refer:
            invite_code = self.get_invite_code(
                user.id, restaurant.id, location.id)
            referral_data = {
                "user": user.id,
                "restaurant": restaurant.id,
                "location": location.id,
                "invite_code": invite_code
            }

            _sr = BaseReferralSerializer(data=referral_data)
            _sr.is_valid(raise_exception=True)
            refer = _sr.save()

        code = refer.invite_code
        __staff_track_obj__ = self.__get_staff_refer_obj__(refer)
        return code, __staff_track_obj__

    def get_invite_code(self, user, restaurant, location):
        if not all([user, restaurant, location]):
            raise ValueError(
                "User, restaurant, and location must all be provided")

        def create_code():
            base_string = f"{user}{restaurant}{location}"
            salt = ''.join(random.choices(
                string.ascii_letters + string.digits, k=8))
            combined_string = base_string + salt
            hash_object = hashlib.sha256(combined_string.encode())
            code = hash_object.hexdigest()[:5]
            return code

        code = create_code()
        while Referral.objects.filter(invite_code=code).exists():
            code = create_code()
        return code

    def __get_staff_refer_obj__(self, refer):
        obj = StaffReferral.objects.filter(refer=refer.id).first()
        if not obj:
            obj = StaffReferral.objects.create(refer=refer)
        return obj


class BaseContactUsDataModelView(viewsets.ModelViewSet):
    serializer_class = BaseContactUsDataSerializer
    queryset = ContactUsData.objects.all()


class BaseDemoDataModelView(viewsets.ModelViewSet):
    serializer_class = BaseDemoDataSerializer
    queryset = DemoData.objects.all()

class BaseAutoReplyToCommentsDetailView(APIView):
    """
    API View to retrieve and update AutoReplyToComments settings.
    """

    def get(self, request, restaurant_id, location_id):
        """
        Retrieve AutoReplyToComments settings for a specific restaurant and location.
        """
        try:
            auto_reply_settings = AutoReplyToComments.objects.get(
                restaurant_id=restaurant_id, location_id=location_id
            )
            serializer = AutoReplyToCommentsSerializer(auto_reply_settings)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except AutoReplyToComments.DoesNotExist:
            return Response({"detail": "Settings not found."}, status=status.HTTP_404_NOT_FOUND)

    def patch(self, request, restaurant_id, location_id):
        """
        Update AutoReplyToComments settings for a specific restaurant and location.
        """
        try:
            auto_reply_settings = AutoReplyToComments.objects.get(
                restaurant_id=restaurant_id, location_id=location_id
            )
            serializer = AutoReplyToCommentsSerializer(auto_reply_settings, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except AutoReplyToComments.DoesNotExist:
            return Response({"detail": "Settings not found."}, status=status.HTTP_404_NOT_FOUND)
          
    def post(self, request):
        """
        Create new AutoReplyToComments settings.
        """
        serializer = AutoReplyToCommentsSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)