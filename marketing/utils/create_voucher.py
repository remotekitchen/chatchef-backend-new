from marketing.models import RewardGroup, Voucher
from reward.models import Reward, UserReward
from reward.api.base.serializers import BaseRewardGroupSerializer
from django.utils import timezone
from django.core.exceptions import PermissionDenied
from food.models import Restaurant
from django.db.models import Q
import random


def create_reward_group_and_user_reward(restaurant_id, location_id, user_id, voucher_amount):
    """
    Create a reward group, reward, and assign it to a specific user via UserReward.
    """
    # Step 1: Create Reward Group
    reward_group_data = {
        "reward_set": [
            {
                "reward_type": "coupon",
                "reward_points_worth": 0,
                "offer_type": "percentage",
                "bogo_type": "any_dish",
                "amount": voucher_amount,
                "min_dishes": 0,
                "max_dishes": 0,
                "is_free_delivery": False,
                "restaurant": restaurant_id,
                "items": [],
                "categories": [],
            }
        ],
        "additionalcondition_set": [
            {
                "condition_type": "minimum_amount",
                "amount": 0,
                "start_time": None,
                "end_time": None,
                "items": [],
            }
        ],
        "name": f"{voucher_amount}% off",
        "description": f"Get {voucher_amount}% off on your order",
        "applies_for": ["delivery", "pickup"],
        "validity_type": "unlimited",
        "validity_days": 0,
        "validity_date": None,
        "restaurant": restaurant_id,
        "location": location_id,
    }

    reward_group_serializer = BaseRewardGroupSerializer(data=reward_group_data)
    reward_group_serializer.is_valid(raise_exception=True)
    reward_group = reward_group_serializer.save()

    # Step 2: Create Reward
    reward = Reward.objects.create(
        reward_group=reward_group,
        reward_type="coupon",
        offer_type="percentage",
        amount=voucher_amount,
        restaurant_id=restaurant_id,
    )

    # Step 3: Assign Reward to User via UserReward
    user_reward = UserReward.objects.create(
        user_id=user_id,
        restaurant_id=restaurant_id,
        location_id=location_id,
        reward=reward,
        reward_type="coupon",
        amount=voucher_amount,
        #  code = combination of ENJOY + voucher_amount
        code = f"ENJOY{voucher_amount}",
    )

    
    return reward_group, reward, user_reward


# from django.utils.text import slugify

# def create_user_rewards_for_audience(user):
#     """
#     Automatically create 6 UserReward coupons (and matching Vouchers) totalling 600 BDT for the user under 'ALL' audience.
#     """
#     audience = UserReward.Audience.ALL
#     voucher_amounts = [200, 100, 100, 75, 75, 50]
#     created_rewards = []

#     # Define minimum spend for each amount
#     minimum_spends = {
#         50: 167,
#         75: 250,
#         100: 333,
#         200: 667,
#     }

#     for voucher_amount in voucher_amounts:
#         min_spend = minimum_spends.get(voucher_amount, 0)

#         # 1. Create Reward Group
#         reward_group_data = {
#             "reward_set": [
#                 {
#                     "reward_type": "coupon",
#                     "reward_points_worth": 0,
#                     "offer_type": "flat",
#                     "bogo_type": "any_dish",
#                     "amount": voucher_amount,
#                     "min_dishes": 0,
#                     "max_dishes": 0,
#                     "is_free_delivery": False,
#                     "items": [],
#                     "categories": [],
#                 }
#             ],
#             "additionalcondition_set": [
#                 {
#                     "condition_type": "minimum_amount",
#                     "amount": min_spend,
#                     "start_time": None,
#                     "end_time": None,
#                     "items": [],
#                 }
#             ],
#             "name": f"{voucher_amount}tk off for all users",
#             "description": f"Get {voucher_amount}tk off your order",
#             "applies_for": ["delivery", "pickup"],
#             "validity_type": "unlimited",
#             "validity_days": 0,
#             "validity_date": None,
#         }

#         reward_group_serializer = BaseRewardGroupSerializer(data=reward_group_data)
#         reward_group_serializer.is_valid(raise_exception=True)
#         reward_group = reward_group_serializer.save()

#         # 2. Create Reward
#         reward = Reward.objects.create(
#             reward_group=reward_group,
#             reward_type="coupon",
#             offer_type="flat",
#             amount=voucher_amount,
#         )

#         # 3. Create UserReward
#         code = f"HUNGRYTIGER{voucher_amount}"
#         user_reward = UserReward.objects.create(
#             user=user,
#             reward=reward,
#             reward_type="coupon",
#             amount=voucher_amount,
#             audience=audience,
#             code=code,
#             platform="remotekitchen",
#         )

#         # 4. Create matching Voucher object
#         Voucher.objects.create(
#             reward=reward,
#             voucher_code=code,
#             amount=voucher_amount,
#             minimum_spend=min_spend,
#             max_redeem_value=voucher_amount,
#             available_to=Voucher.Audience.ALL,
#             is_one_time_use=True,
#             name=slugify(code),
#         )

#         created_rewards.append(user_reward)

#     return created_rewards
