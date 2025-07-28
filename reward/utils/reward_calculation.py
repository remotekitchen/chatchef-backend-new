import datetime

from django.utils import timezone
from icecream import ic
from rest_framework.exceptions import ParseError, PermissionDenied

from core.utils import get_logger
from food.models import MenuItem

logger = get_logger()


class RewardCalculation:
    def apply_reward(
            self, order_list, subtotal, reward, order_method, delivery_fee, awarded_at, redeem=False, loyalty=False, bogo_amount=0, bxgy_amount=0
    ):
        from reward.models import Reward

        self.is_valid(
            order_list=order_list,
            reward=reward,
            subtotal=subtotal,
            order_method=order_method,
            awarded_at=awarded_at,
        )

        subtotal_for_discount, discount, limit = 0, 0, None

        if reward.reward_type == Reward.RewardType.REWARD_POINT:
            if redeem:
                self.handle_reward_points(reward.reward_points_worth)
            return discount, delivery_fee

        """
            For coupon type there's no condition, so subtotal is the original subtotal value
        """
        # if reward.reward_type == Reward.RewardType.COUPON:
        #     limit = 1 \
        #         if reward.limit_type == Reward.LimitType.ONE_DISH \
        #         else reward.max_dishes if reward.limit_type == Reward.LimitType.LIMITED \
        #         else limit

        items = 0

        """ If reward is bogo, initialize this with False """
        has_bogo_items = reward.reward_group != Reward.RewardType.BOGO
        for order_item_data in order_list:
            menu_item = order_item_data.get("menu_item")
            modifiers_data = order_item_data.get("modifiers")
            print('modifiers data --> ', modifiers_data)
            menu_item_instance: MenuItem = MenuItem.objects.get(id=menu_item)

            if reward.reward_type == Reward.RewardType.BOGO:
                """
                If reward type is bogo, check if the item/category is in the list and double the quantity
                """
                if (
                        reward.bogo_type == Reward.BogoType.ANY_DISH
                        or (
                        reward.bogo_type == Reward.BogoType.SELECTED_DISHES
                        and reward.items.filter(id=menu_item).exists())
                        or (
                        reward.bogo_type == Reward.BogoType.SELECTED_CATEGORY
                        and reward.categories.filter(id__in=menu_item_instance.category.all()).exists())
                ):
                    order_item_data["quantity"] = 2 * int(
                        order_item_data.get("quantity")
                    )
                    has_bogo_items = True

            else:
                """
                If reward type is not bogo, then check if the item in in the reward item list
                """
                if reward.reward_type == Reward.RewardType.COUPON or reward.items.filter(id=menu_item).exists():
                    quantity = int(
                        order_item_data.get("quantity")) if reward.offer_type != Reward.OfferType.FREE else 1
                    item_price = menu_item_instance.base_price
                    
                    if order_method == "delivery" and getattr(reward.restaurant, "use_delivery_inflation", False):
                        item_price = menu_item_instance.virtual_price


                    subtotal_for_discount += float(item_price) * quantity

                    items += 1
                    print("order value --> with out modifiers amount ",
                          subtotal_for_discount)

                    loyalty = False

                    if not loyalty:
                        for modifier_data in modifiers_data:
                            modifier_price = 0
                            modifier_quantity = modifier_data.get("quantity")
                            modifiers_items = modifier_data.get(
                                "modifiersItems")

                            for modifier_item in modifiers_items:
                                item_id = modifier_item.get(
                                    "modifiersOrderItems")
                                item_quantity = modifier_item.get("quantity")
                                menu_item_obj = MenuItem.objects.get(
                                    id=item_id)
                                modifier_item_price = menu_item_obj.base_price
                                # print(reward.restaurant, 'reward type --> ')
                                if (
                                    order_method == "delivery" and 
                                    getattr(reward, "restaurant", None) and 
                                    getattr(reward.restaurant, "use_delivery_inflation", False)
                                ):
                                    modifier_item_price = menu_item_obj.virtual_price

                                modifier_price = modifier_item_price * float(item_quantity)
                            subtotal_for_discount += (modifier_price * float(modifier_quantity)) * float(
                                quantity
                            )
                            print("order value --> with modifiers amount ",
                                  subtotal_for_discount)

            # if reward.reward_type == Reward.RewardType.COUPON and items >= limit:
            #     break
        subtotal_for_discount -= (bogo_amount + bxgy_amount)
        ic(subtotal_for_discount)
        """
            If discount amount is 0 or no bogo item's in cart, raise error.
        """
        if (reward.reward_type != Reward.RewardType.BOGO and subtotal_for_discount == 0) or not has_bogo_items:
            raise ParseError('No reward item found in the cart!')

        if reward.reward_type != Reward.RewardType.BOGO:
            """
                Calculate discount according to the offer_type
            """
            discount = (
                reward.amount
                if reward.offer_type == Reward.OfferType.FLAT
                else subtotal_for_discount
                if reward.offer_type == Reward.OfferType.FREE
                else (subtotal_for_discount * (float(reward.amount) / 100))
            )

        """
            Calculate reward points for restaurant user
        """
        print('Discount amount in reward calculation 134 --> ', discount)
        try:
            if reward.reward_points_worth > 0:
                self.handle_reward_points(reward=reward)
        except:
            logger.error('reward point update error')

        fee = delivery_fee - (reward.delivery_discount / 100)
        return discount, fee

    def handle_reward_points(self, reward):
        from accounts.models import RestaurantUser
        restaurant = reward.reward_group.restaurant
        restaurant_user = RestaurantUser.objects.get(restaurant=restaurant)
        restaurant_user.reward_points += (
            reward.reward_points_worth * restaurant.reward_point_equivalent)
        restaurant_user.save(update_fields=['reward_points'])

    def is_valid(self, order_list, reward, subtotal, order_method, awarded_at):
        from reward.models import AdditionalCondition, RewardGroup

        """
            Checks if the reward group is valid for applying
        """
        reward_group: RewardGroup = reward.reward_group
        if order_method not in reward_group.applies_for:
            raise PermissionDenied(
                f"This reward does not apply for {order_method}!")

        current_dt = timezone.now()

        # Handle validity time
        # if reward_group.validity_type != RewardGroup.ValidityType.UNLIMITED:
        #     expiry_date = (
        #         reward_group.validity_date
        #         if reward_group.validity_type == RewardGroup.ValidityType.SPECIFIC_DATE
        #         else awarded_at.date() + datetime.timedelta(days=reward_group.validity_days)
        #     )
        #     if current_dt.date() > expiry_date:
        #         raise PermissionDenied("The reward is expired!")

        # Handle additional conditions
        additional_conditions = reward_group.additionalcondition_set.all()
        for condition in additional_conditions:
            # Minimum amount of order required
            if (
                    condition.condition_type
                    == AdditionalCondition.ConditionType.MINIMUM_AMOUNT
                    and condition.amount > subtotal
            ):
                raise PermissionDenied(
                    f"Minimum order amount is {condition.amount}")

            # A specific item required to be in the cart
            if (
                    condition.condition_type
                    == AdditionalCondition.ConditionType.SPECIFIC_ITEM_IN_CART
            ):
                items = list(condition.items.values_list("id", flat=True))
                for order in order_list:
                    item = order.get("menu_item", None)
                    if item is not None and item in items:
                        items.pop(item)
                if len(items) != 0:
                    raise PermissionDenied(
                        "Some items are required to order to redeem this reward!"
                    )

            # Check if current time is between the time range
            if (
                    condition.condition_type
                    == AdditionalCondition.ConditionType.TIME_OF_DAY
            ):
                current_time = current_dt.time()
                if condition.end_time >= current_time >= condition.start_time:
                    raise PermissionDenied(
                        f"This reward has to be used between {condition.start_time} and {condition.end_time}"
                    )
