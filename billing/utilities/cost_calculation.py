from typing import List
from math import floor
from django.utils import timezone
from rest_framework.exceptions import ParseError, PermissionDenied
from rest_framework.generics import get_object_or_404

from billing.models import DeliveryFeeAssociation, OrderItem, RestaurantFee
from core.utils import get_logger
from food.models import Location, MenuItem, Restaurant
from marketing.models import Bogo, BxGy, SpendXSaveY, Voucher
from reward.models import UserReward
from remotekitchen.utils import get_delivery_fee_rule
from django.db.models import Case, When, Value, IntegerField
from django.db.models import Q
from billing.models import Order
from rest_framework.exceptions import NotAuthenticated


from reward.models import RewardGroup


logger = get_logger()


class CostCalculation:
    def get_updated_cost(
            self,
            order_list: List,
            delivery=0,
            voucher=None,
            location=None,
            order_method="delivery",
            spend_x_save_y=None,
            is_bag=False,
            is_utensil=0,
            tips_for_restaurant=0,
            bogo_id=None,
            bxgy_id=None,
            user=None,
            redeem=False,
            delivery_platform="uber",
            on_time_guarantee_opted_in=False,
            order=None
    
    ):
        print(user, 'user------------>')
        """
        Will calculate item total, tax, service charge, total cost of a list of order items
        :param order_list: List of order item dictionaries
        :return: Dictionary of difference costs
        :param delivery: Default delivery fee which might be replaced
        :param voucher: Voucher code if applied
        :param location: ID of location
        :param order_method: delivery, pickup, dine_in etc
        :param spend_x_save_y: id of spend x save y campaign
        """

        is_delivery_fess_pay_by_user = False

        # Since all items are from a single restaurant, we are getting the first item's restaurant
        menu_item = get_object_or_404(
            MenuItem, id=order_list[0].get("menu_item")
        )
        restaurant_id = menu_item.restaurant.id
        restaurant = menu_item.restaurant
        try:
            location = get_object_or_404(Location, id=location)
        except:
            location = None
        order_value, alcoholic_order_value, total, tax, delivery_fee, convenience_fee, discount, item_cnt = (
            0,
            0,
            0,
            0,
            delivery,
            0,
            0,
            0,
        )
        print(delivery_fee, 'delivery_fee-------->100')

        
        applied_voucher_with_bogo = False
        applied_voucher_with_bxgy = False

        if voucher:
            voucher_obj = Voucher.objects.filter(
                voucher_code=voucher
            ).filter(
                Q(restaurant=restaurant) | Q(restaurant__isnull=True)
            ).annotate(
                priority=Case(
                    When(restaurant=restaurant, then=Value(0)),
                    default=Value(1),
                    output_field=IntegerField(),
                )
            ).order_by('priority').first()
            print(voucher_obj, 'voucher_obj--------------->')
            
            if voucher_obj and voucher_obj.available_to in [
                Voucher.Audience.FIRST_ORDER,
                Voucher.Audience.SECOND_ORDER,
                Voucher.Audience.THIRD_ORDER
            ]:
                if restaurant.voucher_restriction:
                    applied_voucher_with_bogo = True
                if restaurant.voucher_restriction:
                    applied_voucher_with_bxgy = True

        # Ensure only one coupon type is applied at a time
        if (
            (bogo_id and voucher and not applied_voucher_with_bogo) or 
            (bogo_id and spend_x_save_y) or 
            (bxgy_id and voucher and not applied_voucher_with_bxgy) or 
            (bxgy_id and spend_x_save_y) or 
            (bogo_id and bxgy_id)
        ):
            raise ParseError('Only one coupon can be applied at a time')
        
        # Inject platform-specific delivery fee logic for raider_app only
        if delivery_platform == "raider_app":
            try:
                distance_km = float(location.distance)  # assuming distance is stored in km in Location
                if distance_km <= 3:
                    delivery_fee = 0
                else:
                    delivery_fee = 25 + int((distance_km - 3) * 12)
                print(f"Calculated delivery_fee for raider_app: {delivery_fee} Tk")
            except (TypeError, ValueError, AttributeError):
                print("⚠️ Please enter a valid distance")
                # Allow form to continue with default delivery_fee


        if delivery_platform != "raider_app" and delivery_fee > 12:
            raise ParseError(
                f"we can not delivery to this addressSSSSSSSSS")

        bogo = None
        bxgy = None
        if bxgy_id is not None:
            bxgy = BxGy.objects.filter(id=bxgy_id, location=location).first()
            if bxgy is None:
                raise ParseError('This bxgy does not exist for this location')
              
        if bogo_id is not None:
            bogo = Bogo.objects.filter(id=bogo_id, location=location).first()
            if bogo is None:
                raise ParseError('This bogo does not exist for this location')

        bogo_amount = 0
        bxgy_amount = 0
        # calculating order_value from order_item_data and modifiers
        for order_item_data in order_list:
            order_item_id = order_item_data.get("menu_item")
            quantity = int(order_item_data.get("quantity"))
            modifiers_data = order_item_data.get("modifiers")

            try:
                menu_item_instance: MenuItem = MenuItem.objects.get(
                    id=order_item_id
                )
                # discounted_price = float(menu_item_instance.discounted_price)
                # item_price =  discounted_price if discounted_price > 0 else menu_item_instance.base_price 
                item_price = menu_item_instance.base_price
                print("result90",menu_item_instance.discounted_price, item_price)
                if order_method == "delivery" and restaurant.use_delivery_inflation:
                    item_price = menu_item_instance.virtual_price
                print(bogo, 'bogo-------------->')
                print("Current Menu Item:", menu_item_instance, item_price, quantity)
                if bogo is not None and menu_item_instance in bogo.items.all():
                    bogo_amount += item_price * float(quantity)
                if bxgy is not None and menu_item_instance in bxgy.items.all():
                    bxgy_amount += item_price * float(quantity)

                    for modifier_data in modifiers_data:
                        modifier_price = 0
                        modifier_quantity = modifier_data.get("quantity")
                        modifiers_items = modifier_data.get("modifiersItems")

                        for modifier_item in modifiers_items:
                            item_id = modifier_item.get("modifiersOrderItems")
                            item_quantity = modifier_item.get("quantity")
                            menu_item_obj = MenuItem.objects.get(id=item_id)

                            modifier_item_price = menu_item_obj.base_price
                            if order_method == "delivery" and restaurant.use_delivery_inflation:
                                modifier_item_price = menu_item_obj.virtual_price
                            modifier_price = modifier_item_price * \
                                float(item_quantity)

                        bogo_amount += (modifier_price * float(modifier_quantity)) * float(
                            quantity
                        )
                        bxgy_amount += (modifier_price * float(modifier_quantity)) * float(
                            quantity
                        )

                if not menu_item_instance.is_alcoholic:
                    order_value += item_price * float(quantity)
                    item_cnt += quantity
                else:
                    alcoholic_order_value += item_price * float(quantity)
                    item_cnt += quantity

                for modifier_data in modifiers_data:
                    modifier_price = 0
                    modifier_quantity = modifier_data.get("quantity")
                    modifiers_items = modifier_data.get("modifiersItems")

                    for modifier_item in modifiers_items:
                        item_id = modifier_item.get("modifiersOrderItems")
                        item_quantity = modifier_item.get("quantity")
                        menu_item_obj = MenuItem.objects.get(id=item_id)

                        modifier_item_price = menu_item_obj.base_price
                        if order_method == "delivery" and restaurant.use_delivery_inflation:
                            modifier_item_price = menu_item_obj.virtual_price

                        modifier_price = modifier_item_price * \
                            float(item_quantity)
                    order_value += (modifier_price * float(modifier_quantity)) * float(
                        quantity
                    )

            except Exception as e:
                logger.error(f"{e}")

        # Calculating offers
        original_order_value, voucher_discount = order_value, 0
        original_order_value += alcoholic_order_value
        current_order_value = original_order_value
        print("original_order_value --> ", original_order_value)
        if spend_x_save_y is not None:
            current_order_value, discount = self.calculate_spend_x_save_y(
                order_value=current_order_value,
                restaurant_id=restaurant_id,
                location=location,
                spend_x_save_y=spend_x_save_y,
            )

        # delivery_free = False
        print("voucher --> ", voucher, delivery_fee)
        if voucher is not None and voucher != "":
            current_order_value, amount_off, delivery_fee, is_delivery_fess_pay_by_user = self.apply_voucher(
                subtotal=current_order_value,
                voucher=voucher,
                location=location,
                order_list=order_list,
                order_method=order_method,
                delivery_fee=delivery_fee,
                user=user,
                redeem=redeem,
                bogo_amount=bogo_amount,
                bxgy_amount=bxgy_amount
            )
            discount += amount_off
            voucher_discount = amount_off
            print('amount_off --> ', amount_off)
        
        order_items = OrderItem.objects.filter(
            order__location=location,
            order__status__in=["pending", "accepted"],
            menu_item__restaurant=restaurant
        )
        
        bxgy_campaign = BxGy.objects.filter(
            location=location,
            id=bxgy_id
        ).first()
      

        bogo_discount = bogo_amount / 2
        bxgy_discount = calculate_bxgy_discount(order_items, bxgy_campaign)
        current_order_value -= (bogo_discount + bxgy_discount)
        discount += (bogo_discount + bxgy_discount) 
        
        print(bogo_discount, bogo_amount, current_order_value, 'bogo_discount------------------->')

        total = current_order_value - alcoholic_order_value
        tax_percentage = 0
        alcoholic_tax_rate = 0
        print(delivery_fee, 'delivery_fee----->')

        # TODO Clean up code and reconsider tax calculation
        try:
            delivery_fee_association = DeliveryFeeAssociation.objects.get(
                restaurant_id=restaurant_id
            )

            if order_method != 'pickup':
                convenience_fee = delivery_fee_association.convenience_fee
            if delivery_fee_association.use_tax:
                tax_percentage = delivery_fee_association.tax_rate
                alcoholic_tax_rate = delivery_fee_association.alcoholic_tax_rate
        except:
            pass
          
        print("restaurant.remotekitchen --> ", restaurant.is_remote_Kitchen)

        if restaurant.is_remote_Kitchen:  # Assuming this indicates a Bangladeshi restaurant
            distance = 5 if delivery_fee < 70 else 10
        else:  # For Canadian restaurants
            distance = 5 if delivery_fee < 8 else 10
        restaurant_fee = RestaurantFee.objects.filter(
            restaurant=restaurant_id, max_distance= distance
        ).first()
        print('restaurant_fee --> ', restaurant, distance)
        # print(restaurant_fee.delivery_fee, 'restaurant_fee')

        

        delivery_fee_rule = int(get_delivery_fee_rule(user, restaurant))
        delivery_fee += delivery_fee_rule
        original_delivery_fee, discounted_delivery_fee, delivery_discount = restaurant_fee.delivery_fee if restaurant.is_remote_Kitchen else delivery_fee, delivery_fee, 0
        if restaurant_fee is not None and not is_delivery_fess_pay_by_user:
            if order_method == 'delivery':
                convenience_fee = restaurant_fee.service_fee
                discounted_delivery_fee = restaurant_fee.delivery_fee
                delivery_discount = delivery_fee - discounted_delivery_fee

        if delivery_platform == "raider_app" and discounted_delivery_fee == 0:
            discounted_delivery_fee = delivery_fee

        reward_amount_applied = 0
        unclaimed_reward = None

        if user and user.is_authenticated:
            reward_group = RewardGroup.objects.filter(name="On-Time Delivery Guarantee").first()

            if reward_group:
                unclaimed_reward = UserReward.objects.filter(
                    user=user,
                    reward__reward_group=reward_group,
                    is_claimed=False,
                    expiry_date__gte=timezone.now().date()
                ).order_by("created_date").first()

                if unclaimed_reward:
                    reward_amount_applied = float(unclaimed_reward.amount or 0)
                    print(f"✅ Auto-applying on-time reward: -{reward_amount_applied} Tk")



        total += discounted_delivery_fee + convenience_fee
        tax = self.round_decimal(original_order_value * (tax_percentage / 100))
        alcoholic_tax = self.round_decimal(
            alcoholic_order_value * (alcoholic_tax_rate / 100)
        )
        tax += alcoholic_tax
        total += tax
        total += alcoholic_order_value
        total = self.calculate_stripe_fee(total=total, restaurant=restaurant)

        charges = {
            "total": self.round_decimal(total),
            "original_order_value": self.round_decimal(original_order_value),
            "order_value": self.round_decimal((order_value + alcoholic_order_value)),
            "tax": self.round_decimal(tax),
            "tax_without_alcohol": self.round_decimal((tax - alcoholic_tax)),
            "alcohol_tax": self.round_decimal(alcoholic_tax),
            "delivery_fee": self.round_decimal(delivery_fee),
            "convenience_fee": self.round_decimal(convenience_fee),
            "discount": self.round_decimal(discount),
            "voucher_discount": self.round_decimal(voucher_discount),
            "quantity": item_cnt,
            "order_list": order_list,
            "bag_price": 0,
            "utensil_price": 0,
            "tips_for_restaurant": tips_for_restaurant,
            "original_delivery_fee": self.round_decimal(original_delivery_fee),
            "discounted_delivery_fee": self.round_decimal(discounted_delivery_fee),
            "delivery_discount": self.round_decimal(delivery_discount),
            "bogo_discount": self.round_decimal(bogo_discount),
            "bxgy_discount": self.round_decimal(bxgy_discount),
            "used_on_time_reward": 0
        }

        # ✅ Now it's safe to modify `charges`
        if on_time_guarantee_opted_in:
            if order and getattr(order, "on_time_guarantee_fee", None):
                on_time_guarantee_fee = order.on_time_guarantee_fee
            else:
                on_time_guarantee_fee = getattr(restaurant, "on_time_guarantee_fee", 6)

            charges["on_time_guarantee_fee"] = self.round_decimal(on_time_guarantee_fee)
            total += on_time_guarantee_fee
            charges["total"] = self.round_decimal(total)
        else:
            charges["on_time_guarantee_fee"] = 0


                # ✅ Apply On-Time Reward here
        if reward_amount_applied > 0:
            total -= reward_amount_applied
            charges["used_on_time_reward"] = self.round_decimal(reward_amount_applied)
            charges["total"] = self.round_decimal(total)

            
        if is_bag or (order_method == "delivery" and restaurant.use_bag_price_on_delivery):
            print("total without bag --> ", charges["total"])
            bag_price = restaurant.bag_price
            charges["bag_price"] = bag_price
            total += bag_price
            charges["total"] = self.round_decimal(total)
            print("user want bag, bag price --> ", charges["bag_price"])
            print("total with bag --> ", charges["total"])

        else:
            print('bag is free for this restaurant')

        print("utensil --> ", is_utensil)
        if is_utensil:
            counts = float(is_utensil)
            utensil_price = self.round_decimal(
                restaurant.utensil_price * counts
            )
            charges["utensil_price"] = utensil_price
            total += utensil_price
            charges["total"] = self.round_decimal(total)
            print(
                "user want utensil, utensil price --> ",
                charges["utensil_price"]
            )
            print("total with utensil --> ", charges["total"])

        if charges["tips_for_restaurant"]:
            total += charges["tips_for_restaurant"]
            charges["total"] = self.round_decimal(total)
            print("customer give tips to restaurant --> ", charges["total"])
            

        return charges

    def calculate_stripe_fee(self, total, restaurant):
        if restaurant.stripe_fee:
            stripe_fees = (total * (2.9 / 100)) + 0.30
            stripe_fees = float("{0:.2f}".format(stripe_fees))
            total += stripe_fees
        return total

    def get_updated_cost_backup(
            self,
            order_list: List,
            delivery=0,
            voucher=None,
            location=None,
            order_method="delivery",
            spend_x_save_y=None,
            is_bag=False,
            is_utensil=0,
            tips_for_restaurant=0,
    ):
        """
        Will calculate item total, tax, service charge, total cost of a list of order items
        :param order_list: List of order item dictionaries
        :return: Dictionary of difference costs
        :param delivery: Default delivery fee which might be replaced
        :param voucher: Voucher code if applied
        :param location: ID of location
        :param order_method: delivery, pickup, dine_in etc
        :param spend_x_save_y: id of spend x save y campaign
        """
        print(order_method)
        # Since all items are from a single restaurant, we are getting the first item's restaurant
        menu_item = get_object_or_404(
            MenuItem, id=order_list[0].get("menu_item")
        )
        restaurant_id = menu_item.restaurant.id
        restaurant = menu_item.restaurant
        try:
            location = get_object_or_404(Location, id=location)
        except:
            location = None
        order_value, total, tax, delivery_fee, convenience_fee, discount, item_cnt = (
            0,
            0,
            0,
            delivery,
            0,
            0,
            0,
        )

        # calculating order_value from order_item_data and modifiers
        for order_item_data in order_list:
            order_item_id = order_item_data.get("menu_item")
            quantity = int(order_item_data.get("quantity"))
            print(order_item_id, quantity)
            modifiers_data = order_item_data.get("modifiers")

            try:
                menu_item_instance: MenuItem = MenuItem.objects.get(
                    id=order_item_id
                )

                print(menu_item_instance.base_price * float(quantity))
                order_value += menu_item_instance.base_price * float(quantity)
                item_cnt += quantity

                for modifier_data in modifiers_data:
                    modifier_price = 0
                    modifier_quantity = modifier_data.get("quantity")
                    modifiers_items = modifier_data.get("modifiersItems")

                    for modifier_item in modifiers_items:
                        item_id = modifier_item.get("modifiersOrderItems")
                        item_quantity = modifier_item.get("quantity")
                        menu_item_obj = MenuItem.objects.get(id=item_id)
                        modifier_price = menu_item_obj.base_price * \
                            float(item_quantity)
                    order_value += (modifier_price * float(modifier_quantity)) * float(
                        quantity
                    )
                    print("order value --> ", order_value)

            except Exception as e:
                logger.error(f"{e}")

        # Calculating offers
        original_order_value, voucher_discount = order_value, 0
        if spend_x_save_y is not None:
            order_value, discount = self.calculate_spend_x_save_y(
                order_value=order_value,
                restaurant_id=restaurant_id,
                location=location,
                spend_x_save_y=spend_x_save_y,
            )

        # delivery_free = False
        if voucher is not None and voucher != "":
            order_value, amount_off, delivery_fee = self.apply_voucher(
                subtotal=order_value,
                voucher=voucher,
                location=location,
                order_list=order_list,
                order_method=order_method,
                delivery_fee=delivery_fee,
            )
            discount += amount_off
            voucher_discount = amount_off

        total = order_value
        tax_percentage = 0

        try:
            delivery_fee_association = DeliveryFeeAssociation.objects.get(
                restaurant_id=restaurant_id
            )

            # if (
            #         order_method == "delivery"
            #         and delivery_fee_association.delivery_fee is not None
            #         and delivery_fee_association.delivery_fee != 0
            # ):
            #     delivery_fee = delivery_fee_association.delivery_fee
            if order_method != 'pickup':
                convenience_fee = delivery_fee_association.convenience_fee
            if delivery_fee_association.use_tax:
                tax_percentage = delivery_fee_association.tax_rate
        except:
            pass

        print("tax_percentage", tax_percentage)

        logger.info(f"{total} {order_value}")
        
        if restaurant.is_remote_Kitchen:  # Assuming this indicates a Bangladeshi restaurant
            distance = 5 if delivery_fee < 50 else 10
        else:  # For Canadian restaurants
            distance = 5 if delivery_fee < 8 else 10
        restaurant_fee = RestaurantFee.objects.filter(
            restaurant=restaurant, max_distance=distance
        ).first()
        original_delivery_fee, discounted_delivery_fee, delivery_discount = delivery_fee, delivery_fee, 0
        if restaurant_fee is not None:
            print(order_method, restaurant_fee.delivery_fee)
            if order_method == 'delivery':
                convenience_fee = restaurant_fee.service_fee
                discounted_delivery_fee = restaurant_fee.delivery_fee
                delivery_discount = delivery_fee - discounted_delivery_fee

        total += discounted_delivery_fee + convenience_fee
        tax = self.round_decimal(total * (tax_percentage / 100))
        total += tax

        charges = {
            "total": self.round_decimal(total),
            "original_order_value": self.round_decimal(original_order_value),
            "order_value": self.round_decimal(order_value),
            "tax": self.round_decimal(tax),
            "delivery_fee": self.round_decimal(delivery_fee),
            "convenience_fee": self.round_decimal(convenience_fee),
            "discount": self.round_decimal(discount),
            "voucher_discount": self.round_decimal(voucher_discount),
            "quantity": item_cnt,
            "order_list": order_list,
            "bag_price": 0,
            "utensil_price": 0,
            "tips_for_restaurant": tips_for_restaurant,
            "original_delivery_fee": self.round_decimal(original_delivery_fee),
            "discounted_delivery_fee": self.round_decimal(discounted_delivery_fee),
            "delivery_discount": self.round_decimal(delivery_discount)
        }

        # if delivery_fee > 0:
        #     print("delivery order")
        #     charges["original_delivery_fee"] = delivery_fee
        #     _delivery_discount = self.delivery_fees(total, delivery_fee)
        #     total += _delivery_discount["delivery_charge"] + convenience_fee
        #     tax = total * (tax_percentage / 100)
        #     total += tax
        #     charges["total"] = self.round_decimal(total)
        #     charges["tax"] = self.round_decimal(tax)
        #
        #     if delivery_fee > self.round_decimal(_delivery_discount["delivery_charge"]):
        #         charges["discounted_delivery_fee"] = self.round_decimal(
        #             _delivery_discount["delivery_charge"]
        #         )
        #         charges["delivery_discount"] = _delivery_discount["delivery_discount"]

        if is_bag or (order_method == "delivery" and restaurant.use_bag_price_on_delivery):
            print("total without bag --> ", charges["total"])
            bag_price = restaurant.bag_price
            charges["bag_price"] = bag_price
            total += bag_price
            charges["total"] = self.round_decimal(total)
            print("user want bag, bag price --> ", charges["bag_price"])
            print("total with bag --> ", charges["total"])

        else:
            print('bag is free for this restaurant')

        print("utensil --> ", is_utensil)
        if is_utensil:
            counts = float(is_utensil)
            utensil_price = self.round_decimal(
                restaurant.utensil_price * counts
            )
            charges["utensil_price"] = utensil_price
            total += utensil_price
            charges["total"] = self.round_decimal(total)
            print(
                "user want utensil, utensil price --> ",
                charges["utensil_price"]
            )
            print("total with utensil --> ", charges["total"])

        if charges["tips_for_restaurant"]:
            total += charges["tips_for_restaurant"]
            charges["total"] = self.round_decimal(total)
            print("customer give tips to restaurant --> ", charges["total"])

        return charges
      
    def apply_user_reward(self, user, user_reward):
        """
        Apply the UserReward for the given user, enforcing audience order.
        """
        # Check if the reward belongs to the user
        print(user_reward.user, user, user_reward, 'user_reward.user, user') 
        if user_reward.user != user:
            raise PermissionDenied("This reward does not belong to the current user.")

        # Enforce audience order rules
        if user_reward.audience == UserReward.Audience.SECOND_ORDER:
            first_order_reward = UserReward.objects.filter(
                user=user,
                audience=UserReward.Audience.FIRST_ORDER,
                is_claimed=True
            ).exists()
            if not first_order_reward:
                raise PermissionDenied("You must use the first-order reward before using the second-order reward.")

        if user_reward.audience == UserReward.Audience.THIRD_ORDER:
            second_order_reward = UserReward.objects.filter(
                user=user,
                audience=UserReward.Audience.SECOND_ORDER,
                is_claimed=True
            ).exists()
            if not second_order_reward:
                raise PermissionDenied("You must use the first-order and second-order reward before using the third-order reward.")

        # Mark the reward as claimed
        if user_reward.is_claimed:
            raise PermissionDenied("Reward already claimed!")

        # user_reward.is_claimed = True
        # user_reward.save(update_fields=["is_claimed"])
        return user_reward


    def apply_voucher(
            self, subtotal, voucher, location, order_list, order_method, delivery_fee, user=None, redeem=False, bogo_amount=0, bxgy_amount=0
    ):
        """
        Apply a voucher or UserReward for a specific user and location.
        """

         # 🚀 NEW: Check if user is authenticated
        if not user or not user.is_authenticated:
            raise NotAuthenticated("You must be logged in to use a voucher.")
        
        discount = 0
        is_delivery_fees_pay_by_user = False
        fee = delivery_fee

        # Ensure user is authenticated
        # if not user or not user.is_authenticated:
        #     raise PermissionDenied("You must be logged in to use this reward.")

        # Retrieve the UserReward
        user_rewards = UserReward.objects.filter(code=voucher, user=user)
        print(user_rewards, 'user_rewards')

        if user_rewards.exists():
            # You can either pick the first one or handle all
            user_reward = user_rewards.first()  # or loop through all if needed
        else:
            user_reward = None

        if user_reward:
            try:
                # Apply the user reward, enforcing audience rules
                self.apply_user_reward(user, user_reward)

                # Check if the associated restaurant is a remote kitchen
                is_remote_kitchen = False
                accept_first_second_third_user_reward = False
                if location and location.restaurant:
                    restaurant = location.restaurant
                    is_remote_kitchen = restaurant.is_remote_Kitchen
                    accept_first_second_third_user_reward = restaurant.accept_first_second_third_user_reward

                print("is_remote_kitchen: ", is_remote_kitchen)
                print("accept_first_second_third_user_reward: ", accept_first_second_third_user_reward)


                # Validate location for non-remote kitchens
                if not is_remote_kitchen and location and user_reward.restaurant != location.restaurant:
                    raise PermissionDenied("This reward is not valid for this location!")
                
                print('its calling-------------------')
                # if not accept_first_second_third_user_reward then can not use first, second, third order for that specific restaurant
                # if not accept_first_second_third_user_reward:
                #     raise PermissionDenied("This reward is not valid for this location!")

                # Apply the reward and calculate discount
                discount, fee = user_reward.apply_reward(
                    order_list, subtotal, order_method, delivery_fee, redeem=redeem, loyalty=True
                )
                is_delivery_fees_pay_by_user = True
                
                print('its calling-------------------')

            except Exception as e:
                print('error --> ', e)
                raise e
            
            if not user_reward:
                print(f"[Voucher fallback] No user reward found for user={user} and code={voucher}")
        else:
            """
            Standard restaurant or location-based voucher
            """
            logger.info("Checking for standard voucher...")
            obj = Voucher.objects.filter(
                voucher_code=voucher
            ).filter(
                Q(location=location) | Q(location__isnull=True)
            ).annotate(
                priority=Case(
                    When(location=location, then=Value(0)),
                    default=Value(1),
                    output_field=IntegerField()
                )
            ).order_by('priority').first()
            print(obj, voucher, location, 'obj------------------->')
            if obj is None:
                raise ParseError(
                    "Voucher with this code does not exist for this location!"
                )

            discount, fee = obj.apply_reward(
                order_list, subtotal, order_method, delivery_fee, restaurant=location.restaurant, user=user,
                redeem=redeem,
                bogo_amount=bogo_amount,
                bxgy_amount=bxgy_amount
            )
            # Check and cap discount at max_redeem_value
            max_redeem_value = obj.max_redeem_value if obj and obj.max_redeem_value is not None else float("inf")
            if discount > max_redeem_value:
                discount = max_redeem_value

        return subtotal - discount, discount, fee, is_delivery_fees_pay_by_user
      
    

    def round_decimal(self, x):
        return float("{0:.2f}".format(x))

    def calculate_spend_x_save_y(
            self, order_value, restaurant_id, spend_x_save_y, location=None
    ):
        current_time = timezone.now()
        campaign: SpendXSaveY = (
            SpendXSaveY.objects.filter(
                id=spend_x_save_y,
                restaurant_id=restaurant_id,
                location=location,
                # promo_option__min_spend__lte=order_value,
                durations__start_date__lte=current_time,
                durations__end_date__gte=current_time,
            )
            # .order_by("-promo_option__min_spend")
            .first()
        )

        discount = 0
        if campaign is None:
            raise PermissionDenied(
                "No active spend x save y campaign found for this!"
            )
            # return order_value, discount
        if campaign.min_spend > order_value:
            raise PermissionDenied(
                f"Minimum {campaign.min_spend} is required to be ordered for this campaign"
            )
        discount = campaign.save_amount
        order_value = order_value - discount
        return order_value, discount

    def delivery_fees(self, order_subtotal, delivery_charge):
        discount_dict = {25: 40, 30: 50, 40: 75, 50: 80, 60: 100}

        discount_percentage = 0
        for index, (threshold, percentage) in enumerate(discount_dict.items()):
            if order_subtotal >= threshold:
                discount_percentage = percentage

        delivery_discount = delivery_charge * (discount_percentage / 100)
        delivery_charge_after_discount = delivery_charge - delivery_discount

        print("delivery charge --> ", delivery_charge)
        print("delivery discount --> ", delivery_discount)
        print(
            "delivery charge after discount --> ",
            delivery_charge_after_discount
        )
        print("discount percent --> ", discount_percentage)
        print("order total --> ", order_subtotal)

        return {
            "delivery_charge": delivery_charge_after_discount,
            "delivery_discount": delivery_discount,
        }

    def delivery_fees_old(self, order_subtotal, delivery_charge):
        """

        #! NOT IN USE

        """
        print("delivery fee", delivery_charge)
        print("total", order_subtotal)
        charges = {}
        inflation_on_cf = 7
        tax_rates = 5
        cf_acquisition_discount = 20
        discount_dict = {25: 40, 30: 50, 40: 75, 50: 80, 60: 100}

        discount_percentage = 0

        for index, (threshold, percentage) in enumerate(discount_dict.items()):
            if order_subtotal >= threshold:
                discount_percentage = percentage

        print("discount_percentage ", discount_percentage)

        # charges['subtotal'] = (1 + (inflation_on_cf / 100)) * order_subtotal
        charges["subtotal"] = order_subtotal

        # charges['delivery_fees'] = (delivery_charge * (1 - (discount_percentage / 100))) - \
        #     ((inflation_on_cf / 100) * order_subtotal)

        charges["delivery_fees"] = (
            delivery_charge * (1 - (discount_percentage / 100))
        ) - ((inflation_on_cf / 100) * order_subtotal)

        charges["taxes"] = charges["subtotal"] * (tax_rates / 100)

        charges["acquisition_discount"] = (cf_acquisition_discount / 100) * charges[
            "subtotal"
        ]

        charges["total"] = (
            charges["subtotal"] + charges["delivery_fees"] + charges["taxes"]
        ) - charges["acquisition_discount"]

        return charges



def calculate_bxgy_discount(order_items, bxgy_campaign):
    """
    Calculate total discount for Buy X Get Y campaign.

    :param order_items: iterable of order items with attributes:
                        - menu_item (MenuItem instance)
                        - quantity (int)
                        - menu_item.base_price (float)
    :param bxgy_campaign: BxGy campaign instance with fields:
                          - buy_items (related objects with buy_items(list[int]), quantity(int), free_items related)
                          - discount_percent (int)
                          - applies_to_different_items (bool)
    :return: total discount as float
    """

    if not bxgy_campaign or bxgy_campaign.is_disabled:
        return 0.0

    discount_percent = bxgy_campaign.discount_percent or 100
    applies_to_diff = bxgy_campaign.applies_to_different_items

    total_discount = 0.0

    # Map order items by menu_item.id for quick lookup
    order_items_map = {oi.menu_item.id: oi for oi in order_items}

    for buy_item in bxgy_campaign.buy_items.all():
        # Calculate total quantity bought for this buy_item group
        total_buy_qty = 0
        buy_item_ids = buy_item.buy_items or []
        for menu_item_id in buy_item_ids:
            if menu_item_id in order_items_map:
                total_buy_qty += order_items_map[menu_item_id].quantity

        if total_buy_qty < buy_item.quantity:
            # Not enough items bought to trigger this buy_item group
            continue

        times_met = floor(total_buy_qty / buy_item.quantity)

        # For each free_item group under this buy_item
        for free_item in buy_item.free_items.all():
            free_item_ids = free_item.free_items or []
            free_quantity = free_item.quantity * times_met

            remaining_free_qty = free_quantity
            free_items_price = 0.0

            for free_menu_item_id in free_item_ids:
                if free_menu_item_id not in order_items_map:
                    continue

                # If free items must be from same items as buy_items and this free item is not in buy_items, skip
                if not applies_to_diff and free_menu_item_id not in buy_item_ids:
                    continue

                order_free_item = order_items_map[free_menu_item_id]

                applicable_qty = min(order_free_item.quantity, remaining_free_qty)
                free_items_price += order_free_item.menu_item.base_price * applicable_qty
                remaining_free_qty -= applicable_qty

                if remaining_free_qty <= 0:
                    break

            # Apply discount percent on free items total price
            total_discount += (discount_percent / 100) * free_items_price

    return round(total_discount, 2)