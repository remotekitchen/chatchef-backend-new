from threading import Thread

from accounts.models import RestaurantUser
from billing.models import Order
from communication.models import CustomerInfo
from core.utils import get_logger
from marketing.models import FissionCampaign, LoyaltyProgram
from reward.models import UserReward

logger = get_logger()


class OrderRewards:
    def reward_lucky_draw(self, order):
        if order.user is not None:
            campaign = FissionCampaign.objects.filter(
                restaurant=order.restaurant,
                availability=FissionCampaign.Availability.AFTER_EVERY_ORDER
            )

            if campaign is not None:
                restaurant_user: RestaurantUser = RestaurantUser.objects.filter(user=order.user,
                                                                                restaurant=order.restaurant).first()
                restaurant_user.available_lucky_draws.add(*campaign)

    def create_update_customer(self, order):
        kwargs = {
            'name': order.customer,
            'email': order.user.email if order.user is not None else '',
        }
        customer = CustomerInfo.objects.update_or_create(
            contact_no=order.dropoff_phone_number,
            defaults=kwargs
        )[0]
        if order.restaurant is not None:
            customer.restaurant.add(order.restaurant)

    def update_reward_points(self, order):
        user = order.user
        if user is not None:
            try:
                restaurant_user = RestaurantUser.objects.get_or_create(
                    user=user, restaurant=order.restaurant)[0]
                # 1 dollar subtotal = reward_point_equivalent defined by restaurant
                next_point = int(
                    int(order.subtotal) *
                    restaurant_user.restaurant.reward_point_equivalent
                )
                logger.error(f'user reward point :: {next_point}')
                restaurant_user.reward_points += next_point
                restaurant_user.save(update_fields=['reward_points'])
                order.reward_points = next_point
                order.save(update_fields=['reward_points'])
            except Exception as e:
                logger.error(f'Reward point increment error :: {e}')

    def reward_loyalty_program(self, order):
        user = order.user
        order_cnt = Order.objects.filter(
            restaurant=order.restaurant, location=order.location).count()
        loyalty_programs = LoyaltyProgram.objects.filter(
            restaurant=order.restaurant,
            location=order.location,
            min_orders__lte=order_cnt
        )
        rewards = []
        # Give the reward to user
        for program in loyalty_programs:
            reward = UserReward(
                user=user,
                restaurant=order.restaurant,
                location=order.location,
                reward_type=program.reward_type,
                amount=program.discount_amount
            )
            rewards.append(reward)
        UserReward.objects.bulk_create(rewards)

    def main(self, order):
        # Update or Create CustomerInfo object
        customer_thread = Thread(
            target=self.create_update_customer, args=(order,))
        customer_thread.start()

        # Increment reward points for the user and order restaurant
        reward_thread = Thread(target=self.update_reward_points, args=(order,))
        reward_thread.start()

        # Give the user a lucky draw if there's one of relevant type
        fission_thread = Thread(target=self.reward_lucky_draw, args=(order,))
        fission_thread.start()

        # Check if any loyalty program is avaiable and grant the reward if available
        loyalty_thread = Thread(
            target=self.reward_loyalty_program, args=(order,))
        loyalty_thread.start()
