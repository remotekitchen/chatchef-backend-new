from django.db.models import Manager
from core.utils import get_logger

logger = get_logger()


class UserRewardManager(Manager):
    def create_from_reward_group(self, user, reward_group, restaurant, location, prize=None):
        user_rewards = []
        if reward_group is None:
            return []

        for reward in reward_group.reward_set.all():
            reward = self.create(
                user=user,
                restaurant_id=restaurant,
                location_id=location,
                reward=reward
            )
            user_rewards.append(reward)

        return user_rewards
