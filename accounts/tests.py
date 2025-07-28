from django.test import TestCase
#from django.contrib.auth.models import User
# Create your tests here.
import unittest
from datetime import datetime
from django.core.exceptions import ValidationError
from accounts.models import RestaurantUser
from food.models import Restaurant
from marketing.models import FissionCampaign
from django.contrib.auth.models import AbstractUser
from reward.models import RewardManage,UserReward,RewardGroup
from django.contrib.auth import get_user_model
User = get_user_model()

class TestRestaurantUserMethods(unittest.TestCase):
    def setUp(self):
        self.user = User.objects.create(email='test120@example.com')
        self.restaurant = Restaurant.objects.create(name='Test Restaurant')
       # self.fission_campaign = FissionCampaign.objects.create(name='Test Campaign')
        self.restaurant_user = RestaurantUser.objects.create(
            user=self.user,
            restaurant=self.restaurant,
            #last_used_fission=datetime.now(),
            reward_points=100,
            points_spent=20
        )
        self.reward_manage = RewardManage.objects.create(user=self.user, total_reward_point=100, reward_grp=RewardGroup)
        self.user_reward = UserReward.objects.create(user=self.user, reward_type="test_group", restaurant=self.restaurant, location="test_location")

    # def test_calculate_next_level(self):
    #     self.restaurant_user.remain_point = 30
    #     self.restaurant_user.calculate_next_level()
    #     self.assertEqual(self.restaurant_user.next_level, 20)

    # def test_calculate_rewards_category(self):
    #     self.restaurant_user.remain_point = 30
    #     self.restaurant_user.calculate_rewards_category()
    #     self.assertEqual(self.restaurant_user.rewards_category, 'Level-0')

    def test_redeem_reward_points(self):
        self.restaurant_user.reward_points = 100  # Set user's reward points

        # Calling the method under test
        self.restaurant_user.redeem_reward_points(self.reward_manage, self.user_reward)

        # Checking if the reward points and points spent are updated correctly
        self.assertEqual(self.restaurant_user.reward_points, 20)
        self.assertEqual(self.restaurant_user.points_spent, 80)
        #self.reward_manage.total_reward_point = 100  # Set total reward points
        #self.restaurant_user.reward_points = 50  # Set user's reward points
        # Mocking RewardManage and UserReward
        # assuming you have mocked models for these or use django.test.TestCase
        # where you can work with actual database models
       # self.assertRaises(ValidationError, self.restaurant_user.redeem_reward_points)
        #with self.assertRaises(ValidationError):
            #self.restaurant_user.redeem_reward_points(self.reward_manage, self.user_reward)

    def tearDown(self):
        self.user.delete()
        self.restaurant.delete()
       # self.fission_campaign.delete()
        self.restaurant_user.delete()

'''
# Step 1: Start Python shell
$ python

# Step 2: Import required modules and classes
from food.models import Restaurant
 from marketing.models import FissionCampaign
from your_app.models import RestaurantUser  # Replace 'your_app' with the name of your Django app

# Step 3: Create instances of required objects
 user = User.objects.get(id=1)  # Assuming User is imported and you have a user with ID 1
 restaurant = Restaurant.objects.get(id=1)  # Assuming Restaurant is imported and you have a restaurant with ID 1
fission_campaign = FissionCampaign.objects.get(id=1)  # Assuming FissionCampaign is imported and you have a campaign with ID 1

# Step 4: Create an instance of RestaurantUser
 restaurant_user = RestaurantUser(
.    user=user,
     restaurant=restaurant,
     last_used_fission=None,
     reward_points=1000,  # Set reward points to test the validation error
 )

# Step 5: Test methods
 restaurant_user.save()  # This will trigger the save method and possibly raise a validation error
restaurant_user.calculate_next_level()  # Test calculate_next_level method
 restaurant_user.calculate_rewards_category()  # Test calculate_rewards_category method
restaurant_user.redeem_reward_points()  # Test redeem_reward_points method

# You can also check the object's attributes and perform other tests as needed
 restaurant_user.reward_points
1000
restaurant_user.remain_point
1000
 restaurant_user.rewards_category
'Level-6'  # Assuming reward points are 1000, which falls in the Level-6 category


'''


if __name__ == '__main__':
    unittest.main()
