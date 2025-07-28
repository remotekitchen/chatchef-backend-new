from datetime import timedelta

from django.db.models import Manager, Q
from django.utils import timezone


class FissionCampaignManager(Manager):
    def get_random_prize(self, restaurant):
        pass

    def get_available_campaigns(self, user, restaurant, restaurant_user=None):
        """
        :param user: User object
        :param restaurant: Restaurant obj
        :param restaurant_user: RestaurantUser obj
        :return:
        """
        q_exp = Q(is_active=True) & Q(restaurant=restaurant)
        current_date = timezone.now().date()
        start_of_week = current_date - timedelta(days=current_date.weekday())
        # end_of_week = start_of_week + timedelta(days=6)
        # this_week_query = Q(last_used_time__gte=start_of_week, last_used_time__lte=end_of_week)
        """
            Filter logic:
            If it's repeating and current day is included in the weekday. 
            If it's time_limit, check if current time falls in between the range.
            Along with campaigns that user has m2m relation with, the base queryset will contain the once_every_week
            campaigns that user has not used this week. Then apply time limit and repeating filters. 
        """
        repeating_filter = Q(validity_type='repeating') & Q(weekdays__contains=current_date.weekday())
        time_limit_filter = Q(validity_type='time_limit') & Q(
            durations__start_date__lte=current_date,
            durations__end_date__gte=current_date
        )
        once_every_week_filter = Q(availability='once_every_week') & (
                Q(last_used_time__lt=start_of_week) | ~Q(last_week_users=user))
        q_exp = q_exp & (repeating_filter | time_limit_filter)

        once_every_week_qs = self.filter(once_every_week_filter)
        user_campaigns = self.all() if restaurant_user is None else restaurant_user.available_lucky_draws.all()
        # time_limit_qs = user_campaigns.filter(time_limit_filter)
        # repeating_qs = user_campaigns.filter(repeating_filter)
        qs = (user_campaigns | once_every_week_qs).filter(q_exp).distinct()
        return qs

    def get_time_limit_campaigns(self, restaurant):
        q_exp = Q(is_active=True) & Q(restaurant=restaurant)
        current_date = timezone.now().date()

        time_limit_filter = q_exp & Q(validity_type='time_limit') & Q(
            durations__start_date__lte=current_date,
            durations__end_date__gte=current_date
        )
        time_limit_qs = self.filter(time_limit_filter)
        return time_limit_qs

    def get_non_once_in_week_campaigns(self):
        current_date = timezone.now().date()
        repeating_filter = Q(validity_type='repeating') & Q(weekdays__contains=current_date.weekday())
        time_limit_filter = Q(validity_type='time_limit') & Q(
            durations__start_date__lte=current_date,
            durations__end_date__gte=current_date
        )
        q_exp = (repeating_filter | time_limit_filter)
        return self.filter(q_exp)
