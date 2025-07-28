# analytics/utils.py

from collections import defaultdict
from datetime import timedelta
from django.utils import timezone
from accounts.models import User
from billing.models import Order
from accounts.models import (
    UserEvent, DAURecord, ConversionFunnelRecord,
    UserEngagementSegment, UserChurnStatus,
    UserCohort, CohortRetentionRecord,
)

def update_metrics():
    today = timezone.now().date()
    date_range = [today - timedelta(days=i) for i in range(30)]
    event_names = ["app_open", "order_completed"]

    DAURecord.objects.filter(date__in=date_range).delete()
    ConversionFunnelRecord.objects.filter(date__in=date_range).delete()

    user_activity_dates = defaultdict(set)

    for event_name in event_names:
        for date_obj in date_range:
            events = UserEvent.objects.filter(
                event_name=event_name,
                event_time__date=date_obj
            ).select_related("user")

            distinct_users = set()
            for event in events:
                if event.user:
                    distinct_users.add(event.user.id)
                    user_activity_dates[event.user.id].add(date_obj)

            DAURecord.objects.create(
                date=date_obj,
                event_name=event_name,
                count=len(distinct_users)
            )

    for date_obj in date_range:
        opened_count = UserEvent.objects.filter(
            event_name="app_open", event_time__date=date_obj
        ).values("user").distinct().count()

        order_count = UserEvent.objects.filter(
            event_name="order_completed", event_time__date=date_obj
        ).values("user").distinct().count()

        rate = (order_count / opened_count) * 100 if opened_count else 0

        ConversionFunnelRecord.objects.create(
            date=date_obj,
            opened_app=opened_count,
            placed_order=order_count,
            conversion_rate=rate
        )

    UserEngagementSegment.objects.all().delete()
    UserChurnStatus.objects.all().delete()
    UserCohort.objects.all().delete()
    CohortRetentionRecord.objects.all().delete()

    all_users = User.objects.filter(id__in=user_activity_dates.keys())

    for user in all_users:
        activity_dates = user_activity_dates[user.id]
        last_activity = max(activity_dates)
        days_active = len(activity_dates)

        if days_active >= 6:
            segment = "daily_active"
        elif 1 <= days_active <= 5:
            segment = "weekly_active"
        elif any(d >= today - timedelta(days=30) for d in activity_dates):
            segment = "monthly_active"
        else:
            segment = "lapsed"

        UserEngagementSegment.objects.create(user=user, segment=segment)

        days_since_last = (today - last_activity).days
        churn = "active"
        if days_since_last > 30:
            churn = "churned"
        elif 14 <= days_since_last <= 30:
            churn = "at_risk"

        UserChurnStatus.objects.create(
            user=user,
            last_activity_date=last_activity,
            status=churn
        )

        cohort_label = f"{user.date_joined.isocalendar()[0]}-W{user.date_joined.isocalendar()[1]}"
        UserCohort.objects.create(
            user=user,
            cohort_label=cohort_label,
            signup_date=user.date_joined
        )

        for day_offset in [1, 7, 30]:
            retained = sum(
                1 for d in activity_dates
                if d == user.date_joined.date() + timedelta(days=day_offset)
            )
            CohortRetentionRecord.objects.create(
                cohort_label=cohort_label,
                day_offset=day_offset,
                retained_users=retained
            )

    # WAU / MAU
    wau_range_start = today - timedelta(days=6)
    mau_range_start = today - timedelta(days=29)

    wau_users = UserEvent.objects.filter(
        event_name="app_open",
        event_time__date__range=[wau_range_start, today]
    ).values("user").distinct().count()

    mau_users = UserEvent.objects.filter(
        event_name="app_open",
        event_time__date__range=[mau_range_start, today]
    ).values("user").distinct().count()

    DAURecord.objects.create(date=today, event_name="weekly_active", count=wau_users)
    DAURecord.objects.create(date=today, event_name="monthly_active", count=mau_users)
