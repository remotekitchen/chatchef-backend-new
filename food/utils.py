import datetime

from core.utils import get_logger
from food.models import Menu, TimeTable
from billing.models import Order, OrderItem
from food.models import MenuItem
from collections import defaultdict
from collections import Counter
from itertools import combinations
from collections import defaultdict

logger = get_logger()


def is_closed(obj: Menu):

    if obj is None:
        return True

    def _timezone(timezone_str):
        print(timezone_str.replace(':', '.'))
        offset = float(timezone_str.replace(':', '.'))
        print('timezone -->', offset)
        timezone = {
            '-12:00': -12,
            '-11:00': -11,
            '-10:00': -10,
            '-09:00': -9,
            '-08:00': -8,
            '-07:00': -7,
            '-06:00': -6,
            '-05:00': -5,
            '-04:00': -4,
            '-03:00': -3,
            '-02:00': -2,
            '-01:00': -1,
            '+00:00': +0,
            '+01:00': +1,
            '+02:00': +2,
            '+03:00': +3,
            '+04:00': +4,
            '+05:00': +5,
            '+06:00': +6,
            '+07:00': +7,
            '+08:00': +8,
            '+09:00': +9,
            '+10:00': +10,
            '+11:00': +11,
            '+12:00': +12,
        }

        current_utc = datetime.datetime.utcnow()
        utc_converted = datetime.timedelta(hours=timezone[timezone_str])
        _utc_converted = datetime.timedelta(hours=offset)
        _converted_time = current_utc + _utc_converted
        print(_converted_time)
        converted_time = current_utc + utc_converted

        return converted_time

    status = True

    if not obj.opening_hours.all().exists():
        return status

    current_time = _timezone(obj.restaurant.timezone).time()
    current_day = _timezone(obj.restaurant.timezone).strftime("%a").lower()

    if not obj.opening_hours.filter(day_index=current_day).exists():
        return status

    # checking current and next day
    days = obj.opening_hours.filter(day_index=current_day)
    match_found = False

    for date in days:
        if not TimeTable.objects.filter(opening_hour=date.id).exists():
            return status

        # closed set via restaurant owner -->
        if date.is_close:
            return status

        is_times = TimeTable.objects.filter(opening_hour=date.id)

        for is_time in is_times:
            logger.error(f'obj id {obj.id}')
            logger.error(f'time obj id {is_time.id}')
            logger.error(f'start time {is_time.start_time}')
            logger.error(f'current time {current_time}')
            logger.error(f'end time {is_time.end_time}')

            # checking next day -->
            if is_time.end_time <= is_time.start_time and is_time.start_time < current_time:
                logger.error('next day')
                match_found = True

            # checking today day -->
            if is_time.start_time < current_time < is_time.end_time:
                logger.error('today')
                match_found = True

            if match_found:
                status = False
                return status

    # checking prev day -->
    logger.error('checking prev days')
    days_array = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    today_index = days_array.index(f"{is_time.opening_hour}")
    prev_day = days_array[today_index-1]

    if not obj.opening_hours.filter(day_index=prev_day).exists():
        return status

    prev_index_days = obj.opening_hours.filter(day_index=prev_day)

    for prev_index_day in prev_index_days:
        if not TimeTable.objects.filter(opening_hour=prev_index_day.id).exists():
            return status

        prev_days = TimeTable.objects.filter(opening_hour=prev_index_day.id)
        for prev_time in prev_days:
            logger.error(f'prev time obj id {prev_time.id}')
            if prev_time.end_time <= prev_time.start_time and current_time < prev_time.end_time:
                logger.error('prev days')
                status = False
                return status

    return status


def get_recommendations_from_cart(cart_item_ids):
    """
    Recommend dishes frequently bought together with the items in the cart.
    """
    # Step 1: Find all orders containing any of the cart items
    relevant_orders = OrderItem.objects.filter(menu_item_id__in=cart_item_ids).values_list('order_id', flat=True).distinct()
    print(list(relevant_orders), "Relevant Orders Containing Cart Items --->")

    # Step 2: Find all items from these orders (including cart items)
    all_order_items = OrderItem.objects.filter(order_id__in=relevant_orders).values_list('menu_item_id', flat=True)
    print(list(all_order_items), "All Items from Relevant Orders --->")

    # Step 3: Count co-occurrences of items, excluding cart items
    co_occurrence_counts = Counter()
    for item_id in all_order_items:
        if item_id not in cart_item_ids:  # Exclude items already in the cart
            co_occurrence_counts[item_id] += 1

    print(co_occurrence_counts, "Final Co-Occurrence Counts --->")

    # Step 4: Sort by frequency and fetch top recommendations
    most_common_items = co_occurrence_counts.most_common(5)  # Top 5 recommendations
    recommended_item_ids = [item[0] for item in most_common_items]
    print(most_common_items, "Top 5 Most Common Items --->")

    # Step 5: Fetch recommended MenuItem objects
    recommended_dishes = MenuItem.objects.filter(id__in=recommended_item_ids)
    print(recommended_dishes, "Recommended Dishes --->")

    return recommended_dishes
      
   
    