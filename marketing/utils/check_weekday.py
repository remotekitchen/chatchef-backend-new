from datetime import timedelta

from django.utils import timezone

from core.utils import get_logger

logger = get_logger()


def is_current_week(dt=None):
    """
    Check if given datetime is of the current week
    :param dt: Datetime
    :return: Boolean
    """
    if dt is None:
        return False
    current_date = timezone.now().date()
    start_of_week = current_date - timedelta(days=current_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    logger.info(f'{start_of_week} {dt} {end_of_week}')
    return start_of_week <= dt.date() <= end_of_week
