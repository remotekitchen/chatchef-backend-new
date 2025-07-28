from datetime import datetime, timedelta

import pytz
from django_celery_beat.models import ClockedSchedule, PeriodicTask

from billing.clients.doordash_client import DoordashClient
from billing.models import Order
from chatchef.celery import app
from core.utils import get_logger

logger = get_logger()


@app.task(name="chatchef.order_delivery_scheduler")
def create_order_delivery_scheduler(pk):
    order_data = Order.objects.get(id=pk)
    try:
        doordash = DoordashClient()
        created_quote = doordash.create_quote(order=order_data)
        if created_quote.status_code != 200:
            logger.error('----------------------')
            logger.error("issue created on doordash")
            return False

        accepted_quote = doordash.accept_quote(
            order=order_data)
        if accepted_quote.status_code != 200:
            logger.error('----------------------')
            logger.error("issue created on doordash")
            return False

        logger.error('----------------------')
        logger.error("Order accepted on otter")
        return True
    except Exception as error:
        pass


def order_delivery_scheduler(pk):
    status = False
    order_data = Order.objects.get(order_id=pk)
    task_time = order_data.scheduled_time
    current_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

    _difference = task_time - current_utc
    time_difference_in_minutes = _difference.total_seconds() / 60
    time_difference = None

    if time_difference_in_minutes > 30:
        time_difference = task_time - timedelta(minutes=30)

    if time_difference > current_utc:
        scheduled_obj = ClockedSchedule.objects.create(
            clocked_time=time_difference)

        PeriodicTask.objects.create(
            name=f'create delivery for schedule order --> {pk}',
            task="chatchef.order_delivery_scheduler",
            args=[order_data.id,],
            clocked=scheduled_obj,
            one_off=True
        )
        status = True
    return status
