from celery import shared_task
from accounts.analytic.utils import update_metrics

@shared_task
def run_daily_metrics_update():
    update_metrics()
