import os

from celery import Celery

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hungrytiger.settings')

app = Celery()

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


#  Add this line to enable django-celery-beat scheduling
app.conf.beat_scheduler = 'django_celery_beat.schedulers:DatabaseScheduler'



# @app.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
#     pass


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
    

