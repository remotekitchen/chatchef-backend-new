from accounts.models import UserEvent
from reward.models import CampaignProgressLog
def log_user_event(user=None, event_name="", metadata=None, platform=None):
    UserEvent.objects.create(
        user=user,
        event_name=event_name,
        metadata=metadata or {},
        platform=platform,
    )



# Example boost configuration per campaign

def apply_campaign_boost(task, config):
    total_coins = 0
    progress_before = task.task_progress_fake

    for coins, progress in config:
        total_coins += coins
        task.task_progress_fake += progress
        if task.task_progress_fake >= 0.9998:
            task.task_progress_fake = 0.9998
            break

    task.save()

    CampaignProgressLog.objects.create(
        task=task,
        coin_amount=total_coins,
        progress_before=progress_before,
        progress_after=task.task_progress_fake,
        source="system"
    )

    return total_coins, task.task_progress_fake
