from datetime import timedelta
from django.utils import timezone

def get_voucher_expiry(voucher):
    if not voucher.reward:
        return None
    rg = voucher.reward.reward_group
    if not rg:
        return None
    if rg.validity_type == rg.ValidityType.UNLIMITED:
        return None
    if rg.validity_type == rg.ValidityType.SPECIFIC_DATE:
        return rg.validity_date
    if rg.validity_type == rg.ValidityType.DAYS_AFTER_REWARDED:
        created = voucher.created_date or timezone.now()
        return created.date() + timedelta(days=rg.validity_days)
    return None