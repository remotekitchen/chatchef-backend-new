from billing.clients.stripe_client import StripeClient
from billing.models import BillingProfile


def send_payouts(obj_list: list):
    for payout_obj in obj_list:
        profile = BillingProfile.objects.get(company=payout_obj.restaurant.company) if BillingProfile.objects.filter(
            company=payout_obj.restaurant.company).exists() else None
        if not profile:
            return

        if profile.stripe_connect_account is None:
            return

        stripe_client = StripeClient()
        response = stripe_client.payout(
            payment_id=profile.payout_account_id,
            amount=payout_obj.payout_amount,
            currency=profile.currency,
            restaurant=payout_obj.restaurant
        )

        payout_obj.uid = response.get('id')
        payout_obj.is_paid = True
        payout_obj.save()
    return
