import stripe

from billing.models import Order, Purchase
from hungrytiger.settings import env
from core.utils import get_logger

# stripe.api_key = env.str('STRIPE_API_KEY')
logger = get_logger()


class StripeClient:
  
    def get_stripe_client(restaurant):
        """ Dynamically get the correct Stripe API key based on restaurant. """
        if restaurant.payment_account == "techchef":
            stripe.api_key = env.str("TECHCHEF_STRIPE_SECRET_KEY")
        else:
            stripe.api_key = env.str("CHATCHEF_STRIPE_SECRET_KEY")
        logger.info(f"Using Stripe API Key for {restaurant.payment_account}")
        return stripe
  
    def refund(self, order):
        stripe_client = self.get_stripe_client(order.restaurant)
        purchase = order.purchase
        refund = stripe_client.Refund.create(
            payment_intent=purchase.purchase_token
        )
        if refund.get('status') == 'succeeded':
            order.status = Order.StatusChoices.CANCELLED
            order.save(update_fields=['status'])
            purchase.purchase_state = Purchase.PurchaseState.REFUNDED
            purchase.save(update_fields=['purchase_state'])
            # update refund status
            order.refund_status = Order.RefundStatusChoices.REFUNDED
        return refund

    def payout(self, payment_id, amount, currency='USD', restaurant=None):
        stripe_client = self.get_stripe_client(restaurant)
        payout = stripe_client.Payout.create(
            amount=amount,
            currency=currency,
            destination=payment_id
        )
        return payout