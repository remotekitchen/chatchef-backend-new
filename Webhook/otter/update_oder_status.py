from datetime import datetime

import stripe

from billing.clients.doordash_client import DoordashClient
from billing.models import Order
from billing.utilities.delivery_manager import DeliveryManager
from hungrytiger.settings import env
from core.utils import get_logger
from Webhook.tasks import order_delivery_scheduler

logger = get_logger()
# stripe.api_key = env.str("STRIPE_API_KEY")

def get_stripe_client(restaurant):
    """ Dynamically get the correct Stripe API key based on restaurant. """
    if restaurant.payment_account == "techchef":
        stripe.api_key = env.str("TECHCHEF_STRIPE_SECRET_KEY")
    else:
        stripe.api_key = env.str("CHATCHEF_STRIPE_SECRET_KEY")
    logger.info(f"Using Stripe API Key for {restaurant.payment_account}")
    return stripe

def canceled_order_on_otter(data, restaurant):
    try:
        # Extract the order_id from the 'data' dictionary
        order_id = data['metadata']['payload']['externalIdentifiers']['id']

        # Retrieve the corresponding order from the database
        order_data = Order.objects.get(order_id=order_id)

        # Update the order status to "cancelled"
        purchase = order_data.purchase
        if purchase is not None:
            stripe_client = get_stripe_client(restaurant)
            refund = stripe_client.Refund.create(
                payment_intent=purchase.purchase_token)
            order_data.status = Order.StatusChoices.CANCELLED
            order_data.refund_status = Order.RefundStatusChoices.REFUNDED
            

            # Save the changes to the order
            order_data.save()

            logger.error('----------------------')
            logger.error('canceled status updated on order database')

            # Return True to indicate successful cancellation
            return True

    except Exception as error:
        # Handle any exceptions that may occur during the cancellation process
        logger.error('Error raised while updating order status to "cancelled"')
        logger.error(error)

        # Return False to indicate an unsuccessful cancellation
        return False


def updated_order_status_otter(data, restaurant, location):
    try:
        order_id = data['metadata']['resourceId']
        order_data = Order.objects.get(order_id=order_id)
        status = data['metadata']['payload']['orderStatusHistory']

        if len(status) == 1:
            # Updating order status to accept
            if status[0]['status'] == "ORDER_ACCEPTED":
                if order_data.order_method == Order.OrderMethod.DELIVERY or order_data.order_method == Order.OrderMethod.RESTAURANT_DELIVERY:
                    if order_data.scheduling_type == Order.SchedulingType.ASAP:

                        delivery_manager = DeliveryManager()
                        response = delivery_manager.create_quote(
                            order=order_data)
                        if response.get("status") >= 400:
                            logger.error('----------------------')
                            logger.error("issue raised on delivery creating")
                            return False

                        delivery = delivery_manager.create_delivery(
                            order=order_data)
                        if delivery.status_code >= 400:
                            logger.error('----------------------')
                            logger.error("issue raised on delivery creating")
                            return False

                        order_data.extra.update(
                            {
                                'uber_delivery_id': delivery.json().get('id')
                            }
                        )

                        logger.error('----------------------')
                        logger.error("Order accepted on otter")
                        return True

                    else:
                        # create doordash with celery
                        status = order_delivery_scheduler()
                        if status:
                            return True
                        return False
                else:
                    order_data.status = Order.StatusChoices.ACCEPTED
                    order_data.save()
                    return True

        else:
            def extract_event_time(event):
                return datetime.fromisoformat(event['eventTime'][:-1])

            latest_event = max(status, key=extract_event_time)

            # If the latest status is "ORDER_READY_TO_PICKUP," update the order status to "ready_for_pickup"
            if latest_event['status'] == "ORDER_READY_TO_PICKUP":
                order_data.status = "ready_for_pickup"
                order_data.save()

                # Log that the order is ready for pickup
                logger.error('----------------------')
                logger.error("Order ready for pick up")
                return True

            # If the latest status is "ORDER_HANDED_OFF," update the order status to "ORDER_HANDED_OFF"
            if latest_event['status'] == "ORDER_HANDED_OFF":
                order_data.status = "completed"
                order_data.save()

                # Log that the order is completed
                logger.error('----------------------')
                logger.error("Order completed")
                return True

            # If the latest status is "ORDER_FULFILLED," update the order status to "ORDER_FULFILLED"
            if latest_event['status'] == "ORDER_FULFILLED":
                order_data.status = "completed"
                order_data.save()

                # Log that the ORDER FULFILLED
                logger.error('----------------------')
                logger.error("Order completed")
                return True

    except Exception as error:
        # Handle any exceptions that may occur during the update process
        logger.error('Error raised while updating order status')
        logger.error(error)

        # Return False to indicate an unsuccessful update
        return False