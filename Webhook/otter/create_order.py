import datetime
import json

import pytz
from dateutil import parser

from billing.models import Order, OrderItem
from core.utils import get_logger
from Event_logger.models import Action_logs
from Event_logger.utils import action_saver
from Webhook.models import OtterAuth
from Webhook.otter.auth import refresh_auth_token
from Webhook.otter.request_handler import send_post_request_with_auth_retry

from .country_list import country_code

logger = get_logger()


def create_order(order, action=None):
    if action is None:
        action = Action_logs.objects.create(
            action='Creating Order', logs='creating action')
        action.restaurant = order.restaurant
        action.location = order.location
        action.save()

    action_saver(action, f'order id {order.id}')

    # Get the current UTC time
    current_utc_time = datetime.datetime.utcnow()
    formatted_time = current_utc_time.strftime(
        '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    handover_time = current_utc_time + datetime.timedelta(minutes=15)
    handover_time = handover_time.strftime(
        '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    print('current time --> ', current_utc_time)
    print('formatted time --> ', formatted_time)
    print('handover time --> ', handover_time)

    action_saver(action, f'otter store id {order.location.otter_x_store_id}')

    # Check if the restaurant has an Otter store ID
    if order.location.otter_x_store_id:
        # Define the Otter API endpoint for creating orders
        endpoint = 'v1/orders'

        # Define the initial headers for the API request
        token = ''
        if OtterAuth.objects.filter().exists():
            token = OtterAuth.objects.filter().first().token
        else:
            token = refresh_auth_token()
        initial_headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'X-Store-Id': f'{order.location.otter_x_store_id}',
            'scope': 'menus.publish'
        }

        # Retrieve order items associated with the order
        order_items = OrderItem.objects.filter(order=order)
        items = [
            {
                "quantity": item.quantity,
                "id": str(item.menu_item.otter_item_id),
                "name": item.menu_item.name,
                "price": item.menu_item.base_price,
                "modifiers": [
                    {
                        "id": modifiers.modifiers.otter_id,
                        "quantity": modifiers.quantity,
                        "name": modifiers.modifiers.name,
                        "modifiers": [
                            {
                                "id": modifiers_items.modifiersOrderItems.otter_item_id,
                                "quantity": modifiers_items.quantity,
                                "name": modifiers_items.modifiersOrderItems.name,
                                "price": modifiers_items.modifiersOrderItems.base_price,

                            }for modifiers_items in modifiers.modifiersItems.all()]

                    }for modifiers in item.modifiers.all()
                ]
            }
            for item in order_items
        ]

        # Define customer email based on user or default value
        email = f"{order.user}" if order.user else 'info@chatchef.com'
        delivery_info = {}
        if order.dropoff_address and order.dropoff_address != ', , , , ,':
            address = order.dropoff_address.split(',')
            logger.error('--------------------------------')

            delivery_info = {
                "destination": {
                    "fullAddress": f"{order.dropoff_address}",
                    "postalCode": address[4].strip(),
                    "city": address[2].strip(),
                    "state": address[3].strip(),
                    "countryCode": country_code[address[5].strip()],
                    "addressLines": [
                        f"{order.dropoff_address}"
                    ],
                    "linesOfAddress": [
                        f"{order.dropoff_address}"
                    ],
                    "location": {
                        "latitude": "",
                        "longitude": ""
                    }
                }
            }
        # fulfillmentInfo
        fulfillmentInfo = {
            "fulfillmentMode": f"{order.order_method}".upper(),
            "schedulingType": f"{order.scheduling_type}".upper(),

        }

        if order.order_method == Order.OrderMethod.PICKUP:
            fulfillmentInfo['pickupTime'] = f"{formatted_time}"

        if order.order_method == Order.OrderMethod.DELIVERY:
            fulfillmentInfo['deliveryTime'] = f"{handover_time}"
            fulfillmentInfo['CourierStatus'] = "COURIER_ASSIGNED",

        if order.scheduling_type == Order.SchedulingType.FIXED_TIME and order.scheduled_time:

            scheduled_time = str(order.scheduled_time)
            scheduled_time = datetime.datetime.strptime(
                scheduled_time, "%Y-%m-%d %H:%M:%S%z")
            scheduled_time = scheduled_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            if order.order_method == Order.OrderMethod.PICKUP:
                fulfillmentInfo['pickupTime'] = f"{scheduled_time}"

            elif order.order_method == Order.OrderMethod.DELIVERY or order.order_method == Order.OrderMethod.RESTAURANT_DELIVERY:
                fulfillmentInfo['deliveryTime'] = f"{scheduled_time}"

        if order.is_paid:
            customer_payment = [
                {
                    "value": order.total,
                    "processingStatus": "PROCESSED",
                    "paymentMethod": "CARD",
                    "paymentAuthorizer": "OTHER_TYPE",
                    "cardInfo": {
                        "paymentNetwork": "OTHER",
                        "type": "OTHER"
                    },
                    "externalPaymentType": ""
                }
            ]
        elif not order.is_paid and order.payment_method == Order.PaymentMethod.CASH:
            customer_payment = [
                {
                    "value": order.total,
                    "processingStatus": "COLLECTABLE",
                    "paymentMethod": "CASH",
                    "paymentAuthorizer": "",
                    "externalPaymentType": ""
                }
            ]
        else:
            return 'failed'

        # Define the data payload for creating the order in Otter
        data = {
            "externalIdentifiers": {
                "id": f"{order.order_id}",
                "friendlyId": f"{order.order_id}"
            },
            "currencyCode": f"{order.currency}".upper(),
            "status": "NEW_ORDER",
            "customerNote": "",  # Replace with dynamic customer note
            "items": items,
            "orderedAt": f"{formatted_time}",
            "customer": {
                "name": f"{order.dropoff_contact_first_name} {order.dropoff_contact_last_name}",
                "phone": f"{order.dropoff_phone_number}",
                "email": email
            },
            "deliveryInfo": delivery_info,
            "orderTotal": {
                "subtotal": order.subtotal,
                "claimedSubtotal": order.total,
                "discount": order.discount,
                "tax": order.tax,
                "tip": order.tips,
                "deliveryFee": order.delivery_fee,
                "total": order.total,
                "couponCode": ""
            },
            "orderTotalV2": {
                "customerTotal": {
                    "foodSales": {
                        "breakdown": [
                            {
                                "subType": "VALUE",
                                "name": f"{order.order_id}",
                                "value": order.subtotal
                            }, {
                                "name": f"{order.order_id}",
                                "value": order.tax,
                                "subType": "TAX"
                            }
                        ]
                    },
                    "serviceFee": {
                        "breakdown": [
                            {
                                "subType": "VALUE",
                                "name": f"{order.order_id}",
                                "value": order.convenience_fee
                            }
                        ]
                    },
                }
            },
            "customerPayments": customer_payment,
            "fulfillmentInfo": fulfillmentInfo
        }

        action_saver(action, f'otter data {data}')

        # Send a POST request to Otter to create the order and retrieve the response
        response = send_post_request_with_auth_retry(
            endpoint, initial_headers, data, order.restaurant)

        action_saver(action, f'response {response.json()}')

        # Return the response from Otter
        return response.json()
