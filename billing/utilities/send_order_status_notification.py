import datetime
import json
import math
from datetime import timedelta
from string import Template
from threading import Thread

from django.utils import timezone
from django_celery_beat.models import ClockedSchedule, PeriodicTask
from twilio.rest import Client

from billing.api.base.serializers import \
    BaseOrderGetSerializerWithModifiersDetails
from billing.api.v1.serializers import OrderSerializer
from billing.models import Order
from billing.tasks import schedule_order_restaurant_notification_task
from communication.models import CustomerInfo
from communication.utils import Twilo
from core.utils import get_logger
from firebase.models import (CompanyPushToken, FirebasePushToken,
                             NotificationTemplate, Platform)
from firebase.utils.fcm_helper import FCMHelper

logger = get_logger()


def send_order_status_notification_helper(
        status, token, platform=Platform.WEB
):
    template = NotificationTemplate.objects.filter(key="ORDER_STATUS").first()
    if template is None:
        logger.error("Order status notification template not found")
        return
    platform_key = (
        "webpush" if platform == Platform.WEB else "android"
    )

    fcm_payload = {
        "message": {
            platform_key: {
                "data": template.data,
                "notification": {
                    "title": Template(template.notification_title).substitute(
                        status=status
                    ),
                    "body": Template(template.notification_title).substitute(
                        status=status
                    ),
                    "image": template.notification_image,
                    "click_action": template.click_action,
                },
            },
            "data": template.data,
            # 'topic': 'order_status',
            "token": token,
        }
    }

    fcm_helper = FCMHelper()
    fcm_helper.send_notification(fcm_payload)


def send_order_status_notification_to_user(instance: Order):
    try:
        order_status = instance.get_status_display()
        if instance.user is not None:
            for token in FirebasePushToken.objects.filter(user=instance.user):
                # thread = Thread(
                #     target=send_order_status_notification_helper,
                #     args=(order_status, token.push_token, token.platform),
                # )
                # thread.start()
                try:
                    send_order_status_notification_helper(
                        order_status, token.push_token, token.platform
                    )
                except:
                    pass

    except Exception as e:
        logger.error(f"Order status notification error: {e}")


def send_schedule_order_notification_to_restaurant(instance: Order):
    print("order comeing")
    try:
        if instance.id is not None:
            order = Order.objects.get(id=instance.id)

            if order.status == Order.StatusChoices.ACCEPTED and order.scheduling_type == \
                    Order.SchedulingType.FIXED_TIME and order.is_paid:
                print('scheduled order')
                notification_time = order.scheduled_time - \
                    timedelta(minutes=30)
                print('order time --> ', order.scheduled_time)
                print('notification time --> ', notification_time)

                schedule_order_restaurant_notification_task(order.id)

                # create task for sending schedule order notification

            else:
                print("Do nothing")

    except Exception as e:
        logger.error(f"Order status notification error: {e}")


def send_new_order_notification_helper(order: Order, token, platform):
    template = NotificationTemplate.objects.filter(key="NEW_ORDER").first()
    if template is None:
        logger.error("New order notification template not found")
        return
    platform_key = (
        "webpush" if platform == Platform.WEB else "android"
    )
    fcm_payload = {
        "message": {
            platform_key: {
                "data": template.data,
                "notification": {
                    "title": template.notification_title,
                    "body": template.notification_body,

                    "image": template.notification_image,
                    "click_action": template.click_action,
                },
            },
            "data": {
                # 'order': json.dumps(OrderSerializer(order).data)
                # 'order': json.dumps(BaseOrderGetSerializerWithModifiersDetails(order).data)
                'order': str(order.id),  # key value must be string
                'location': str(order.location.id)
                # 'order_items': list(order.orderitem_set.all().values('menu_item__name', 'quantity')),
                # 'quantity': str(order.quantity),
                # 'subtotal': str(order.subtotal),
                # 'username': str(order.customer),

            },
            # 'topic': 'order_status',
            "token": token,
        }
    }
    # Log the payload to confirm it's being sent
    logger.info(f"Sending push notification with payload: {fcm_payload}")

    fcm_helper = FCMHelper()
    fcm_helper.send_notification(fcm_payload)
    logger.info("Push notification sent successfully")





def send_new_order_notification_to_restaurant_helper(instance: Order):
    
    try:
        if instance.scheduling_type == Order.SchedulingType.FIXED_TIME and instance.status == \
                Order.StatusChoices.SCHEDULED_ACCEPTED:
            # reminder_obj = OrderReminder.objects.get(id=pk)
            # order_data = reminder_obj.order_data
            # # creating celery task
            time = instance.scheduled_time - timedelta(minutes=30)
            scheduled_obj = ClockedSchedule.objects.create(
                clocked_time=time
            )

            print('scheduled order task creating')
            PeriodicTask.objects.create(
                name=f'{instance.customer} order --> {instance.id}',
                task="chatchef.schedule_order_restaurant_notification_task",
                args=[instance.id, ],
                clocked=scheduled_obj,
                one_off=True
            )
            print('Scheduled order task creating')
            return

        if instance.restaurant is not None:
            for token in CompanyPushToken.objects.filter(company=instance.restaurant.company):
                try:
                    send_new_order_notification_helper(
                        instance, token.push_token, token.platform
                    )
                except Exception as e:
                    logger.error(f'New order notification error for token {e}')
                # thread = Thread(
                #     target=send_new_order_notification_helper,
                #     args=(instance, token.push_token, token.platform),
                # )
                # thread.start()
    except Exception as e:
        logger.error(
            f"New order notification error order id -> {instance.id}: {e}"
        )


def send_order_sms_notification(instance: Order):
    try:
        account_sid = Twilo['account_sid']
        auth_token = Twilo['account_token']
        sender = Twilo['msg_from']
        client = Client(account_sid, auth_token)
        """
            Mapping order method and status to notification keys 
        """
        notification_keys = {
            'pickup': {
                'accepted': 'PICKUP_ORDER_PLACED_SMS',
                'ready_for_pickup': 'READY_FOR_PICKUP_SMS',
                'cancelled': 'ORDER_CANCEL_SMS',
                'rejected': 'ORDER_REJECTED_SMS'
            },
            'delivery': {
                # 'pending': 'DELIVERY_ORDER_PLACED_SMS',
                # 'accepted': 'DELIVERY_ORDER_ACCEPTED_SMS',
                # 'cancelled': 'ORDER_CANCEL_SMS'
            }
        }

        est_time = instance.pickup_time
        est_time = "25 minutes" if est_time is None else \
            f'{math.ceil((est_time - timezone.now()).seconds / 60)} minutes'

        variables = {
            'customer': instance.customer or 'customer',
            'estimated_time': est_time,
            'restaurant_name': instance.restaurant.name or '',
            'restaurant_phone': instance.location.phone or '',
            'tracking_link': instance.tracking_url or '',
            'dasher_number': instance.dasher_dropoff_phone_number or '',
            'order_id': instance.order_id or '',
            'restaurant_address': instance.location.details or ''
        }

        if instance.scheduling_type == Order.SchedulingType.FIXED_TIME:
            print("advance order")
            print("sending sms for advanced order")
            notification_keys = {
                'pickup': {
                    'accepted': 'ADVANCED_ORDER_PICKUP_ORDER_PLACED_SMS',
                    'ready_for_pickup': 'ADVANCED_ORDER_READY_FOR_PICKUP_SMS',
                    'cancelled': 'ADVANCED_ORDER_ORDER_CANCEL_SMS'
                },
                'delivery': {
                    # 'pending': 'DELIVERY_ORDER_PLACED_SMS',
                    # 'accepted': 'ADVANCED_ORDER_DELIVERY_ORDER_ACCEPTED_SMS',
                    # 'cancelled': 'ADVANCED_ORDER_ORDER_CANCEL_SMS'
                }
            }

            variables['estimated_time'] = instance.scheduled_time

        template = NotificationTemplate.objects.filter(
            key=notification_keys[instance.order_method][instance.status]
        ).first()
        if template is None:
            logger.error("Sms notification template not found")
            return
        content = Template(template.notification_body).substitute(variables)
        contact_no = instance.dropoff_phone_number
        message = client.messages.create(
            body=content,
            from_=sender,
            to=f'{contact_no}'
        )
        print(f'sent pickup sms {message.date_sent}')
    except Exception as e:
        logger.error(f"Pickup sms notification error: {e}")
