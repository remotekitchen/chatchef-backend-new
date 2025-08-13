import json
import os

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from hungrytiger.settings import BASE_DIR, FIREBASE_SERVICE_ACCOUNT_FILE, env
from core.utils import get_logger
from firebase.models import NotificationTemplate, TokenFCM
from firebase_admin.messaging import Message, Notification, send
from firebase_admin import messaging, credentials
from django.dispatch import receiver
from django.db.models.signals import post_save, pre_save

logger = get_logger()

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCOPES = ['https://www.googleapis.com/auth/firebase.messaging']


class FCMHelper:
    def __init__(self):
        self.access_token = None
        self.headers = None

    def get_access_token(self):
        if self.access_token:
            return self.access_token

        with open(os.path.join(BASE_DIR, FIREBASE_SERVICE_ACCOUNT_FILE)) as file:
            data = json.load(file)

        credentials = service_account.Credentials.from_service_account_info(
            data, scopes=SCOPES)
        request = Request()
        credentials.refresh(request)
        self.access_token = credentials.token
        return self.access_token

    def get_headers(self):
        if self.headers:
            return self.headers

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.get_access_token()}'
        }
        return self.headers

    def send_notification(self, request_body):
        try:
            response = requests.post(
                'https://fcm.googleapis.com/v1/projects/chatchef-391108/messages:send',

                json.dumps(request_body),
                headers=self.get_headers()
            )
            print('line 55 fcm --> ', response.json(),
                  response.status_code, response)
        except Exception as e:
            logger.error(f'line 58 Request was unsuccessful!! {e}')

    # def send_notification_to_topic(self, template: NotificationTemplate):
    #     fcm_payload = {
    #         'message': {
    #             'android': {
    #                 'data': template.data,
    #                 'notification': {
    #                     'title': template.notification_title,
    #                     'body': template.notification_body,
    #                     'image': template.notification_image,
    #                     'click_action': template.click_action
    #                 }
    #             },
    #             'data': template.data,
    #             'topic': 'daily_blog'
    #         }
    #     }
    #
    # fcm_helper = FCMHelper()
    # fcm_helper.send_notification(fcm_payload)


    # tokens = ["eARqwgn909Od2l537qeQM6:APA91bEole8ng1y-q5QIaFOE4HvlQpSdolfpO7-m8lvXG6crL-EKg2SD-0n6psTajCaAa7-ZLvn30PGiYia7K6nD6O9OKEoO1tgULMj5zexBhDiDCysk4GI"]

def send_push_notification(tokens, data):

    print("test data -----", data)


    print(f"Sending to {len(tokens)} devices")

    title = data.pop("campaign_title", "")  # Default if missing
    body = data.pop("campaign_message", "")  # Default if missing
    campaign_image = data.pop("campaign_image", "")     # Default if missing
    campaign_category = data.pop("campaign_category", "")     # Default if missing
    campaign_is_active = data.pop("campaign_is_active", "")     # Default if missing
    restaurant_name = data.pop("restaurant_name", "")     # Default if missing
    screen = data.pop("screen", "")     # Default if missing
    id = data.pop("id", "")     # Default if missing
    order_id = data.pop("order_id", "")



    print("campaign_image", campaign_image)
    
    results = {
        "successful": 0,
        "failed": 0,
        "failures": [],
        "invalid_tokens": []  
    }
    
    for token in tokens:
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
                image=str(campaign_image),  
            ),
            android=messaging.AndroidConfig(
                notification=messaging.AndroidNotification(
                    icon="notification_icon",  # Icon resource name in your Android app
                    color="#FF0000",  # Accent color for the notification
                    image="https://www.example.com/notification-image.jpg",  # Android large image
                )
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        badge=1,  # iOS badge count
                        mutable_content=True,  # Required for iOS to download and display images
                        sound="default"
                    )
                ),
                fcm_options=messaging.APNSFCMOptions(
                    image="https://www.example.com/notification-image.jpg"  # iOS image URL
                )
            ),
            webpush=messaging.WebpushConfig(
                notification=messaging.WebpushNotification(
                    icon="https://www.example.com/icon.png",  # Web notification icon
                    badge="https://www.example.com/badge.png",  # Web notification badge
                    image="https://www.example.com/image.jpg"  # Web notification image
                )
            ),
            data={
                "click_action": "https://www.hungry-tiger.com/",
                "image_url": str(campaign_image),  # Also including in data for custom handling
                "badge_count": "1",
                "campaign_category": str(campaign_category),
                "campaign_is_active": str(campaign_is_active),
                "restaurant_name": str(restaurant_name),
                "screen": str(screen),
                "id": str(id),
                "order_id": str(order_id),
                "sound": 'order_sound'

            },
            token=token
        )
        print("token----88", message)


        try:
            response = messaging.send(message)
            print(f"Notification sent successfully to {token[:10]}...: {response}")
            results["successful"] += 1
        except Exception as e:
            error_message = f"Error sending to {token[:10]}...: {e}"
            print(error_message)
            results["failed"] += 1
            results["failures"].append({"token": token, "error": str(e)})
            
            error_str = str(e).lower()
            if any(reason in error_str for reason in [
                "invalid registration", 
                "not registered", 
                "invalid token", 
                "unregistered", 
                "expired"
            ]):
                results["invalid_tokens"].append(token)
                print(f"Token marked for removal: {token[:10]}...")
    print("invalid token", results["failures"])
    # Remove all failed tokens from TokenFCM
    if results["invalid_tokens"]:
        remove_invalid_tokens_from_database(results["invalid_tokens"])
        print(f"Removed {len(results['invalid_tokens'])} invalid tokens from TokenFCM database")
    elif results["failures"]:
        # If we didn't catch any as invalid but there were failures, remove those tokens too
        failed_tokens = [failure["token"] for failure in results["failures"]]
        remove_invalid_tokens_from_database(failed_tokens)
        print(f"Removed {len(failed_tokens)} failed tokens from TokenFCM database")
    return results


# def send_push_notification_for_order(tokens, title, body):
#     """
#     Sends a push notification with only the data payload to prevent Firebase merging.
#     """
#     print("data --- >", title, body)

#     if not tokens:
#         print("No tokens provided.")
#         return {"successful": 0, "failed": 0, "failures": []}

#     print(f"Sending notification to {len(tokens)} devices")
    
#     results = {
#         "successful": 0,
#         "failed": 0,
#         "failures": [],
#         "invalid_tokens": []
#     }

#     for token in tokens:
#         message = messaging.Message(
#             data={  # Everything is now in data payload
#                 "title": title,
#                 "body": body
#             },
#             token=token
#         )

#         try:
#             response = messaging.send(message)
#             print(f"Notification sent successfully to {token[:10]}...: {response}")
#             results["successful"] += 1
#         except Exception as e:
#             error_message = f"Error sending to {token[:10]}...: {e}"
#             print(error_message)
#             results["failed"] += 1
#             results["failures"].append({"token": token, "error": str(e)})

#             # Identify invalid tokens
#             if any(reason in str(e).lower() for reason in [
#                 "invalid registration", "not registered", 
#                 "invalid token", "unregistered", "expired"
#             ]):
#                 results["invalid_tokens"].append(token)

#     # Remove invalid tokens from the database
#     if results["invalid_tokens"]:
#         remove_invalid_tokens_from_database(results["invalid_tokens"])
#         print(f"Removed {len(results['invalid_tokens'])} invalid tokens.")

#     return results


def remove_invalid_tokens_from_database(invalid_tokens):
    """
    Remove invalid tokens from the TokenFCM database using Django ORM
    """
    
    removed_count = 0
    for token_value in invalid_tokens:
        try:
            # Find the TokenFCM object by token value
            token_objects = TokenFCM.objects.filter(token=token_value)
            
            # Delete all matching objects
            deleted_count = token_objects.delete()[0]
            removed_count += deleted_count
            print(f"Removed {deleted_count} records for token {token_value[:10]}...")
        except Exception as e:
            print(f"Error removing token {token_value[:10]}...: {e}")
    
    print(f"Successfully removed {removed_count} token records from database")



    """
    Remove invalid tokens from the TokenFCM database using Django ORM
    """
    print("invalid_tokens", invalid_tokens)
    removed_count = 0
    for token_value in invalid_tokens:
        print(token_value, "token value")
        try:
            # Find the TokenFCM object by token value
            # Assuming you have a field named 'token' that stores the token value
            token_objects = TokenFCM.objects.filter(token=token_value)
            
            # Delete all matching objects
            deleted_count = token_objects.delete()[0]
            removed_count += deleted_count
            print(f"Removed {deleted_count} records for token {token_value[:10]}...")
        except Exception as e:
            print(f"Error removing token {token_value[:10]}...: {e}")
    
    print(f"Successfully removed {removed_count} token records from database")








def get_dynamic_message(order, event_type, restaurant_name):    
    event_type_upper = event_type.upper()  # Convert status to uppercase
    print(f"Getting dynamic message for Order ID: {order.id}, Event Type: {event_type_upper}")

    if event_type_upper == "PENDING":
        title = f"#{order.id} Order Placed! 🎉 ({event_type_upper})"
        body = f"We’ve received your order! Just waiting for {restaurant_name} to confirm. 🤞 (STATUS: {event_type_upper})"

    elif event_type_upper == "ACCEPTED":
        title = f"#{order.id} ✅ Order Accepted – Chef’s on It! ({event_type_upper})"
        body = f"{restaurant_name} has accepted your order. The kitchen is heating up! 🔥 (STATUS: {event_type_upper})"

    elif event_type_upper == "SCHEDULED_ACCEPTED":
        title = f"Order #{order.id} Scheduled ({event_type_upper})"
        body = f"Your order has been scheduled and accepted. It will be delivered at the scheduled time. (STATUS: {event_type_upper})"

    elif event_type_upper == "NOT_READY_FOR_PICKUP":
        title = f"Order #{order.id} Not Ready ({event_type_upper})"
        body = f"Your order is not ready for pickup yet. We'll notify you when it's ready. (STATUS: {event_type_upper})"

    elif event_type_upper == "WAITING_FOR_DRIVER":
        title = f"Order #{order.id} Waiting for Driver ({event_type_upper})"
        body = f"A driver has not been assigned yet. We'll update you once it's picked up. (STATUS: {event_type_upper})"

    elif event_type_upper == "DRIVER_ASSIGNED":
        title = f"Driver Assigned to Order #{order.id} ({event_type_upper})"
        body = f"A driver has been assigned to your order. They'll pick it up soon! (STATUS: {event_type_upper})"

    elif event_type_upper == "READY_FOR_PICKUP":
        title = f"Order #{order.id} Ready for Pickup ({event_type_upper})"
        body = f"Your order is ready for pickup! A driver will collect it shortly. (STATUS: {event_type_upper})"

    elif event_type_upper == "RIDER_CONFIRMED":
        title = f"Rider Confirmed for Order #{order.id} ({event_type_upper})"
        body = f"Your rider has confirmed pickup. They're on their way! (STATUS: {event_type_upper})"

    elif event_type_upper == "RIDER_CONFIRMED_PICKUP_ARRIVAL":
        title = f"🚗 Driver Has Arrived! ({event_type_upper})"
        body = f"The restaurant has prepared your order, and the rider is picking it up now. (STATUS: {event_type_upper})"

    elif event_type_upper == "RIDER_ON_THE_WAY":
        title = f"Rider On the Way for Order #{order.id} ({event_type_upper})"
        body = f"Your rider is on the way with your order. Hang tight! (STATUS: {event_type_upper})"

    elif event_type_upper == "RIDER_PICKED_UP":
        title = f"Rider Picked Up Order #{order.id} ({event_type_upper})"
        body = f"Your order has been picked up and is on its way to you. (STATUS: {event_type_upper})"

    elif event_type_upper == "RIDER_CONFIRMED_DROPOFF_ARRIVAL":
        title = f"⏳ Almost There – Get Ready! ({event_type_upper})"
        body = f"Your food is just minutes away! Get ready to enjoy your meal. 😋 (STATUS: {event_type_upper})"

    elif event_type_upper == "COMPLETED":
        title = f"Delivered! 🍕🎉 ({event_type_upper})"
        body = f"Bon appétit! Your order has arrived. Time to dig in! 🍽️ (STATUS: {event_type_upper})"

    elif event_type_upper == "CANCELLED":
        cancel_reason = order.cancellation_reason or ""
        
        if "driver" in cancel_reason.lower():
            title = f"Driver Cancelled Order #{order.id} 😞"
            body = (
                "The delivery driver encountered an issue and couldn't complete your order. "
                "If you already paid, you will receive a full refund. RK will cover the full cost. "
                "We apologise for the inconvenience."
            )


        elif "restaurant" in cancel_reason.lower():
            title = f"Restaurant Cancelled Your Order #{order.id} 🍽️"
            body = f"{restaurant_name} could not fulfil your order. Please try again later."

        elif "no driver" in cancel_reason.lower():
            title = f"No Driver Found for Order #{order.id} 🛵"
            body = (
                "We couldn’t assign a driver within the time limit. "
                "Your order has been cancelled and a full refund will be issued. "
                "Thanks for your patience!"
            )

        else:
            title = f"Order #{order.id} Cancelled 😢"
            body = (
                "Your order has been canceled. If you need assistance, contact support or place a new order."
            )
        # Append status to body for consistency
        body += f" (STATUS: {event_type_upper})"


    elif event_type_upper == "REJECTED":
        title = f"Order #{order.id} Rejected ({event_type_upper})"
        body = f"Your order was rejected. Please try again later. (STATUS: {event_type_upper})"

    elif event_type_upper == "MISSING":
        title = f"Order #{order.id} Missing ({event_type_upper})"
        body = f"We couldn't locate your order. We're looking into it and will update you soon. (STATUS: {event_type_upper})"

    elif event_type_upper == "NA":
        title = f"Order #{order.id} Status Unavailable ({event_type_upper})"
        body = f"The status of your order is currently unavailable. Please check back later. (STATUS: {event_type_upper})"

    else:
        title = f"Order Notification – {event_type_upper}"
        body = f"An update is available for your order. Check the app for details. (STATUS: {event_type_upper})"

    return title, body
