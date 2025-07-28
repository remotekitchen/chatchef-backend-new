import hashlib
import hmac
import json
import uuid
from threading import Thread
from firebase.models import TokenFCM

import requests
import stripe
from django.contrib.auth.decorators import login_required
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render
from rest_framework import status
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.views import APIView
from twilio.twiml.messaging_response import MessagingResponse

from analytics.api.base.utils import create_visitor_analytics
from billing.api.v1.serializers import OrderSerializer, StripePaymentSerializer
from billing.models import (Order, Purchase, StripeCapturePayload,
                            StripeConnectAccount, Transactions)
from billing.utiils import MakeTransactions
from billing.utilities.order_rewards import OrderRewards
from chatchef.settings import env
from communication.api.base.views import BaseTwiloSendMsgView
from communication.models import CustomerInfo
from core.utils import get_logger
from Event_logger.models import Action_logs
from Event_logger.utils import action_saver
from food.models import Location, Restaurant
from pos.code.clover.core.views import (create_order_on_clover, log_saver,
                                        order_payment_information_on_clover)
from pos.models import POS_logs
from Webhook.otter.create_order import create_order
from Webhook.otter.send_order_status import update_order_status_on_otter
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from billing.models import Order
from django.http import JsonResponse
from firebase.utils.fcm_helper import  get_dynamic_message, send_push_notification
from .otter.utils import otter_webhook_handler
from firebase.utils.fcm_helper import FCMHelper

# Create your views here.
logger = get_logger()

# stripe.api_key = env.str("STRIPE_API_KEY")
e_url = f"https://partners.cloudkitchens.com/v1/auth/oauth2/authorize?client_id={env.str('OTTER_CLIENT_ID', default='005f0e66-c524-4a10-8f4a-9f8fcc953d29')}redirect_uri=https%3A%2F%2Fapi.chatchefs.com%2Fapi%2Fwebhook%2Fv1%2Fotter%2Fcallback%20&response_type=code&scope=organization.read organization.service_integration&state=8A9D16B4"


def get_stripe_client(restaurant):
    """ Dynamically get the correct Stripe API key based on restaurant. """
    if restaurant.payment_account == "techchef":
        stripe.api_key = env.str("TECHCHEF_STRIPE_SECRET_KEY")
    else:
        stripe.api_key = env.str("CHATCHEF_STRIPE_SECRET_KEY")
    logger.info(f"Using Stripe API Key for {restaurant.payment_account}")
    return stripe
class DoordashWebhookView(APIView):
    def post(self, request):
        logger.info(f"{request.data}")
        print(request.data)
        data = request.data
        doordash_event_map = {
            "DASHER_CONFIRMED": Order.StatusChoices.RIDER_CONFIRMED,
            "DASHER_CONFIRMED_PICKUP_ARRIVAL": Order.StatusChoices.RIDER_CONFIRMED_PICKUP_ARRIVAL,
            "DASHER_PICKED_UP": Order.StatusChoices.RIDER_PICKED_UP,
            "DASHER_CONFIRMED_DROPOFF_ARRIVAL": Order.StatusChoices.RIDER_CONFIRMED_DROPOFF_ARRIVAL,
            "DASHER_DROPPED_OFF": Order.StatusChoices.COMPLETED,
            "DELIVERY_CANCELLED": Order.StatusChoices.CANCELLED,
        }
        order = get_object_or_404(
            Order, doordash_external_delivery_id=data.get("external_delivery_id"))
        _status = doordash_event_map.get(data.get("event_name"), order.status)
        pickup_time_estimated = data.get(
            "pickup_time_estimated", order.pickup_time)
        pickup_time_actual = data.get(
            "pickup_time_actual", pickup_time_estimated)
        dropoff_time_estimated = data.get(
            "dropoff_time_estimated", order.delivery_time)
        dropoff_time_actual = data.get(
            "dropoff_time_actual", dropoff_time_estimated)
        tracking_url = data.get("tracking_url", "")
        dasher_dropoff_phone_number = data.get(
            "dasher_dropoff_phone_number", "")
        dasher_pickup_phone_number = data.get("dasher_pickup_phone_number", "")

        dropoff_phone_number = data.get("dropoff_phone_number", "")

        order.status = _status
        order.pickup_time = pickup_time_actual
        order.delivery_time = dropoff_time_actual
        order.tracking_url = tracking_url
        order.dasher_dropoff_phone_number = dasher_dropoff_phone_number
        order.dasher_pickup_phone_number = dasher_pickup_phone_number
        order.save(
            update_fields=["status", "pickup_time", "delivery_time", "tracking_url", "dasher_dropoff_phone_number",
                           "dasher_pickup_phone_number"]
        )
        event = (
            "Status: "
            + data.get("event_name", "")
            + "  Time:- "
            + data.get("created_at", "")
        )

        if order.status == Order.StatusChoices.RIDER_PICKED_UP:
            logger.info(f"Sending message to {dropoff_phone_number}")
            msgSender = BaseTwiloSendMsgView()
            msgSender.send_twilio_message(
                "Your order has been picked up by the rider", dropoff_phone_number
            )

        if order.status == Order.StatusChoices.COMPLETED:
            """
            This code snippet is designed to update the status of an order using a separate thread when the order status is "COMPLETED." Specifically, it creates a new thread, otter_thread, which calls the update_order_status_on_otter function with the order and the new status "FULFILLED" as arguments. The otter_thread is then started to execute this update process asynchronously.By executing the 'update status' function in a separate thread, there will be no errors in the current process.
            """
            otter_thread = Thread(
                target=update_order_status_on_otter, args=(order, "FULFILLED")
            )
            otter_thread.start()

        elif order.status == Order.StatusChoices.CANCELLED:
            """
            This code snippet is designed to update the status of an order using a separate thread when the order status is "COMPLETED." Specifically, it creates a new thread, otter_thread, which calls the update_order_status_on_otter function with the order and the new status "FULFILLED" as arguments. The otter_thread is then started to execute this update process asynchronously.By executing the 'update status' function in a separate thread, there will be no errors in the current process.
            """
            otter_thread = Thread(
                target=update_order_status_on_otter, args=(order, "CANCELED")
            )
            otter_thread.start()

            """
                Process refund if the order is cancelled by doordash
            """
            purchase = order.purchase
            if purchase is not None:
                stripe_client = get_stripe_client(order.restaurant)
                refund = stripe_client.Refund.create(
                    payment_intent=purchase.purchase_token, amount=order.total
                )
                # change refund status to refunded
                order.refund_status = Order.RefundStatusChoices.REFUNDED

        return Response(event, status=200)

    # uber Webhook


class UberWebhookView(APIView):
    def post(self, request):
        # Replace this with your actual signing key
        signing_key = "023912de-ac2f-403d-b864-f24a44577ed6"
        wh_data = request.data
        logger.info(f"{request.data}")
        # Retrieve the payload from the request data
        # wh_data_str = json.dumps(wh_data)

        # # Retrieve the provided HMAC signature from the request headers
        provided_signature = request.headers.get("x-uber-signature", "")
        # print(provided_signature)
        # logger.info(f'provided={provided_signature}')
        # # Calculate the HMAC signature using the signing key and payload
        # calculated_signature = hmac.new(signing_key.encode('utf-8'), wh_data_str.encode('utf-8'), hashlib.sha256).hexdigest()
        # print(calculated_signature)
        # logger.info(f'provided={calculated_signature}')
        # # Compare the calculated signature with the provided signature
        # if provided_signature == calculated_signature:
        # Signature is valid, process the webhook data here
        # Replace this with your own logic
        if provided_signature is not None and provided_signature != "":
            uber_event_map = {
                "pickup": Order.StatusChoices.RIDER_CONFIRMED_PICKUP_ARRIVAL,
                "pickup_complete": Order.StatusChoices.RIDER_PICKED_UP,
                "dropoff": Order.StatusChoices.RIDER_CONFIRMED_DROPOFF_ARRIVAL,
                "delivered": Order.StatusChoices.COMPLETED,
                "CANCELLED": Order.StatusChoices.CANCELLED,
            }

            order = get_object_or_404(
                Order, uber_delivery_id=wh_data.get("delivery_id"))

            courier_imminent_value = wh_data.get(
                "data", {}).get("courier_imminent")
            if courier_imminent_value is False and wh_data.get("status") == "pickup":
                status = Order.StatusChoices.RIDER_CONFIRMED

            status = uber_event_map.get(wh_data.get("status"), order.status)
            pickup_time_estimated = wh_data.get("data").get(
                "pickup_eta", order.pickup_time
            )
            dropoff_time_estimated = wh_data.get("data").get(
                "dropoff_eta", order.delivery_time
            )

            tracking_url = wh_data.get("data").get("tracking_url", "")

            print(
                courier_imminent_value,
                status,
                pickup_time_estimated,
                dropoff_time_estimated,
            )
            if order is not None:
                order.status = status
                order.pickup_time = pickup_time_estimated
                order.delivery_time = dropoff_time_estimated
                order.tracking_url = tracking_url
                order.save(
                    update_fields=[
                        "status",
                        "pickup_time",
                        "delivery_time",
                        "tracking_url",
                    ]
                )

            event = (
                "Status: "
                + wh_data.get("status", "")
                + "  Time:- "
                + wh_data.get("updated", "")
            )

            # if order.status == "COMPLETED":

            #     # otter thread --> completing order on otter
            #     otter_thread = Thread(
            #         target=update_order_status_on_otter, args=(order, "FULFILLED"))
            #     otter_thread.start()

            #     # clover pos thread --> locked order state complete
            #     clover_thread = Thread(
            #         target=order_payment_information_on_clover, args=(order,))
            #     clover_thread.start()
            logger.info(f"Received and validated Uber webhook data:{wh_data}")
            response_data = event
            return Response(response_data, status=200)
        else:
            # Invalid signature, reject the request
            return Response(status=403)

        # Whats Webhook


class WhatAppWebhookView(APIView):
    def post(self, request):
        logger.info(f"{request.data}")
        body = request.data.get("Body", None)
        logger.info(f"{body}")
        # Start our TwiML response
        resp = MessagingResponse()

        # Determine the right reply for this message
        if body == "hello":
            resp.message("Hi!")
        elif body == "bye":
            resp.message("Goodbye")

        return Response(body)


class StripeWebhookView(APIView):

    def post(self, request, *args, **kwargs):
        
        action = Action_logs.objects.create(
            action="Creating Order from webhook", logs="creating action"
        )
        body = request.body
        data = request.data
        signature = request.headers.get("stripe-signature")
        
        metadata = data.get("data", {}).get("object", {}).get("metadata", {})
        restaurant_id = metadata.get("restaurant_id")
        print("Metadata:------------->", metadata)
        restaurant = Restaurant.objects.get(id=restaurant_id)
        print("Restaurant:------------->", restaurant.payment_account)
        endpoint_secret = None
        
        if restaurant.payment_account == "techchef":
            endpoint_secret = env.str("TECHCHEF_STRIPE_ENDPOINT_SECRET")
        else:
            endpoint_secret = env.str("CHATCHEF_STRIPE_ENDPOINT_SECRET")

        if endpoint_secret is not None:
            try:
                stripe_client = get_stripe_client(restaurant)
                data = stripe_client.Webhook.construct_event(
                    payload=body, sig_header=signature, secret=endpoint_secret
                )
            except ValueError as e:
                # Invalid payload.
                return Response(status=200)
            except stripe_client.error.SignatureVerificationError as e:
                print(e)
                # Invalid Signature.
                return Response(status=200)

        logger.info(f"{data}")
        action_saver(action, f"{data}")
        print(data, 'data----------------->')
        if data and data.get("type") == "payment_intent.succeeded":
            uid = data["data"]["object"].get("id")
            print('uid --> ', uid)

            if Transactions.objects.filter(gateway_transaction_id=uid).exists():
                try:
                    status = MakeTransactions.top_up_success(uid)
                    if status:
                        return Response({"success": True}, status=200)
                    return Response({"success": False}, status=200)
                except Exception as error:
                    print('errors --> ', error)
                    return Response({"success": False}, status=200)

            purchase = Purchase.objects.get(purchase_token=uid)
            
            print('restaurant --> ', restaurant, purchase)
            # Determine the correct Stripe account
            stripe_account_id = None
            if restaurant.payment_account == "techchef":
                stripe_account_id = env.str("TECHCHEF_STRIPE_ACCOUNT")
            else:
                stripe_account_id = env.str("CHATCHEF_STRIPE_ACCOUNT")
            
            print('stripe account id --> ', stripe_account_id)
            order_data = purchase.extra.get("order", None)
            action_saver(action, f"order data {order_data}")
            if order_data is None:
                return Response({"success": False}, status=200)
            # logger.info(f'{metadata}')
            try:
                print("running")
                # serializer = OrderSerializer(data=order_data)
                # serializer.is_valid(raise_exception=True)
                # order = serializer.save()
                order = Order.objects.get(purchase=purchase)
                logger.info(f"{order}")
                action_saver(action, f"order serializer data {order}")
                try:
                    # order.purchase = purchase
                    # order.user = purchase.user
                    order.is_paid = True
                    StripeCapturePayload.objects.create(
                        user=purchase.user,
                        payload=data["data"]["object"],
                        uid=uid,
                        purchase=purchase,
                    )

                    purchase.purchase_state = Purchase.PurchaseState.PURCHASED
                    purchase.save(update_fields=["purchase_state"])
                    order.save(update_fields=["is_paid"])

                    try:
                        voucher = order.voucher
                        user = order.user
                        if voucher.is_one_time_use:
                            already_applied_for_voucher = user in self.applied_users.all()
                            if not already_applied_for_voucher:
                                self.applied_users.add(user)

                    except Exception as error:
                        print(error)

                    # Handle order rewards
                    order_reward = OrderRewards()
                    order_reward.main(order)
                    # create_order_on_otter(order)
                    # adding action details

                    action.restaurant = order.restaurant
                    action.location = order.location
                    action.save()

                    action_saver(action, f"calling otter thread")
                    otter_order_thread = Thread(
                        target=create_order, args=(order, action)
                    )
                    otter_order_thread.start()

                    # push order on clover pos
                    clover_order_thread = Thread(
                        target=create_order_on_clover, args=(order,)
                    )
                    clover_order_thread.start()

                    create_visitor_analytics(
                        order.restaurant.id, order.location.id, source="na", count="payment_confirm", user=order.user.id if order.user else None)

                except Exception as e:
                    logger.error(f"{e}")
                    pass
            except Exception as e:
                logger.error(f"{e}")
                return Response({"success": False}, status=200)

        if data["type"] == "account.updated":
            try:
                account = data["data"]["object"]
                account_id = account.get("id")
                stripe_account = get_object_or_404(
                    StripeConnectAccount, stripe_id=account_id
                )
                stripe_account.account_details = account
                stripe_account.charges_enabled = account.get(
                    "charges_enabled", False)
                stripe_account.save(
                    update_fields=["account_details", "charges_enabled"]
                )

            except:
                return Response({"success": False}, status=200)

        return Response({"success": True})


class OtterWebhook(APIView):
    def post(self, request, *args, **kwargs):
        data = request.data
        webhook_status = otter_webhook_handler(data)
        if webhook_status:
            return Response({"success": True})
        return Response(
            {"message": "invalid store"}, status=status.HTTP_400_BAD_REQUEST
        )

    def get(self, request):
        action = request.query_params.get("action")
        pk = request.query_params.get("pk")
        order = Order.objects.get(id=pk)
        if action == "create":
            data = create_order(order)
        elif action == "update":
            data = update_order_status_on_otter(order, "FULFILLED")
        else:
            return Response({"error": "Invalid action parameter"})
        return Response({"message": "Action completed successfully", "order": data})


# otter callback


def auth_token(code):
    data = {
        "grant_type": "authorization_code",
        "client_id": f"{env.str('OTTER_CLIENT_ID', default='005f0e66-c524-4a10-8f4a-9f8fcc953d29')}",
        "client_secret": f"{env.str('OTTER_CLIENT_SECRET', default='CC57DM5TDN6VD5ZPNRDQ')}",
        "scope": "organization.read organization.service_integration",
        "code": f"{code}",
        "redirect_uri": "https://api.chatchefs.com/api/webhook/v1/otter/callback ",
    }

    url = "https://partners.cloudkitchens.com/v1/auth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, headers=headers, data=data)

    response_dict = json.loads(response.text)
    if "access_token" in response_dict:
        return response_dict["access_token"]
    return "error"


@login_required(login_url="/admin/login/")
def otter_callback(request):
    context = {}

    # Check if the 'code' parameter is present in the request's GET parameters
    if request.GET.get("code"):
        code = request.GET.get("code")
        token = auth_token(code)
        if token == "error":
            return redirect(e_url)

        request.session["token"] = token
        headers = {"Authorization": f"Bearer {token}"}

        url = f"https://partners.cloudkitchens.com/organization/v1/organization/brands?limit=500"
        response = requests.get(url=url, headers=headers)
        data = response.json()
        print(data)

        context["org"] = data
    else:
        # Redirect the user to the constructed OAuth2 authorization URL
        return redirect(e_url)

    # If the 'code' parameter is present, render the 'o_auth.html' template with the context
    return render(request, "o_auth.html", context)


@login_required(login_url="/admin/login/")
def otter_callback_store(request, pk):
    context = {}

    try:
        # Try to retrieve the token from the session
        token = request.session.get("token")

        # Set the 'brand' in the session
        request.session["brand"] = pk

        # Check if the token is available in the session
        if token:
            headers = {"Authorization": f"Bearer {token}"}

            # Construct the API URL
            url = f"https://partners.cloudkitchens.com/organization/v1/organization/brands/{pk}/stores?limit=500"

            # Make a GET request to the API
            response = requests.get(url=url, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                data = response.json()
                print(data)
                context["stores"] = data
            else:
                # Handle the case where the API request was not successful
                print(
                    f"API request failed with status code: {response.status_code}")
                return redirect(e_url)
        else:
            # Handle the case where the token is not available in the session
            print("Token not found in the session")
            return redirect(e_url)
    except Exception as e:
        # Catch any other exceptions that might occur
        print(f"An error occurred: {e}")
        return redirect(e_url)

    # Render the template with the context
    return render(request, "o_auth.html", context)


@login_required(login_url="/admin/login/")
def otter_connection_status(request, pk):
    context = {}
    try:
        # Retrieve token and brand from the session
        token = request.session.get("token")
        brand = request.session.get("brand")

        # Construct the headers and URLs
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://partners.cloudkitchens.com/organization/v1/organization/brands/{brand}/stores/{pk}/connection"
        url2 = f"https://partners.cloudkitchens.com/organization/v1/organization/brands/{brand}/stores/{pk}"

        # Make requests to the URLs
        response1 = requests.get(url=url, headers=headers)
        response2 = requests.get(url=url2, headers=headers)

        # Check the status code of the first response
        if response1.status_code == 404:
            context["connected_status"] = False
        else:
            context["connected_status"] = True

        # Add store information to the context
        context["store"] = response2.json()

    except Exception as e:
        return redirect(e_url)

    # Render the template with the context
    return render(request, "o_auth.html", context)


@login_required(login_url="/admin/login/")
def connect_to_otter(request, pk):
    context = {}
    try:
        token = request.session.get("token")
        brand = request.session.get("brand")
        id_ = str(uuid.uuid4())
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://partners.cloudkitchens.com/organization/v1/organization/brands/{brand}/stores/{pk}/connection"
        data2 = {"storeId": id_}
        print(data2)

        response = requests.post(url=url, headers=headers, json=data2)

        if response.status_code == 204:
            context["Connected"] = "Connected"
            context["otter_id"] = id_
        else:
            # Handle other status codes or error conditions as needed
            context["Error"] = f"Failed to connect. Status code: {response.status_code}"

    except Exception as e:
        return redirect(e_url)

    return render(request, "o_auth.html", context)


@login_required(login_url="/admin/login/")
def delete_to_otter(request, pk):
    context = {}
    try:
        token = request.session.get("token")
        brand = request.session.get("brand")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = f"https://partners.cloudkitchens.com/organization/v1/organization/brands/{brand}/stores/{pk}/connection"
        response = requests.delete(url=url, headers=headers)
        if response.status_code == 204:
            context["connection_removed"] = True

    except Exception as e:
        return redirect(e_url)
    return render(request, "o_auth.html", context)


class StripeConnectWebhookView(APIView):
    """
    Webhook for connect account updated, external account created, updated, deleted events from stripe
    """

    def post(self, request):
        body = request.body
        request_data = request.data
        signature = request.headers.get("stripe-signature")
        print(signature)
        endpoint_secret = "whsec_Guc1gvHItlv5nxxjr2klqu4AB200KLSk"
        logger.info(f"{request_data}")
        # Verify webhook signature and extract the event.
        # See https://stripe.com/docs/webhooks#verify-events for more information.
        try:
            stripe_client = get_stripe_client(request.data.user.restaurant)
            event = stripe.Webhook.construct_event(
                payload=body, sig_header=signature, secret=endpoint_secret
            )
        except ValueError as e:
            # Invalid payload.
            return Response(status=400)
        except stripe.error.SignatureVerificationError as e:
            print(e)
            # Invalid Signature.
            return Response(status=400)
        # event = request_data

        # if event["type"] == "account.updated":
        try:
            """
            Since different events can have different structure of responses, we are calling the account retrieve
            api for reading the whole account data
            """
            account_id = event.get("account")
            account = stripe.Account.retrieve(account_id)
            stripe_account = get_object_or_404(
                StripeConnectAccount, stripe_id=account_id
            )
            stripe_account.account_details = account
            stripe_account.charges_enabled = account.get(
                "charges_enabled", False)
            stripe_account.save(
                update_fields=["account_details", "charges_enabled"])

        except:
            return Response({"success": False}, status=400)

        return Response({"success": True})


#####
class WhatsappWebhook(APIView):
    def get(self, request):
        """
        Handle the GET request for webhook verification.
        """
        # Update this value with your Verify Token
        verify_token = env.str("VERIFY_TOKEN")

        # Parse parameters from the verification request
        mode = request.GET.get("hub.mode")
        token = request.GET.get("hub.verify_token")
        challenge = request.GET.get("hub.challenge")

        # Check if a token and mode were sent
        if mode and token:
            # Check if the mode and token sent are correct
            if mode == "subscribe" and token == verify_token:
                # Respond with a 200 OK and the challenge token from the request
                print("WEBHOOK_VERIFIED")
                return HttpResponse(challenge, content_type="text/plain")
            else:
                # Respond with a '403 Forbidden' if verify tokens do not match
                return HttpResponse(status=403)

        return HttpResponse(status=200)

    def post(self, request):
        if request.method == "POST":
            try:
                payload = json.loads(request.body.decode("utf-8"))
                if payload.get("object") == "page" and payload.get("entry"):
                    for entry in payload["entry"]:
                        if "changes" in entry and entry["changes"][0].get("value"):
                            message_data = entry["changes"][0]["value"]["messages"][0]
                            phone_number_id = entry["changes"][0]["value"]["metadata"][
                                "phone_number_id"
                            ]
                            sender_id = message_data["from"]
                            sender_name = entry["changes"][0]["value"]["contacts"][0][
                                "profile"
                            ]["name"]
                            message_text = message_data["text"]["body"]

                            token = env.str("VERIFY_TOKEN")

                            # save the contact in db
                            try:
                                customer_info = get_object_or_404(
                                    CustomerInfo, contact_no=sender_id
                                )
                                if message_text == "Stop promotions":
                                    customer_info.is_subscribed = False
                                    customer_info.save(
                                        update_fields=["is_subscribed"])
                                else:
                                    response = requests.post(
                                        f"https://graph.facebook.com/v12.0/{phone_number_id}/messages?access_token={token}",
                                        json={
                                            "messaging_product": "whatsapp",
                                            "to": sender_id,
                                            "text": {
                                                "body": "Thank you for your message"
                                            },
                                        },
                                        headers={
                                            "Content-Type": "application/json"},
                                    )

                            except Http404:
                                customer_info = CustomerInfo()
                                customer_info.name = sender_name
                                customer_info.contact_no = sender_id
                                if message_text == "Stop promotions":
                                    customer_info.is_subscribed = False
                                else:
                                    response = requests.post(
                                        f"https://graph.facebook.com/v12.0/{phone_number_id}/messages?access_token={token}",
                                        json={
                                            "messaging_product": "whatsapp",
                                            "to": sender_id,
                                            "text": {
                                                "body": "Thank you for your response"
                                            },
                                        },
                                        headers={
                                            "Content-Type": "application/json"},
                                    )
                                customer_info.save()

            except json.JSONDecodeError:
                return HttpResponse(status=400)

                # Check if the payload is from WhatsApp

            return HttpResponse(status=200)

@method_decorator(csrf_exempt, name='dispatch')
class RaiderAppWebhook(APIView):
    def post(self, request, *args, **kwargs):
        print('raider webhook called--------->')
        data = request.data
        action = Action_logs.objects.create(
            action="raider webhook", logs="creating hook"
        )
        action_saver(action, f"data {data}")
        print(data, 'data----------------->')

        event = data.get("event")
        client_id = data.get("client_id")
        if event == "status":
            status = data.get("status")
            status_map = {
                "created": "pending",
                "waiting_for_driver": "waiting_for_driver",
                "driver_assign": "driver_assigned",
                "order_picked_up": "rider_picked_up",
                "on_the_way": "rider_on_the_way",
                "arrived": "rider_confirmed_dropoff_arrival",
                "delivery_success": "completed",
                "delivery_failed": "cancelled",
                "driver_rejected": "rejected",
                "canceled": "cancelled",
            }
            driver_info = data.get("driver_info") or {}
            delivery_time = data.get("actual_delivery_completed_time")
            rider_accepted_time = data.get("rider_accepted_time")
            rider_pickup_time = data.get("rider_pickup_time")

            cancel_reason = data.get("cancel_reason")

            order = Order.objects.get(id=client_id)
            order.status = status_map[status]
            order.driver_info = driver_info 
            order.delivery_time = delivery_time
            order.rider_accepted_time = rider_accepted_time
            order.rider_pickup_time=rider_pickup_time
            order.cancellation_reason = cancel_reason
            
            order.save()
            print(order.status)
            # ✅ Step 1: Get user tokens
            user_id = order.user_id if order.user else None
            tokens = list(
                TokenFCM.objects.filter(user_id=user_id).values_list("token", flat=True)
            )

            if tokens:
                # ✅ Step 2: Generate dynamic title and message
                restaurant_name = order.restaurant.name if order.restaurant else "Restaurant"
                title, body = get_dynamic_message(order, order.status, restaurant_name)

                # ✅ Step 3: Send the notification
                send_push_notification(tokens, {
                    "campaign_title": title,
                    "campaign_message": body,
                    "screen": "order_detail",
                    "id": str(order.id),
                    "campaign_category": "order",
                    "campaign_is_active": "true",
                    "restaurant_name": restaurant_name,
                    "campaign_image": "",  # Optional
                })
            else:
                print(f"No push tokens found for user_id {user_id}")

            return Response({"message": "updated"})
          
        return Response()
    

# Status mapping from Delivery to Order
# DELIVERY_TO_ORDER_STATUS_MAPPING = {
#     "created": "pending",
#     "waiting_for_driver": "not_ready_for_pickup",
#     "driver_assign": "rider_confirmed",
#     "order_picked_up": "rider_picked_up",
#     "on_the_way": "rider_confirmed_dropoff_arrival",
#     "delivery_success": "completed",
#     "delivery_failed": "cancelled",
#     "driver_rejected": "rejected",
#     "canceled": "cancelled",
# }

# @method_decorator(csrf_exempt, name='dispatch')
# class DeliveryStatusWebhookView(APIView):

#     def post(self, request, *args, **kwargs):
#         # Step 1: Verify the API Key
#         api_key = request.headers.get("Authorization")
        
#         # RIDER_WEBHOOK_API_KEY is the authorization token from driver app of any user
#         if not api_key or api_key != f"token {env.str('RIDER_WEBHOOK_API_KEY')}": 
#             return JsonResponse({'error': 'Unauthorized'}, status=401)

#         # Step 2: Process the Webhook Payload
#         try:
#             data = json.loads(request.body)

#             delivery_id = data.get('client_id')
#             delivery_status = data.get('status')

#             if not delivery_id or not delivery_status:
#                 return JsonResponse({'error': 'Invalid data'}, status=400)

#             chatchef_status = DELIVERY_TO_ORDER_STATUS_MAPPING.get(delivery_status)

#             if not chatchef_status:
#                 return JsonResponse({'error': f'Unknown delivery status: {delivery_status}'}, status=400)

#             # Find and update the Order status
#             try:
#                 order = Order.objects.get(id=delivery_id)
#                 order.status = chatchef_status
#                 order.save()

#                 return JsonResponse({'message': f'Order status updated to {chatchef_status}!'})
#             except Order.DoesNotExist:
#                 return JsonResponse({'error': 'Order not found in chatchef'}, status=404)

#         except json.JSONDecodeError:
#             return JsonResponse({'error': 'Invalid JSON data'}, status=400)
#         except Exception as e:
#             return JsonResponse({'error': f'An unexpected error occurred: {str(e)}'}, status=500)
