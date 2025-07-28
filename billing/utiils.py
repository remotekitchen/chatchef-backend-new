import math
import random
import time
from marketing.email_sender import send_email
from rest_framework.generics import (get_object_or_404)
import django.dispatch
import jwt
import requests
import stripe
from django.conf import settings
from django.db.models import Q
from twilio.rest import Client
import pytz
from django.utils.timezone import now
from accounts.models import Contacts, RestaurantUser, User
from billing.clients.paypal_client import PaypalClient
from billing.models import Order, Transactions, UberAuthModel, Wallet
from chatchef.settings import env
from food.models import Restaurant
from billing.models import UnregisteredGiftCard
from datetime import datetime, timedelta, timezone
from django.utils.timezone import make_aware
from datetime import datetime, timedelta, timezone
import qrcode
from io import BytesIO
from django.core.files.uploadedfile import InMemoryUploadedFile


# stripe.api_key = env.str("STRIPE_API_KEY")


def get_doordash_jwt():
    test_access_key = {
        "developer_id": "1a761ddc-5807-410f-b755-ac1c21251cd7",
        "key_id": "39bdb89f-f15c-49b9-9c5a-bfbf1b0c28e2",
        "signing_secret": "BKFoqiJWfR6zqGHXplbWKNIOtipvU-GpbmP-L6uv9cg"
    }
    live_action_key = {
        "developer_id": "1a761ddc-5807-410f-b755-ac1c21251cd7",
        "key_id": "cb725fdf-335f-41e3-9d34-e59f44125933",
        "signing_secret": "F0vGcxrFo2RDRuvYUUN0qLbob5BwtY0nmF5owgdmgM4"
    }
    access_key = test_access_key if env.str(
        'DOORDASH_ENV', default='TEST'
    ) == 'TEST' else live_action_key

    token = jwt.encode(
        {
            "aud": "doordash",
            "iss": access_key["developer_id"],
            "kid": access_key["key_id"],
            "exp": str(math.floor(time.time() + 1800)),
            "iat": str(math.floor(time.time())),
        },
        jwt.utils.base64url_decode(access_key["signing_secret"]),
        algorithm="HS256",
        headers={"dd-ver": "DD-JWT-V1"}
    )

    return token


def get_doordash_headers():
    headers = {
        "Accept-Encoding": "application/json",
        "Authorization": "Bearer " + get_doordash_jwt(),
    }
    return headers


# Uber Direct
uber_test_creds = {
    'UBER_CLIENT_ID': 'Bvq3bnN2qOTOypFC1v9OHkdbOVwOJike',
    'UBER_CLIENT_SECRET': '1Z95m_kEUBmb7W1iwIgRK_tPTIWGo4Z8n9RooENi',
    'UBER_CUSTOMER_ID': '240442f0-70ba-5890-9d37-bae98931d0c9',
}

uber_prod_creds = {
    'UBER_CLIENT_ID': 'jpvD7UvtTAW-1Z8__0oRpySdJI47xSDc',
    'UBER_CLIENT_SECRET': 'wpgavR7Zekamh1FdI0xpCwYKdCs9SMmwySSlF5JA',
    'UBER_CUSTOMER_ID': 'af1694bc-a29f-4a32-abfb-9e4a18345409',
}

uber_creds = uber_test_creds if env.str(
    'DOORDASH_ENV', default='TEST') == 'TEST' else uber_prod_creds


def get_Uber_jwt():
    access_key = {
        # "client_id": "9d1BWn7OJedUGHTwPfAPlMqx73OONkb3",
        # "client_secret": "BSOoiWxjBuRDR7ZMnR17uCbAHd4sSl8TlAeGNWTO",

        "client_id": uber_creds.get("UBER_CLIENT_ID"),
        "client_secret": uber_creds.get("UBER_CLIENT_SECRET"),
        "grant_type": "client_credentials",
        "scope": "eats.deliveries"
    }
    url = 'https://login.uber.com/oauth/v2/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, headers=headers, data=access_key)
    token = response.json().get('access_token')

    if not UberAuthModel.objects.filter().exists():
        UberAuthModel.objects.create(token=token)

    token_obj = UberAuthModel.objects.filter().first()
    token_obj.token = token
    token_obj.save()

    return token_obj


def get_uber_header():
    token = UberAuthModel.objects.filter().first()
    if token is None:
        token = get_Uber_jwt()
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token.token}',
    }


class get_Uber_Credentials:
    # customerId = "b04c781e-3990-5f5a-b759-87c43f898d2f"
    customerId = uber_creds.get("UBER_CUSTOMER_ID")

    # headers = {
    #     'Content-Type': 'application/json',
    #     'Authorization': 'Bearer ' + get_Uber_jwt(),
    # }


'''
Manage transactions

'''


class MakeTransactions:
    def get_wallet(user_id, restaurant):
        wallet = None
        user = User.objects.get(id=user_id)
        restaurant = Restaurant.objects.get(id=restaurant)
        if Wallet.objects.filter(user=user_id, restaurant=restaurant).exists():
            wallet = Wallet.objects.get(user=user_id, restaurant=restaurant)
        else:
            wallet = Wallet.objects.create(user=user, restaurant=restaurant)
        return wallet

    def top_up(user_id, amount, currency, restaurant, gateway='stripe'):
        if not Restaurant.objects.filter(id=restaurant).exists():
            return False

        wallet = None
        user = User.objects.get(id=user_id)
        restaurant = Restaurant.objects.get(id=restaurant)
        if Wallet.objects.filter(user=user_id, restaurant=restaurant).exists():
            wallet = Wallet.objects.get(user=user_id, restaurant=restaurant)
        else:
            wallet = Wallet.objects.create(user=user, restaurant=restaurant)

        gateways = {
            'unknown': Transactions.PaymentGateway.UNKNOWN,
            'stripe': Transactions.PaymentGateway.STRIPE,
            'paypal': Transactions.PaymentGateway.PAYPAL
        }

        charges = {
            'unknown': 0,
            'stripe': MakeTransactions.calculateStripe(amount, currency),
            'paypal': MakeTransactions.calculatePaypal(amount, currency)
        }

        charge = charges[gateway]

        intent = None
        uid = ''
        if gateway == 'stripe':
            intent = MakeTransactions.stripe_indent(charge, currency)
            uid = intent.get("id")

        if gateway == 'paypal':
            intent = MakeTransactions.paypal_intent(
                charge, currency, restaurant)
            uid = intent["id"]

        transactions = Transactions.objects.create(
            wallet=wallet,
            user=user,
            amount=amount,
            charges=float(charge.get('fee') or 0),
            currency=currency,
            type=Transactions.TransactionType.IN,
            status=Transactions.TransactionStatus.PENDING,
            gateway=gateways[gateway],
            gateway_transaction_id=uid,
            restaurant=restaurant
        )
        return {'charges': charge, 'transactions': transactions, 'intent': intent}

    def top_up_success(uid):
        if not Transactions.objects.filter(gateway_transaction_id=uid).exists():
            return False

        transactions = Transactions.objects.get(gateway_transaction_id=uid)
        if transactions.status == Transactions.TransactionStatus.SUCCESS:
            return False

        wallet = transactions.wallet

        if transactions.used_for == Transactions.UsedFor.GIFT:
            wallet = MakeTransactions.get_wallet(
                user_id=transactions.gift_user.user.id, restaurant=transactions.restaurant.id)

            Transactions.objects.create(
                wallet=wallet,
                user=transactions.gift_user.user,
                amount=transactions.amount,
                charges=transactions.charges,
                currency=transactions.currency,
                type=Transactions.TransactionType.IN,
                status=Transactions.TransactionStatus.SUCCESS,
                used_for=Transactions.UsedFor.GIFT,
                gateway=transactions.gateway,
                restaurant=transactions.restaurant,
                gift_by=transactions.gift_by,
                
            )

        wallet.balance += transactions.amount
        wallet.save()
        transactions.status = Transactions.TransactionStatus.SUCCESS
        transactions.save()

        return True

    def deduct_amount(user, order_id):
        status = 'transactions completed'
        if not Order.objects.filter(id=order_id).exists():
            status = 'invalid order!'
            return status

        order = Order.objects.get(id=order_id)
        if order.is_paid:
            status = 'order already paid!'
            return status

        amount = order.total
        currency = order.currency
        restaurant = order.restaurant
        location = order.location

        if not Wallet.objects.filter(user=user).exists():
            status = 'invalid wallet'
            return status

        wallet = Wallet.objects.get(user=user)

        if wallet.balance < amount:
            status = 'Insufficient balance'
            return status

        user = User.objects.get(id=user)
        transactions = Transactions.objects.create(
            wallet=wallet,
            user=user,
            amount=amount,
            charges=0,
            currency=currency,
            type=Transactions.TransactionType.OUT,
            status=Transactions.TransactionStatus.PENDING,
            gateway=Transactions.PaymentGateway.UNKNOWN,
            restaurant=restaurant,
            location=location
        )

        wallet.balance -= transactions.amount
        wallet.save()

        transactions.status = Transactions.TransactionStatus.SUCCESS
        transactions.save()

        order.is_paid = True
        order.save()

        return status

    def stripe_indent(charge, currency, restaurant):
        """ Create a Stripe PaymentIntent for the correct account """
        
        stripe_account_id = None
        if restaurant.payment_account == "techchef":
            stripe.api_key = env.str("TECHCHEF_STRIPE_SECRET_KEY")
            stripe_account_id = env.str("TECHCHEF_STRIPE_ACCOUNT")
        else:
            stripe.api_key = env.str("CHATCHEF_STRIPE_SECRET_KEY")
            stripe_account_id = env.str("CHATCHEF_STRIPE_ACCOUNT")
        print('stripe_account_id', stripe_account_id)
        stripe_params = {
            "amount": int(float(charge.get("total")) * 100),
            "currency": currency,
            "automatic_payment_methods": {"enabled": True},
        }
        # Route to the correct Stripe account
        if stripe_account_id:
            stripe_params["stripe_account"] = stripe_account_id  
        return stripe.PaymentIntent.create(**stripe_params)

    def calculateStripe(amount, currency):
        fess = {
            'USD': {'Percent': 2.9, 'Fixed': 0.30},
            'GBP': {'Percent': 2.4, 'Fixed': 0.20},
            'EUR': {'Percent': 2.4, 'Fixed': 0.24},
            'CAD': {'Percent': 2.9, 'Fixed': 0.30},
            'AUD': {'Percent': 2.9, 'Fixed': 0.30},
        }

        _fee = fess[currency]
        amount = float(amount)
        total = (amount + float(_fee['Fixed'])) / \
                (1 - float(_fee['Percent']) / 100)
        fee = total - amount

        return {
            'amount': amount,
            'fee': '{:.2f}'.format(fee),
            'total': '{:.2f}'.format(total)
        }

    def paypal_intent(charge, currency, restaurant):
        return PaypalClient().create_order(
            items=[
                {
                    "name": "Top up",
                    "price": charge.get('total'),
                    "currency": currency,
                    "quantity": 1,
                }
            ],
            total_amount=charge.get('total'),
            payment_details=restaurant.payment_details
        ).json()

    def calculatePaypal(amount, currency):

        fess = {
            'USD': {'Percent': 4.4, 'Fixed': 0.34},
            'GBP': {'Percent': 4.4, 'Fixed': 0.34},
            'EUR': {'Percent': 4.4, 'Fixed': 0.34},
            'CAD': {'Percent': 4.4, 'Fixed': 0.34},
            'AUD': {'Percent': 4.4, 'Fixed': 0.34},
        }

        _fee = fess[currency]
        amount = float(amount)
        total = (amount + float(_fee['Fixed'])) / \
                (1 - float(_fee['Percent']) / 100)
        fee = total - amount

        return {

            'amount': amount,
            'fee': '{:.2f}'.format(fee),
            'total': '{:.2f}'.format(total)
        }


"""

Gift cards -->
    1. use wallet
    2. use stripe

"""


class GiftCardManager:
    def check_wallet(self, user, restaurant, amount):
        wallet = MakeTransactions.get_wallet(user.id, restaurant.id)
        can_pay_via_wallet = wallet.balance >= amount
        return wallet, can_pay_via_wallet

    def send_gift(
        self,
        sender: RestaurantUser,
        receiver=None,
        receiver_email=None,
        gateway="stripe",
        amount=0,
        currency="CAD",
        restaurant=None,
        sender_name= User.first_name
        
    ):
      
        print('sender', sender_name, sender)
        # Validate sender and receiver connection
        # if receiver:
        #     self.check_connection(sender, receiver)

        # Payment gateways mapping
        gateways = {
            'unknown': Transactions.PaymentGateway.UNKNOWN,
            'stripe': Transactions.PaymentGateway.STRIPE,
            'paypal': Transactions.PaymentGateway.PAYPAL,
            'wallet': Transactions.PaymentGateway.WALLET
        }

        uid = None
        charge = None
        wallet, can_pay_via_wallet = self.check_wallet(sender.user, restaurant, amount)
        
        print('receiver', receiver, receiver_email)
        # Handle wallet gateway
        if gateway == "wallet":
            if not can_pay_via_wallet:
                return {"status": "error", "message": "Insufficient balance!"}
            return self.use_wallet(sender, receiver, amount, restaurant, currency)

        # Handle stripe gateway
        if gateway == "stripe":
            charge = MakeTransactions.calculateStripe(amount, currency)
            intent = self.use_stripe(charge, currency)
            uid = intent.get("id")

            if uid is None:
                raise ValueError("Stripe UID required!")
        print('receiver', receiver)
          
        # Add amount to receiver's wallet
        if receiver:
            receiver_wallet = MakeTransactions.get_wallet(receiver.user.id, restaurant.id)
            # receiver_wallet.balance += amount
            # receiver_wallet.save()
            
            

            # Log transaction
            transactions = Transactions.objects.create(
            wallet=wallet,
            user=sender.user,
            amount=amount,
            charges=float(charge.get('fee', 0) if charge else 0),
            currency=currency,
            type=Transactions.TransactionType.IN,
            status=Transactions.TransactionStatus.PENDING,
            used_for=Transactions.UsedFor.GIFT,
            gift_user=receiver,
            gift_by=sender,
            sender_name=sender.user.first_name,
            gateway=gateways[gateway],
            gateway_transaction_id=uid,
            restaurant=restaurant
        )

            return {"charges": charge, "transactions": transactions, "intent": intent if gateway == "stripe" else None}
       
      
        if not receiver:
            return self._save_unregistered_gift(sender, receiver_email, amount, currency, restaurant, gateway)

    
        receiver_wallet = MakeTransactions.get_wallet(receiver.user.id, restaurant.id)
        # receiver_wallet.balance += amount
        # receiver_wallet.save()

        transactions = Transactions.objects.create(
            wallet=wallet,
            user=sender.user,
            amount=amount,
            charges=float(charge.get('fee', 0) if charge else 0),
            currency=currency,
            type=Transactions.TransactionType.IN,
            status=Transactions.TransactionStatus.PENDING,
            used_for=Transactions.UsedFor.GIFT,
            gift_user=receiver,
            gift_by=sender,
            sender_name=sender.user.first_name,
            gateway=gateways[gateway],
            gateway_transaction_id=uid,
            restaurant=restaurant
        )
        
        
        return {"charges": charge, "transactions": transactions, "intent": intent if gateway == "stripe" else None}

  
    def _save_unregistered_gift(self, sender, receiver_email, amount, currency, restaurant, gateway="stripe", receiver=None,):
        # Save gift card for unregistered users
        
        uid = None
        charge = None
        intent = None
        wallet, can_pay_via_wallet = self.check_wallet(sender.user, restaurant, amount)
        # print(receiver.user.id, 'receiver --> 476')
        
        # # Handle wallet gateway
        # if gateway == "wallet":
        #     if not can_pay_via_wallet:
        #         return {"status": "error", "message": "Insufficient balance!"}
        #     return self.use_wallet(sender, receiver, amount, restaurant, currency)
        if not can_pay_via_wallet:
          return {"status": "error", "message": "Insufficient balance in wallet!"}
        
        if gateway == "stripe":
            charge = MakeTransactions.calculateStripe(amount, currency)
            intent = self.use_stripe(charge, currency)
            uid = intent.get("id")
        wallet.balance -= amount
        wallet.save()
        
        # if gateway == 'wallet':
        #   wallet.balance -= amount
        #   wallet.save()
        
        UnregisteredGiftCard.objects.create(
            email=receiver_email,
            restaurant=restaurant,
            amount=amount,
            currency=currency,
            status="PENDING",
        )
        print("checking the function call")
        if uid is None and gateway == "stripe":
                raise ValueError("Stripe UID required!")
              
        return {"charges": charge, "intent": intent, "status": "success", "message": f"Gift card saved for unregistered user {receiver_email}"}
        # return {"status": "success", "message": f"Gift card saved for unregistered user {receiver_email}"}

    def use_wallet(self, sender, receiver, amount, restaurant, currency):
        sender_wallet = MakeTransactions.get_wallet(sender.user.id, restaurant.id)
        print('sender', receiver)
        receiver_wallet = MakeTransactions.get_wallet(receiver.user.id, restaurant.id)
        
       

        self.gift_out_from_wallet(sender_wallet, amount, sender, receiver, currency)
        self.gift_in_via_wallet(receiver_wallet, amount, sender, receiver, currency)

        return {"status": "success", "message": "Transaction completed using wallet!"}

    def gift_out_from_wallet(self, sender_wallet, amount, sender, receiver, currency):
        sender_wallet.balance -= amount
        sender_wallet.save()

        Transactions.objects.create(
            wallet=sender_wallet,
            user=sender.user,
            amount=amount,
            charges=0,
            currency=currency,
            type=Transactions.TransactionType.OUT,
            status=Transactions.TransactionStatus.SUCCESS,
            used_for=Transactions.UsedFor.GIFT,
            gift_user=receiver,
            gift_by=sender,
            sender_name=sender.user.first_name,
            gateway=Transactions.PaymentGateway.WALLET,
            restaurant=sender.restaurant,
        )
        
        return True

    def gift_in_via_wallet(self, receiver_wallet, amount, sender, receiver, currency):
        receiver_wallet.balance += amount
        receiver_wallet.save()
        
        print('receiver', receiver_wallet.balance)

        Transactions.objects.create(
            wallet=receiver_wallet,
            user=receiver.user,
            amount=amount,
            charges=0,
            currency=currency,
            type=Transactions.TransactionType.IN,
            status=Transactions.TransactionStatus.SUCCESS,
            used_for=Transactions.UsedFor.GIFT,
            gift_user=receiver,
            gift_by=sender,
            sender_name=sender.user.first_name,
            gateway=Transactions.PaymentGateway.WALLET,
            restaurant=sender.restaurant,
        )
        
        return True

    def use_stripe(self, charge, currency="CAD"):
        return MakeTransactions.stripe_indent(
            charge=charge,
            currency=currency,
            customer="chatchef",
        )



    # def check_connection(self, sender, receiver):
    #     if not Contacts.objects.filter(
    #         Q(sender=sender, receiver=receiver) |
    #         Q(sender=receiver, receiver=sender)
    #     ).exists():
    #         raise ValueError("You both are not connected!")
    #     return


def check_for_order_status_and_call(pk):
    if not Order.objects.filter(id=pk).exists():
        return 'invalid order id!'
    order_obj = Order.objects.get(id=pk)
    if not order_obj.restaurant.receive_call_for_order:
        return 'restaurant dont want to receive calls!'

    if order_obj.status == Order.StatusChoices.PENDING:
        account_sid = settings.TWILIO_ACCOUNT_SID
        auth_token = settings.TWILIO_AUTH_TOKEN
        client = Client(account_sid, auth_token)

        call = client.calls.create(
            twiml=f'<Response> \
                                    <Say> \
                                        Dear {order_obj.restaurant.name}, please accept the incoming order \
                                        from {order_obj.customer}. Order amount {order_obj.total}. \
                                    </Say></Response>',
            to=f'{order_obj.location.phone}',
            from_=settings.TWILIO_FROM_NUMBER,
        )
        return call.sid
    else:
        pass
      
      


def convert_to_restaurant_timezone(dt, offset_str):
    # Parse the offset string (e.g., "+06:00" or "-07:00")
    sign = 1 if offset_str.startswith('+') else -1
    hours, minutes = map(int, offset_str[1:].split(':'))
    offset = timedelta(hours=hours, minutes=minutes)
    tz = timezone(sign * offset)

    return dt.astimezone(tz)


def send_order_receipt(order_id):
    # Retrieve the order by ID
    order = get_object_or_404(Order, id=order_id)
    is_remote_kitchen = order.restaurant.is_remote_Kitchen

    # Filter by is_paid and restaurant flags
    if not order.is_paid and not is_remote_kitchen:
        print("Order not paid. Skipping email.")
        return
    print("order.is_paid", order.is_paid)

    if not order.restaurant  or order.restaurant.is_chatchef_bd:
        print("Restaurant not eligible for sending receipt. Skipping email.")
        return

    user_email = order.email
    if not user_email:
        print("No email provided. Skipping email.")
        return
    
    html_path = (
        "email/order_receipt_hungry.html" if is_remote_kitchen else "email/order_receipt.html"
    )


    from_email = settings.DEFAULT_HUNGRY_TIGER_EMAIL if is_remote_kitchen else settings.DEFAULT_FROM_EMAIL

    items = []
    for order_item in order.orderitem_set.all():
        item = {
            "name": order_item.menu_item.name,
            "quantity": order_item.quantity,
            "base_price": order_item.menu_item.base_price,
            "virtual_price": order_item.menu_item.virtual_price,
            "modifiers": [
                {
                    "group_name": modifier.modifiers.name,
                    "items": [
                        {
                            "name": mod_item.modifiersOrderItems.name,
                            "quantity": mod_item.quantity,
                            "price": mod_item.modifiersOrderItems.base_price,
                        }
                        for mod_item in modifier.modifiersItems.all()
                    ],
                }
                for modifier in order_item.modifiers.all()
            ],
        }
        items.append(item)

    # Convert datetime to local timezone
    offset_str = order.restaurant.timezone
    local_time = convert_to_restaurant_timezone(order.receive_date, offset_str)
    formatted_date = local_time.strftime('%Y-%m-%d %H:%M:%S')

    # Custom subject based on order status
    status_subjects = {
        Order.StatusChoices.COMPLETED: "Your Order is Completed",
        Order.StatusChoices.CANCELLED: "Your Order has been Cancelled",
        Order.StatusChoices.PENDING: "Order Confirmation",
        Order.StatusChoices.ACCEPTED: "Order Accepted",
        Order.StatusChoices.READY_FOR_PICKUP: "Order Ready for Pickup",
        Order.StatusChoices.RIDER_PICKED_UP: "Your Order is on the Way",
    }
    default_subject = f"Order Update - {order.order_id}"
    subject = status_subjects.get(order.status, default_subject)

    # Email context
    context = {
        "order_id": order.order_id,
        "user_name": order.user.get_full_name() if order.user else order.customer,
        "restaurant_name": order.restaurant.name if order.restaurant else "Unknown",
        "order_status": order.status,
        "order_date": formatted_date,
        "pickup_address": order.pickup_address,
        "dropoff_address": order.dropoff_address,
        "items": items,
        "subtotal": order.subtotal,
        "tax": order.tax,
        "delivery_fee": order.delivery_fee,
        "total": order.total,
        "currency": order.currency.upper(),
        "payment_method": order.payment_method,
        # ....
        "discount": order.discount,
        "discount_hungrytiger": order.discount_hungrytiger or 0,
        "restaurant_discount_percentage": order.restaurant_discount_percentage or 0,
        "bogo_discount": order.bogo_discount,
        "bxgy_discount": order.bxgy_discount,
        "solid_voucher_code": order.solid_voucher_code if order.solid_voucher_code else None,
        "reward_points_used": order.reward_points,
    }

    # Send the email
    print("bfore sendingmail------99",  subject, order.status)
    send_email(
        subject=subject,
        html_path=html_path,
        context=context,
        to_emails=[user_email],
        restaurant=order.restaurant.id if order.restaurant else 0,
        from_email=from_email
    )

    
    
    
def calculate_pickup_and_dropoff_times(order, offset_field):
    """
    Dynamically calculates pickup and dropoff times based on restaurant's timezone.

    Args:
        order: Order object containing restaurant information.
        offset_field: Timezone offset field (e.g., "-05:00").
    Returns:
        A dictionary with pickup_time and dropoff_time in the restaurant's timezone.
    """
    try:
        # Get the restaurant's timezone offset
        timezone_offset = int(offset_field.split(":")[0])  # Convert "-05:00" to -5
        restaurant_tz = timezone(timedelta(hours=timezone_offset))

        # Calculate local times for pickup and dropoff
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)  # Current time in UTC
        now_local = now_utc.astimezone(restaurant_tz)  # Convert to restaurant's local timezone

        # Define pickup and dropoff offsets (e.g., 30 mins for pickup, 1 hour for dropoff)
        pickup_time_local = now_local + timedelta(minutes=1)
        dropoff_time_local = pickup_time_local + timedelta(minutes=1)
        
        print('pickup_time_local', pickup_time_local)

        # Format as ISO 8601 strings in the restaurant's timezone
        return {
            "pickup_time": pickup_time_local,
            "dropoff_time": dropoff_time_local
        }

    except Exception as e:
        raise ValueError(f"Error calculating pickup/dropoff times: {e}")
      
 

