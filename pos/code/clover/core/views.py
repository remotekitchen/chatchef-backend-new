import environ
import requests
from django.shortcuts import redirect, render
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from billing.models import Order, OrderItem
from hungrytiger.settings.defaults import BASE_DIR
from food.models import POS_DATA, Menu, MenuItem
from pos.models import POS_logs, PosDetails

env = environ.Env()
environ.Env.read_env((BASE_DIR / '.env').as_posix())
END_POINT = env.str('CLOVER_END_POINT')


def log_saver(log, text):
    log.logs += f"\n\n---------------------------------------------\n{text}"
    log.save()


class BaseGetCloverAuthCode(APIView):
    def get(self, request):
        # Retrieve necessary environment variables from Django settings
        APP_ID = env.str('CLOVER_APP_ID')
        APP_SECRET = env.str('CLOVER_APP_SECRET')

        # Extract query parameters from the HTTP request
        AUTHORIZATION_CODE = request.query_params.get('code', None)
        client_id = request.query_params.get('client_id', None)
        merchant_id = request.query_params.get('merchant_id', None)

        # If there's no authorization code but there is a client ID, initiate the authentication process
        if not AUTHORIZATION_CODE and client_id:
            get_auth_url = f"{END_POINT}/oauth/authorize?client_id={client_id}"
            return redirect(get_auth_url)

        # If there's no merchant ID, return a bad request response
        if not merchant_id:
            return Response({'message': 'merchant id not found!'}, status=status.HTTP_400_BAD_REQUEST)

        # Check if the PosDetails record with the given merchant ID exists
        if PosDetails.objects.filter(pos_merchant_id=merchant_id).exists():
            # Retrieve the PosDetails object for the merchant
            posInfo = PosDetails.objects.get(pos_merchant_id=merchant_id)

            # Construct the URL to obtain an access token
            url = f"{END_POINT}/oauth/token?client_id={APP_ID}&client_secret={APP_SECRET}&code={AUTHORIZATION_CODE}"

            # Send a request to Clover to obtain an access token
            response = requests.get(url)

            # Extract the access token from the response JSON
            access_token = response.json().get("access_token")

            # If the access token is not obtained, return an appropriate response
            if not access_token:
                return Response({'message': 'failed to connect merchant!'})

            # Update the access token in the PosDetails object and save it
            posInfo.access_token = access_token
            posInfo.save()

            # Return a success response indicating that the merchant is connected
            return Response({'message': 'merchant connected!'})

        # If no PosDetails record is found for the merchant, return a bad request response
        return Response({'message': 'merchant details not found!'}, status=status.HTTP_400_BAD_REQUEST)

    def post(self, request):
        print(request.body)
        order_id = request.data.get('id', None)
        action = request.data.get('action', None)

        order = Order.objects.get(id=order_id)
        if action == 'order':
            create_order_on_clover(order)
        elif action == 'pay':
            order_payment_information_on_clover(order)
        elif action == 'complete':
            mark_order_complete_on_clover(order)

        return Response({'message': f'ordering --> {order_id}'}, status=status.HTTP_200_OK)

    def put(self, request, pk):
        menu = MenuItem.objects.filter(menu=pk)
        for item in menu:
            push_items_to_clover(item)

        return Response({'message': 'running'}, status=status.HTTP_200_OK)


def push_items_to_clover(item):
    pos_data = POS_logs.objects.create(
        logs=f"Pos logs for item --> {item} --> restaurant --> {item.restaurant}")

    log_saver(pos_data, 'pushing menu to the clover pos')

    END_POINT = env.str('CLOVER_END_POINT')

    if PosDetails.objects.filter(
            restaurant=item.restaurant, pos_type='clover').exists():

        log_saver(pos_data, 'pos details found')
        clover = PosDetails.objects.get(
            restaurant=item.restaurant, pos_type='clover')

        log_saver(pos_data, 'Creating data for pos system')
        create_data = {
            "id": item.id,
            "hidden": False,
            "available": True,
            "autoManage": False,
            "name": item.name,
            "price": int(item.base_price * 100),
            "cost": int(item.base_price * 100),
        }

        # send request to clover
        create_url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/items"
        log_saver(pos_data, f'requested url --> {create_url}')

        headers = {"content-type": "application/json",
                   'Authorization': f'Bearer {clover.access_token}'}

        log_saver(pos_data, 'sending request to clover')

        create_response = requests.post(
            create_url, json=create_data, headers=headers)

        print(create_response)
        if create_response.status_code != 200:
            log_saver(pos_data, f'create item request failed')
            log_saver(pos_data, f'exiting')
            return

        create_json = create_response.json()

        log_saver(pos_data, f'item json --> {create_json}')

        log_saver(pos_data, 'creating pos details')
        pos_new = POS_DATA.objects.create(
            pos_type='Clover', external_id=create_json['id'])

        log_saver(pos_data, 'adding pos details to item')
        item.pos_identifier.add(pos_new)

        category_url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/category_items?expand=items"
        log_saver(pos_data, f'requested url --> {category_url}')

        categories = item.category.all()
        log_saver(pos_data, f'categories --> {categories}')
        category_data_items = []
        for category in categories:
            category_pos_ids = category.pos_identifier.all()
            log_saver(pos_data, f'pos ids --> {category_pos_ids}')
            if category_pos_ids.filter(pos_type='Clover').exists():
                log_saver(pos_data, 'Category exits')
                clover_category_id = category_pos_ids.get(pos_type='Clover')

                category_data_items.append({
                    "category": {
                        "id": clover_category_id.external_id
                    },
                    "item": {
                        "id": f"{create_json['id']}"
                    }
                })
            else:
                log_saver(pos_data, 'creating new category')
                create_category_data = {"name": category.name}
                create_category_url = f"{END_POINT}/v3/merchants/HK24E05P6G1W1/categories"
                create_category_response = requests.post(
                    create_category_url, json=create_category_data, headers=headers)

                if create_category_response.status_code != 200:
                    log_saver(pos_data, f'create category request failed')
                    log_saver(pos_data, f'exiting')
                    return

                create_category_json = create_category_response.json()
                pos_new_category = POS_DATA.objects.create(
                    pos_type='Clover', external_id=create_category_json['id'])
                category.pos_identifier.add(pos_new_category)

                category_data_items.append({
                    "category": {
                        "id": create_category_json['id']
                    },
                    "item": {
                        "id": f"{create_json['id']}"
                    }
                })

        category_data = {
            "elements": category_data_items
        }

        log_saver(pos_data, f'{category_data}')

        category_response = requests.post(
            category_url, json=category_data, headers=headers)
        if category_response.status_code != 200:
            log_saver(pos_data, f'adding category to item request failed')
            log_saver(pos_data, f'exiting')
            return

        print(category_response.text)
        log_saver(pos_data, 'Category Added to the item')

    return


class BaseCloverWebhook(APIView):
    def post(self, request):
        POS_logs.objects.create(logs=f"Pos logs Webhook --> {request.body}")
        return Response(status=status.HTTP_200_OK)


def create_order_on_clover(order):

    pos_data = POS_logs.objects.create(
        logs=f"Pos logs for clover --> {order} --> restaurant --> {order.restaurant}")
    log_saver(pos_data, 'creating order on clover')

    if PosDetails.objects.filter(restaurant=order.restaurant, pos_type='clover').exists():
        log_saver(pos_data, 'clover details exists for this restaurant')
        order_items = OrderItem.objects.filter(order=order)

        items = []
        for item in order_items:
            pos_data_ = item.menu_item.pos_identifier.all()
            if pos_data_.filter(pos_type='Clover').exists():
                clover_data = pos_data_.get(pos_type='Clover')
                for i in range(item.quantity):
                    print(i)

                    items.append({
                        "item": {
                            "id": clover_data.external_id,

                        }

                    })
            else:
                log_saver(
                    pos_data, f'clover details not exists for --> {item}')
                return

        order_data = {
            "orderCart": {
                "groupLineItems": "false",
                "currency": f"{order.currency}",
                "lineItems": items,
                "amount": 2209,
            }
        }

        log_saver(pos_data, f'data --> {order_data}')

        clover = PosDetails.objects.get(
            restaurant=order.restaurant, pos_type='clover')
        order_url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/atomic_order/orders"

        log_saver(pos_data, f'url --> {order_url}')
        headers = {"content-type": "application/json",
                   'Authorization': f'Bearer {clover.access_token}'}
        order_response = requests.post(
            order_url, json=order_data, headers=headers)

        if order_response.status_code != 200:
            log_saver(pos_data, f'failed to create an order')
            log_saver(pos_data, f'exiting')
            return

        order_response_json = order_response.json()

        log_saver(pos_data, f'order details created on clover')
        log_saver(pos_data, f'adding clover order id on CF DB')

        pos_new_order = POS_DATA.objects.create(
            pos_type='Clover', external_id=order_response_json['id'])
        log_saver(pos_data, f'pos identifier -->{pos_new_order}')
        pos_identifiers = order.pos_data.all()
        if pos_identifiers:
            if pos_identifiers.filter(pos_type='Clover').exists():
                old_pos_id = pos_identifiers.get(pos_type='Clover')

                order.pos_data.remove(old_pos_id)
                order.pos_data.add(pos_new_order)
                log_saver(pos_data, f'added new pos identifier')
        else:
            order.pos_data.add(pos_new_order)
        log_saver(pos_data, f'pos identifier --> linked')
        log_saver(
            pos_data, f'order created on clover. order id --> {pos_new_order}')
        log_saver(
            pos_data, f'operation completed')

    return


def order_payment_information_on_clover(order):
    pos_data = POS_logs.objects.create(
        logs=f"Pos logs for add payment details on clover --> {order} --> restaurant --> {order.restaurant}")
    if PosDetails.objects.filter(restaurant=order.restaurant, pos_type='clover').exists():
        log_saver(pos_data, f'clover pos details found')
        log_saver(pos_data, f'updating payment details')
        clover = PosDetails.objects.get(
            restaurant=order.restaurant, pos_type='clover')
        pos_identifiers = order.pos_data.all()

        clover_id = None
        if pos_identifiers:
            if pos_identifiers.filter(pos_type='Clover').exists():
                pos_id = pos_identifiers.get(pos_type='Clover')
                clover_id = pos_id.external_id

        log_saver(pos_data, f'order id --> {clover_id}')

        headers = {"content-type": "application/json",
                   'Authorization': f'Bearer {clover.access_token}'}

        tender_data = {
            "editable": False,
            "label": "payment",
            "opensCashDrawer": False,
            "supportsTipping": False,
            "enabled": False,
            "visible": False,
            "instructions": "please deliver order as soon as possible",
            "labelKey": "com.clover.tender.external_payment"
        }
        tender_url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/tenders"

        tender_response = requests.post(
            tender_url, json=tender_data, headers=headers)

        if tender_response.status_code != 200:
            log_saver(pos_data, f'failed to create tender')
            log_saver(pos_data, f'task failed')
            return

        tender_response_json = tender_response.json()

        log_saver(pos_data, f'tender created --> {tender_response_json}')

        payment_data = {
            "order": {
                "id": clover_id
            },
            "offline": "false",
            "transactionSettings": {
                "disableCashBack": "false",
                "cloverShouldHandleReceipts": "true",
                "forcePinEntryOnSwipe": "false",
                "disableRestartTransactionOnFailure": "false",
                "allowOfflinePayment": "false",
                "approveOfflinePaymentWithoutPrompt": "false",
                "forceOfflinePayment": "false",
                "disableReceiptSelection": "false",
                "disableDuplicateCheck": "false",
                "autoAcceptPaymentConfirmations": "false",
                "autoAcceptSignature": "false",
                "returnResultOnTransactionComplete": "false",
                "disableCreditSurcharge": "false"
            },
            "transactionInfo": {
                "isTokenBasedTx": "false",
                "emergencyFlag": "false"
            },
            "amount": int(order.total*100),
            "tipAmount": int(order.tips*100),
            "taxAmount": int(order.tax*100),
            "tender": {
                "id": tender_response_json['id']
            }
        }

        log_saver(pos_data, f'payment details --> {payment_data}')

        payment_url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/orders/{clover_id}/payments"
        payment_response = requests.post(
            payment_url, json=payment_data, headers=headers)
        if payment_response.status_code != 200:
            log_saver(pos_data, f'failed to update payment')
            log_saver(pos_data, f'task failed')
            return

        log_saver(pos_data, f'payment details added to the order')
        log_saver(pos_data, f'task completed')

        order_complete_data = {
            "id": clover_id,
            "state": "locked"
        }

        url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/orders/{clover_id}"
        order_complete_response = requests.post(
            url, json=order_complete_data, headers=headers)
        if order_complete_response.status_code != 200:
            log_saver(pos_data, f'failed to mark an order complete')
            log_saver(pos_data, f'task failed')
            return

        log_saver(pos_data, f'order completed on clover')
        log_saver(pos_data, f'task completed')

    return


'Currently not using this'


def mark_order_complete_on_clover(order):
    pos_data = POS_logs.objects.create(
        logs=f"Pos logs for marking order complete on clover --> {order} --> restaurant --> {order.restaurant}")
    if PosDetails.objects.filter(restaurant=order.restaurant, pos_type='clover').exists():
        log_saver(pos_data, f'clover pos details found')
        log_saver(pos_data, f'completing details')
        clover = PosDetails.objects.get(
            restaurant=order.restaurant, pos_type='clover')
        pos_identifiers = order.pos_data.all()

        clover_id = None
        if pos_identifiers:
            if pos_identifiers.filter(pos_type='Clover').exists():
                pos_id = pos_identifiers.get(pos_type='Clover')
                clover_id = pos_id.external_id

        log_saver(pos_data, f'order id --> {clover_id}')

        headers = {"content-type": "application/json",
                   'Authorization': f'Bearer {clover.access_token}'}

        order_complete_data = {
            "id": clover_id,
            "state": "locked"
        }

        url = f"{END_POINT}/v3/merchants/{clover.pos_merchant_id}/orders/{clover_id}"
        payment_response = requests.post(
            url, json=order_complete_data, headers=headers)
        if payment_response.status_code != 200:
            log_saver(pos_data, f'failed to mark an order complete')
            log_saver(pos_data, f'task failed')
            return

        log_saver(pos_data, f'order completed on clover')
        log_saver(pos_data, f'task completed')

    return


'''
Task list -->
    - link creating menu on clover from 
    - link order creating -- done
    - link order payment details -- done 

'''

'''

Clover POS --> Workflow
- Inventory Items
    - right now we are only able to push items from our side
    - Are we going to store item stock details like pos have?

- Order 
    - Order created by chat chef
        - With user order -- with payment
        - Anonymous order -- with payment

    - Order created by POS
        - With customer
        - Without customer 
        - with payment
        - without payment

- Payment
    - 
        
    
- User Role (chat chef)
    - customer (who can place order)
    - owner (restaurant owner)
    - customer (customer details created by POS)

'''
'''
        - creating order
            - use this function when we need to create an order on clover pos

        - create tender

        - update payment options

'''
