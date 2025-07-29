import datetime
import json

import requests
from django.utils.timezone import make_aware
from rest_framework.exceptions import APIException, ParseError

from billing.models import Order
from billing.utiils import get_Uber_Credentials, get_uber_header, get_Uber_jwt
from hungrytiger.settings import env
from core.utils import get_logger
from food.models import MenuItem
from django.utils import timezone

logger = get_logger()


class UberClient:
    def create_quote(self, data=None, order: Order = None, trying=0):
        if data is not None:
            pickup_addr = self.get_uber_address(
                address=data["pickup_address_details"], data_type='dict')
            dropoff_addr = self.get_uber_address(
                address=data["dropoff_address_details"], data_type='dict')
            pickup_phone_number = data["pickup_phone_number"]
            dropoff_phone_number = data["dropoff_phone_number"]
            external_store_id = data["restaurant"]
            pickup_time = data.get("pickup_time")
            dropoff_time = data.get("dropoff_time")
        else:
            pickup_addr = self.get_uber_address(
                address=order.pickup_address_details, data_type='Order')
            dropoff_addr = self.get_uber_address(
                address=order.dropoff_address_details, data_type='Order')
            pickup_phone_number = order.location.phone if order.location else order.restaurant.phone
            dropoff_phone_number = order.dropoff_phone_number
            external_store_id = order.restaurant_id
            pickup_time = (
                None
                if order.pickup_time is None
                else order.pickup_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )
            dropoff_time = (
                None
                if order.delivery_time is None
                else order.delivery_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )

        customer_id = get_Uber_Credentials.customerId
        url = f"https://api.uber.com/v1/customers/{customer_id}/delivery_quotes"
        headers = get_uber_header()
        payload = {
            "pickup_address": json.dumps(
                {
                    "street_address": pickup_addr.get("street_address"),
                    "state": pickup_addr.get("state"),
                    "city": pickup_addr.get("city"),
                    "zip_code": pickup_addr.get("zip"),
                    "country": pickup_addr.get("country"),
                }
            ),
            "dropoff_address": json.dumps(
                {
                    "street_address": dropoff_addr.get("street_address"),
                    "state": dropoff_addr.get("state"),
                    "city": dropoff_addr.get("city"),
                    "zip_code": dropoff_addr.get("zip"),
                    "country": dropoff_addr.get("country"),
                }
            ),
            "pickup_phone_number": pickup_phone_number,
            "dropoff_phone_number": dropoff_phone_number,
            # "external_store_id": str(external_store_id),
        }

        if pickup_time is not None:
            payload["pickup_ready_dt"] = pickup_time

        if dropoff_time is not None:
            payload["dropoff_ready_dt"] = dropoff_time

        print("uber payload", payload)
        # response = requests.post(url, headers=headers, json=payload)
        response = requests.post(url, headers=headers,
                                 data=json.dumps(payload))

        if response.status_code == 401 or response.status_code == 403:
            get_Uber_jwt()
            self.create_quote(data, order)

        if response.status_code != 200:
            print('retrying to get response from uber --> ', trying)
            if trying <= 6:
                trying += 1
                print("trynumber>", trying)
                self.create_quote(data, order, trying)
            else:
                print("trynumber>response", response)
                return response

        return response

    def get_uber_address(self, address, data_type='dict'):
        """
            For uber api, we need tp pass the street address a bit differently
        :param address: address dictionary
        :return: updated address format for uber api
        """
        fields = ["street_number", "street_name",
                  "city", "state", "zip", "country"]
        if data_type == 'dict':
            updated_address = {
                **address,
                "street_address": [
                    f'{address.get("street_number")} {address.get("street_name")}'
                ],
            }
        else:
            updated_address = {
                "street_address": [
                    f'{address.street_number} {address.street_name}'
                ],
                **{field: getattr(address, field) for field in fields}
            }
        updated_address.pop("street_number")
        updated_address.pop("street_name")
        return updated_address

    def create_delivery(self, data=None, order: Order = None, trying=0):
        if data is not None:
            order_list = data.get("order_list")
            pickup_addr = self.get_uber_address(
                address=data["pickup_address_details"], data_type='dict')
            dropoff_addr = self.get_uber_address(
                address=data["dropoff_address_details"], data_type='dict')
            pickup_phone_number = data["pickup_phone_number"]
            dropoff_phone_number = data["dropoff_phone_number"]
            external_store_id = data["restaurant"]
            tip = data["tip"]
            pickup_name = data.get("pickup_business_name")
            dropoff_name = data.get("customer")
            pickup_time = data.get("pickup_time")
            dropoff_time = data.get("dropoff_time")
            # quote_id = data['quote_id']
        else:
            order_list = list(order.orderitem_set.values(
                "menu_item", "quantity"))
            pickup_addr = self.get_uber_address(
                address=order.pickup_address_details, data_type='Order')
            dropoff_addr = self.get_uber_address(
                address=order.dropoff_address_details, data_type='Order')
            pickup_phone_number = order.location.phone if order.location else order.restaurant.phone
            dropoff_phone_number = order.dropoff_phone_number
            external_store_id = order.restaurant_id
            tip = order.tips
            pickup_name = order.restaurant.name
            dropoff_name = order.customer
            pickup_time = (
                None
                if order.pickup_time is None
                else order.pickup_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )
            dropoff_time = (
                None
                if order.delivery_time is None
                else order.delivery_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            )
            # quote_id = order.order_id

        # pickup_addr = data["pickup_address"]
        # dropoff_addr = data["dropoff_address"]
        manifest_items_list = []
        order_value = 0
        for order_item_data in order_list:
            order_item_id = order_item_data.get("menu_item")
            quantity = order_item_data.get("quantity")
            try:
                menu_item_instance: MenuItem = MenuItem.objects.get(
                    id=order_item_id)
                price_in_cents = int(menu_item_instance.base_price * 100)
                manifest_items_list.append(
                    {
                        "name": menu_item_instance.name[:95]
                        if len(menu_item_instance.name) > 95
                        else menu_item_instance.name,
                        "price": price_in_cents,
                        "quantity": quantity
                        # Add more properties as needed
                    }
                )
                order_value += price_in_cents * int(quantity)
            except:
                pass
        # If you have a token and customer ID stored somewhere, use those.
        
        print(pickup_time, dropoff_time, 'pickup_time, dropoff_time')
        CustomerId = get_Uber_Credentials.customerId
        url = f"https://api.uber.com/v1/customers/{CustomerId}/deliveries"
        headers = get_uber_header()

        payload = {
            # "quote_id": data["quote_id"],
            "pickup_name": pickup_name,
            "pickup_address": json.dumps(
                {
                    "street_address": pickup_addr.get("street_address"),
                    "state": pickup_addr.get("state"),
                    "city": pickup_addr.get("city"),
                    "zip_code": pickup_addr.get("zip"),
                    "country": pickup_addr.get("country"),
                }
            ),
            "dropoff_name": dropoff_name,
            "dropoff_address": json.dumps(
                {
                    "street_address": dropoff_addr.get("street_address"),
                    "state": dropoff_addr.get("state"),
                    "city": dropoff_addr.get("city"),
                    "zip_code": dropoff_addr.get("zip"),
                    "country": dropoff_addr.get("country"),
                }
            ),
            "pickup_phone_number": pickup_phone_number,
            "dropoff_phone_number": dropoff_phone_number,
            "manifest_items": manifest_items_list,
            "tip": int(tip * 100),
            # "external_store_id": str(external_store_id),
            # "test_specifications": {"robo_courier_specification": {"mode": "auto"}},
        }

        if env.str('DOORDASH_ENV', default='TEST') == 'TEST':
            payload["test_specifications"] = {
                "robo_courier_specification": {"mode": "auto"}}

        if pickup_time is not None:
            payload["pickup_ready_dt"] = pickup_time

        if dropoff_time is not None:
            payload["dropoff_ready_dt"] = dropoff_time
            
      
        print("uber payload", payload)
        response = requests.post(url, headers=headers,
                                 data=json.dumps(payload))
        print(response.json())
        if response.status_code == 200:
            delivery = response.json()
            try:
                order.status = Order.StatusChoices.ACCEPTED

                order.pickup_time = make_aware(
                    datetime.datetime.strptime(
                        delivery.get("pickup_eta"),
                        "%Y-%m-%dT%H:%M:%SZ",
                    ), timezone=datetime.timezone.utc
                )
                order.delivery_time = make_aware(
                    datetime.datetime.strptime(
                        delivery.get("dropoff_eta"),
                        "%Y-%m-%dT%H:%M:%SZ",
                    ), timezone=datetime.timezone.utc
                )
                order.uber_delivery_id = delivery.get('id', None)
                order.delivery_fee = (delivery.get('fee', None)) / 100
                # order.support_reference = delivery.get("support_reference", "")
                order.tracking_url = delivery.get("tracking_url", "")
                order.delivery_platform = Order.DeliveryPlatform.UBEREATS
                order.ubereats_requested_time = timezone.now()
                order.save(
                    update_fields=[
                        "status",
                        "delivery_fee",
                        "pickup_time",
                        "delivery_time",
                        # "support_reference",
                        "tracking_url",
                        "delivery_platform",
                        "ubereats_requested_time"
                    ]
                )
            except Exception as e:
                logger.error(f"Order update error --> {e}")
                raise ParseError(f"Order update error --> {e}")
        # else:
        #     raise APIException(response.text, code=response.status_code)

        if response.status_code == 401 or response.status_code == 403:
            get_Uber_jwt()
            self.create_delivery(data, order)

        if response.status_code != 200:
            print('retrying to get response from uber --> ', trying)
            if trying <= 5:
                trying += 1
                self.create_delivery(data, order, trying)
            else:
                return response

        return response

    def cancel_delivery(self, delivery_id):
        """
        Cancel delivery of given id
        """
        customer_id = get_Uber_Credentials.customerId
        url = f"https://api.uber.com/v1/customers/{customer_id}/deliveries/{delivery_id}/cancel"

        headers = get_uber_header()

        response = requests.post(url, headers=headers)

        if response.status_code == 401 or response.status_code == 403:
            get_Uber_jwt()
            self.cancel_delivery(delivery_id)

        return response

    def get_delivery(self, delivery_id):
        """
        :param delivery_id: uber delivery id stored in order
        :return: delivery details from uber api
        """
        customer_id = get_Uber_Credentials.customerId
        url = f'https://api.uber.com/v1/customers/{customer_id}/deliveries/{delivery_id}'
        headers = get_uber_header()
        response = requests.get(url, headers=headers)

        if response.status_code == 401 or response.status_code == 403:
            get_Uber_jwt()
            self.get_delivery(delivery_id)
        return response