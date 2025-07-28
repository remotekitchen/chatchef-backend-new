import datetime
import uuid

import requests
from django.utils.timezone import make_aware
from rest_framework.exceptions import ParseError, APIException
from rest_framework.generics import get_object_or_404

from billing.models import Order
from billing.utiils import get_doordash_headers
from core.utils import get_logger
from food.models import MenuItem, Restaurant
from billing.utiils import calculate_pickup_and_dropoff_times

logger = get_logger()


class DoordashClient:
    print("DoordashClient called")
    def create_quote(self, data=None, order: Order = None):
        item_list = []
        order_value = 0
        restaurant_timezone = Restaurant.objects.get(
            id=data["restaurant"]).timezone
        print('restaurant_timezone', restaurant_timezone)
        times = calculate_pickup_and_dropoff_times(order, restaurant_timezone)
        pickup_time = times["pickup_time"]
        dropoff_time = times["dropoff_time"]
        print('pickup_time-------------------------------->100', pickup_time, dropoff_time, times)

        if data is not None:
            order_list = data.get("order_list")
            dropoff_address = data.get("dropoff_address")
            dropoff_phone_number = data.get("dropoff_phone_number")
            pickup_address = data.get("pickup_address")
            pickup_business_name = data.get("pickup_business_name")
            first_name = data.get("first_name", "")
            last_name = data.get("last_name", "")
            uuid_str = uuid.uuid4()
            external_delivery_id = f"{str(uuid_str)}"
            pickup_phone_number = data.get("pickup_phone_number", "")
            pickup_time = data.get("pickup_time", None)
            dropoff_time = data.get("delivery_time", None)
            tip = data.get("tips", None)
            pickup_external_store_id = data.get("pickup_external_store_id", "")
            if pickup_address is None:
                pickup_address = self.get_address_string(
                    data.get("pickup_address_details"), data_type='dict'
                )
            if dropoff_address is None:
                dropoff_address = self.get_address_string(
                    data.get("dropoff_address_details"), data_type='dict'
                )
        else:
            order_list = list(order.orderitem_set.values("menu_item", "quantity"))
            dropoff_address = order.dropoff_address
            dropoff_phone_number = order.dropoff_phone_number
            pickup_address = order.pickup_address
            pickup_business_name = order.restaurant.name
            first_name = order.dropoff_contact_first_name
            last_name = order.dropoff_contact_last_name
            external_delivery_id = f"{order.doordash_external_delivery_id}"
            pickup_phone_number = (
                order.location.phone if order.location.phone else order.restaurant.phone
            )
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
            tip = order.tips
            pickup_external_store_id = (
                order.location.doordash_external_store_id
                if order.location.doordash_external_store_id
                else order.restaurant.doordash_external_store_id
            )
            if pickup_address is None:
                pickup_address = self.get_address_string(
                    order.pickup_address_details, data_type='Order'
                )
            if dropoff_address is None:
                dropoff_address = self.get_address_string(
                    order.dropoff_address_details, data_type='Order'
                )

        for order_item_data in order_list:
            order_item_id = order_item_data.get("menu_item")
            quantity = order_item_data.get("quantity")
            try:
                menu_item_instance: MenuItem = MenuItem.objects.get(id=order_item_id)
                price_in_cents = int(menu_item_instance.base_price * 100)
                item_list.append(
                    {
                        "name": menu_item_instance.name[:95]
                        if len(menu_item_instance.name) > 95
                        else menu_item_instance.name,
                        "description": menu_item_instance.description[:400],
                        "external_id": f"item::{menu_item_instance.id}",
                        "price": price_in_cents,
                        "quantity": quantity
                        # Add more properties as needed
                    }
                )
                order_value += price_in_cents * int(quantity)
            except:
                pass

        # return Response(serializer.data)

        request_body = {
            "external_delivery_id": external_delivery_id,
            "dropoff_contact_given_name": first_name,
            "dropoff_contact_family_name": last_name,
            "dropoff_address": dropoff_address,
            "dropoff_phone_number": dropoff_phone_number,
            "pickup_address": pickup_address,
            "pickup_business_name": pickup_business_name,
            "items": item_list,
            "order_value": order_value,
            "pickup_phone_number": pickup_phone_number,
        }
        

        if pickup_external_store_id is not None and pickup_external_store_id != "":
            request_body["pickup_external_business_id"] = "default"
            request_body["pickup_external_store_id"] = pickup_external_store_id

        if pickup_time is not None:
            request_body["pickup_time"] = pickup_time

        if dropoff_time is not None:
            request_body["dropoff_time"] = dropoff_time

        if tip is not None:
            request_body["tip"] = tip
        


        headers = get_doordash_headers()
        create_quote_url = "https://openapi.doordash.com/drive/v2/quotes"
        logger.info(f"Create quote request: {request_body}")
        create_quote = requests.post(
            create_quote_url, headers=headers, json=request_body
        )
        
        
        if create_quote.status_code != 200:
          logger.error(f"Doordash API response: {create_quote.json()}")
          raise APIException(create_quote.json(), code=create_quote.status_code)
        return create_quote

    def get_address_string(self, address, data_type='dict'):
        fields = ["street_number", "street_name", "city", "state", "zip", "country"]
        return ",".join([address.get(field) if data_type == 'dict' else getattr(address, field) for field in fields])

    def accept_quote(self, data=None, order: Order = None):
        if data is not None:
            external_delivery_id = data.get("external_delivery_id")
            order = get_object_or_404(Order, id=data.get("doordash_external_delivery_id"))
        else:
            external_delivery_id = f"{order.doordash_external_delivery_id}"

        accept_quote_url = f"https://openapi.doordash.com/drive/v2/quotes/{external_delivery_id}/accept"
        headers = get_doordash_headers()
        response = requests.post(accept_quote_url, headers=headers)
        accepted_quote = response.json()

        if response.status_code == 200:
            try:
                # calculator = CostCalculation()
                # costs = calculator.get_updated_cost(
                #     OrderItemSerializer(instance=order.orderitem_set, many=True).data,
                #     delivery=accepted_quote.get('fee', 0)
                # )
                order.status = Order.StatusChoices.ACCEPTED
                # order.delivery_fee = costs.get('delivery_fee')
                # order.tax = costs.get('tax')
                # order.convenience_fee = costs.get('convenience_fee')
                # order.total = costs.get('total')
                order.pickup_time = make_aware(
                    datetime.datetime.strptime(
                        accepted_quote.get("pickup_time_estimated"),
                        "%Y-%m-%dT%H:%M:%SZ",
                    ), timezone=datetime.timezone.utc
                )
                order.delivery_time = make_aware(
                    datetime.datetime.strptime(
                        accepted_quote.get("dropoff_time_estimated"),
                        "%Y-%m-%dT%H:%M:%SZ",
                    ), timezone=datetime.timezone.utc
                )
                order.support_reference = accepted_quote.get("support_reference", "")
                order.tracking_url = accepted_quote.get("tracking_url", "")
                order.delivery_platform = Order.DeliveryPlatform.DOORDASH
                order.save(
                    update_fields=[
                        "status",
                        "delivery_fee",
                        "pickup_time",
                        "delivery_time",
                        "support_reference",
                        "tracking_url",
                        "delivery_platform"
                    ]
                )
            except Exception as e:
                logger.error(f"Order update error --> {e}")
                raise ParseError(f"Order update error --> {e}")
        return response

    def cancel_delivery(self, external_delivery_id):
        cancel_delivery_endpoint = f"https://openapi.doordash.com/drive/v2/deliveries/{external_delivery_id}/cancel"
        headers = get_doordash_headers()
        response = requests.put(cancel_delivery_endpoint, headers=headers)
        # cancelled_order = response.json()
        return response

    def create_store(self, name, address, phone=None):
        uid = str(uuid.uuid4())
        request_body = {"external_store_id": uid, "name": name, "address": address}
        if phone is not None and phone != "":
            request_body["phone_number"] = phone

        url = "https://openapi.doordash.com/developer/v1/businesses/default/stores"
        headers = get_doordash_headers()
        response = requests.post(url, headers=headers, json=request_body)
        return response

    def get_delivery(self, external_delivery_id):
        url = f"https://openapi.doordash.com/drive/v2/deliveries/{external_delivery_id}"
        headers = get_doordash_headers()
        response = requests.get(url, headers=headers)
        return response

    def create_delivery(self, instance: Order):
        created_quote = self.create_quote(order=instance)
        if created_quote.status_code != 200:
            raise APIException(created_quote.json(), code=created_quote.status_code)
        accepted_quote = self.accept_quote(order=instance)
        if accepted_quote.status_code != 200:
            raise APIException(accepted_quote.json(), code=accepted_quote.status_code)
