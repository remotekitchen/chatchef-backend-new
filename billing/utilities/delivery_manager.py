import json
import time
from rest_framework.exceptions import ParseError

from billing.clients.doordash_client import DoordashClient
from billing.clients.raider_app import Raider_Client
print("Raider_Client imported successfully")
from billing.clients.uber_client import UberClient
from billing.models import Order
from Event_logger.models import Action_logs
from Event_logger.utils import action_saver


class DeliveryManager:
    def create_quote(self, data=None, order: Order = None):
        print('data------------------------> 13', data)
        # print('order.dropoff_address_details.country', order.dropoff_address_details.country)
        def is_raider_app_country(country: str) -> bool:
          # List of countries where Raider App operates
          RAIDER_APP_COUNTRIES = ["bangladesh"]
          return country.lower() in RAIDER_APP_COUNTRIES
        
        if is_raider_app_country(data.get('pickup_address_details').get('country')):
            delivery_platform = (
            order.delivery_platform
            if order is not None
            else data.get('delivery_platform', Order.DeliveryPlatform.RAIDER_APP.value)
        )
        else:
            delivery_platform = (
            order.delivery_platform
            if order is not None
            else data.get('delivery_platform', Order.DeliveryPlatform.UBEREATS)
        )
        # delivery_platform = (
        #     order.delivery_platform
        #     if order is not None
        #     else data.get('delivery_platform', Order.DeliveryPlatform.RAIDER_APP)
        # )
        if delivery_platform == Order.DeliveryPlatform.RAIDER_APP:
            print("raider app called start")
            data["pickup_address_details"] = data.get("pickup_address_details")
            print(data, 'raider----------------------------->100')
            raider = Raider_Client()
            print("----->", data,'raider----------------------------->24')
            responses = raider.check_deliverable(data)
            print('raider----------------------------->26')
            fee = 0
            
            # print(status_code, response, 'raider----------------------------->27')
            print(responses, 'responses----------------------------->27')
            
            print('raider----------------------------->28' , responses)
            if not isinstance(responses, dict):
                print("❌ Raider returned error string:", responses)
                return responses  # or raise an APIException here if you want to stop immediately
            if responses != "We can not deliver to this address!":
                fee = responses.get("fees")
            results = {
                "fee": fee,
                "data": responses,
                "delivery_platform": delivery_platform,
                "status_code": responses.get("status_code")
            }
            if responses == "We can not deliver to this address!":
                results['errors'] = "We can not deliver to this address!"
            
          
            return results

        fee = 1e9
        responses = {}
        statuses = 400
        errors = {}
        
        if delivery_platform is None or delivery_platform == Order.DeliveryPlatform.UBEREATS:
            print('uber called')
            uber = UberClient()
            # payload = data.copy()
            # payload["pickup_address"] = self.get_uber_address(
            #     data.get("pickup_address_details") if data else order.pickup_address_details,
            #     data_type='dict' if data is not None else 'Order'
            # )
            # payload["dropoff_address"] = self.get_uber_address(
            #     data.get("dropoff_address_details") if data else order.dropoff_address_details,
            #     data_type='dict' if data is not None else 'Order'
            # )
            response = uber.create_quote(data=data, order=order)
            print('ud', response.status_code, response.text)
            uber_data = response.json()
            responses = uber_data
            statuses = response.status_code
            if statuses < 400:
                fee = uber_data.get("fee", 0)
            else:
                errors["uber"] = uber_data
            delivery_platform = Order.DeliveryPlatform.UBEREATS
            print(uber_data)

        # if delivery_platform is None or delivery_platform == Order.DeliveryPlatform.DOORDASH:
        if statuses >= 400:
            # data["pickup_address"] = self.get_address_string(
            #     data.get("pickup_address_details") if data else order.pickup_address_details,
            #     data_type='dict' if data is not None else 'Order'
            # )
            # data["dropoff_address"] = self.get_address_string(
            #     data.get("dropoff_address_details") if data else order.dropoff_address_details,
            #     data_type='dict' if data is not None else 'Order'
            # )
            doordash = DoordashClient()
            response = doordash.create_quote(data=data, order=order)
            doordash_data = response.json()
            print(response.status_code)
            statuses = response.status_code
            if response.status_code >= 400:
                errors["doordash"] = doordash_data
            else:
                doordash_fee = doordash_data.get("fee", 0)

                if doordash_fee < fee:
                    fee = doordash_fee
                    responses = doordash_data
                    delivery_platform = Order.DeliveryPlatform.DOORDASH

        print(errors)
        # if statuses >= 400:
        #     raise ParseError(errors)

        fee = 0 if fee == 1e9 else fee
        results = {
            "fee": fee,
            "data": responses,
            "status": statuses,
            "delivery_platform": delivery_platform,
        }
        if statuses >= 400:
            results['errors'] = errors
        print('results', results)
        return results
# test
    def get_uber_address(self, address, data_type='dict'):
        """
        For uber api, we need tp pass the street address a bit differently
        :param address: address dictionary
        :return: updated address format for uber api
        """
        updated_address = {
            **address,
            "street_address": [
                f'{address.get("street_number")} {address.get("street_name")}' if data_type == 'dict'
                else f'{address.street_number} {address.street_name}'
            ],
        }
        updated_address.pop("street_number")
        updated_address.pop("street_name")
        return updated_address

    def get_address_string(self, address, data_type='dict'):
        fields = ["street_number", "street_name",
                  "city", "state", "zip", "country"]
        return ",".join([address.get(field) if data_type == 'dict' else getattr(address, field) for field in fields])

    def create_delivery(self, data=None, order: Order = None, action=None):
        """
        Create delivery with rider app, uber or doordash depending on the passed platform
        """
        print('create_delivery called --------------------------> 156')
        status = 200     
        if action is None:
            action = Action_logs.objects.create(
                action='Creating Delivery from delivery manager', logs='creating action')
            action.restaurant = order.restaurant
            action.location = order.location
            action.save()
            
        action_saver(action, f'order id {order.id}')
        

        
        if order.delivery_platform == Order.DeliveryPlatform.RAIDER_APP:
            print("Raider App called for delivery------------>159")
            raider = Raider_Client()
            delivery = raider.create_delivery(instance=order)
            print("Raider delivery created -----------------> 163", delivery)

            if delivery.status_code == 200:
                print("Raider App delivery created successfully!")
            else:
                print(f"Failed to create Raider App delivery: {delivery.text}")
        max_retries = 5
        retry_delay = 5
        if order.delivery_platform == Order.DeliveryPlatform.UBEREATS:
            uber = UberClient()
            # action saver
            action_saver(action, f'Uber delivery id {order.order_id}')
            print('data-------------------_>', data, order)
            delivery = uber.create_delivery(data=data, order=order)
            
            print('delivery---------->', delivery.status_code, delivery.text)
            if delivery.status_code >= 400:
                print('delivery', delivery.status_code, delivery.text)
                for i in range(max_retries):
                    # action saver
                    action_saver(action, f'Uber delivery id {order.order_id}')
                    delivery = uber.create_delivery(data=data, order=order)
                    if delivery.status_code < 400:
                        break
                    # action saver
                    action_saver(action, f'Uber delivery id {order.order_id}')
                    print(f"Failed to create Uber delivery: {delivery.text}")
                    time.sleep(retry_delay)

        if order.delivery_platform == Order.DeliveryPlatform.DOORDASH or status >= 400:
            doordash = DoordashClient()
            delivery = doordash.create_delivery(instance=order)
        
        print('delivery------>', delivery.status_code, delivery.text)
        return delivery

    def cancel_delivery(self, instance: Order):
        if instance.delivery_platform == Order.DeliveryPlatform.DOORDASH:
            doordash = DoordashClient()
            response = doordash.cancel_delivery(
                external_delivery_id=instance.order_id)
        elif instance.delivery_platform == Order.DeliveryPlatform.UBEREATS:
            uber = UberClient()
            response = uber.cancel_delivery(
                delivery_id=instance.extra.get("uber_delivery_id"))
        elif instance.delivery_platform == Order.DeliveryPlatform.RAIDER_APP:
            raider = Raider_Client()
            response = raider.cancel_delivery(instance=instance)
        return response
