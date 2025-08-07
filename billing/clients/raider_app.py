import json

import requests

from billing.api.base.serializers import BaseRaiderAppCheckAddressSerializer
from billing.models import Order, RaiderAppAuth
from hungrytiger.settings import env
from food.models import Restaurant, Location
from datetime import timedelta
from datetime import datetime
from food.models import MenuItem


class Raider_Client:
    print('Raider_Client initialized -------------------->')

    def get_header(self):
        """
        Retrieves the authentication token for API requests.
        """
        token = None
        if not RaiderAppAuth.objects.exists():
            token = self.get_auth()
        else:
            print('Using existing token -------------------->')
            token = RaiderAppAuth.objects.get(id=1).token
            print(token, 'Token retrieved ------------------------>')

        return {
            'Content-Type': 'application/json',
            'Authorization': f'Token {token}',  # Ensure it's `Token` with uppercase `T`
        }

    def get_auth(self):
        """
        Authenticates and retrieves a new token from the Raider API.
        """
        url = f"https://raider.api.chatchefs.com/auth/api/v1/login/email/"
        
        data = {
            "email": env.str('RAIDER_APP_EMAIL'),
            "password": env.str('RAIDER_APP_PASS')
        }
        response = requests.post(url, json=data)  # Use JSON for better API support
        response_data = response.json()
        print(response_data, 'Auth response ------------------------>')

        if "token" in response_data:
            if not RaiderAppAuth.objects.exists():
                RaiderAppAuth.objects.create(token=response_data["token"])
            else:
                obj = RaiderAppAuth.objects.get(id=1)
                obj.token = response_data["token"]
                obj.save()
            return response_data["token"]
        else:
            print("Error: Could not retrieve authentication token")
            return None

    def create_delivery(self, instance: Order):
        """
        Sends a request to the Raider API to create a delivery order.
        Now ensures orders start as WAITING_FOR_DRIVER without a driver assigned.
        """
        print('Raider_Client create_delivery called -------------------->', vars(instance))

        # pickup_time = (
        #     None
        #     if instance.pickup_time is None
        #     else instance.pickup_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # )
        pickup_ready_at = None
        pickup_last_time = None
        pickup_time = None  # ✅ define it first

        if instance.pickup_time:
            pickup_time = instance.pickup_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # ✅ set this first
            pickup_ready_dt = instance.pickup_time - timedelta(minutes=5)
            pickup_ready_at = pickup_ready_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            pickup_last_time = pickup_time  # ✅ reuse

        dropoff_time = (
            None
            if instance.delivery_time is None
            else instance.delivery_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        )
        
        # Add 15 minutes to the pickup time
        if dropoff_time is None:
             dropoff_time = pickup_time
            
        order_list = list(instance.orderitem_set.values(
          "menu_item", "quantity"
        ))
        
        items = []
        order_value = 0
        for order in order_list:
            order_item_id = order.get("menu_item")
            quantity = order.get("quantity")
            try:
                menu_item_instance: MenuItem = MenuItem.objects.get(id=order_item_id)
                price_per_item = menu_item_instance.base_price
                items.append({
                    "name": menu_item_instance.name,
                    "quantity": quantity,
                    "price": price_per_item
                })
                order_value += price_per_item * quantity
            except:
              pass
        
        customer_info = [
          {
            "name": instance.customer,
            "phone": instance.dropoff_phone_number,
            "email": instance.user.email if instance.user and instance.user.email else None,
            "user_id": instance.user.id if instance.user else None,
            "address": {
              "street_address": f"{instance.dropoff_address_details.street_number} {instance.dropoff_address_details.street_name}",
              "city": instance.dropoff_address_details.city,
              "state": instance.dropoff_address_details.state,
              "postal_code": instance.dropoff_address_details.zip,
              "country": instance.dropoff_address_details.country
            }
          }
        ]
            
        print(items, 'Items ------------------------>')
            
        print(dropoff_time, pickup_time, instance.delivery_time, 'Dropoff time ------------------------>')

        url = "https://raider.api.chatchefs.com/delivery/api/v1/create-delivery/"
        # url = "http://127.0.0.1:9000/delivery/api/v1/create-delivery/"

        data = {
            "client_id": str(instance.id),
            "pickup_address": {
                "street_address": f"{instance.pickup_address_details.street_number} {instance.pickup_address_details.street_name}",
                "city": instance.pickup_address_details.city,
                "state": instance.pickup_address_details.state,
                "postal_code": instance.pickup_address_details.zip,
                "country": instance.pickup_address_details.country
            },
            "pickup_customer_name": instance.restaurant.name,
            "pickup_phone": "01980796731",
           "pickup_ready_at": pickup_ready_at,
            "pickup_last_time": pickup_last_time,

            "drop_off_address": {
                "street_address": f"{instance.dropoff_address_details.street_number} {instance.dropoff_address_details.street_name}",
                "city": instance.dropoff_address_details.city,
                "state": instance.dropoff_address_details.state,
                "postal_code": instance.dropoff_address_details.zip,
                "country": instance.dropoff_address_details.country
            },
            "drop_off_customer_name": f"{instance.dropoff_contact_first_name} {instance.dropoff_contact_last_name}",
            "drop_off_phone": instance.dropoff_phone_number,
            "drop_off_last_time": dropoff_time,

            "currency": "bdt",
            "tips": instance.tips,
            "amount": instance.total,
            "payment_type": "cash" if instance.payment_method == Order.PaymentMethod.CASH else "card",
            "pickup_latitude": instance.restaurant.latitude,
            "pickup_longitude": instance.restaurant.longitude,

            # Orders should start as `WAITING_FOR_DRIVER` without a driver assigned
            "status": "waiting_for_driver",
            "assigned": False,
            "items": items,
            "customer_info": customer_info,

             # Pass the On-Time Guarantee Information if opted-in
            "on_time_guarantee_opted_in": instance.on_time_guarantee_opted_in,
            "on_time_guarantee_fee": instance.on_time_guarantee_fee
        }
        print('Raider_Client create_delivery payload -------------------->', data)

        try:
            print('Sending request to Raider API -------------------->' , url, data, self.get_header())
            res = requests.post(
                url,
                headers=self.get_header(),
                json=data,  
                allow_redirects=False
            )
            print(res, 'Raider API Responses ------------------------>133')
            
            if res.status_code != 200:
                print(f"Error creating delivery: {res.status_code}")
                return res

            res_data = res.json()
            print(res_data, 'Raider API Response ------------------------>')

            instance.delivery_platform = Order.DeliveryPlatform.RAIDER_APP
            instance.raider_id = res_data.get("uid")
            instance.save()
            return res

        except requests.exceptions.HTTPError as err:
            print("HTTP error occurred:", err)
        except Exception as e:
            print("An error occurred:", e)

    def cancel_delivery(self, instance: Order):
        """
        Cancels an existing delivery order via the Raider API.
        """
        url = "https://raider.api.chatchefs.com/delivery/api/v1/cancel-delivery/"
        # url = "http://127.0.0.1:9000/delivery/api/v1/cancel-delivery/"
    
        data = {"uid": instance.raider_id}

        res = requests.post(
            url,
            headers=self.get_header(),
            json=data,  
            allow_redirects=False
        )
        return res

    def check_deliverable(self, data=None):
        """
        Checks if an address is deliverable using the Raider API.
        """
        print(data, 'Checking deliverability ------------------------>')

        location = Location.objects.get(id=data.get("location"))
        restaurant = Restaurant.objects.get(id=data.get("restaurant"))
        drop_off = data.get("dropoff_address_details")
        print(restaurant, drop_off.get('city'), 'Restaurant address ------------------------>')

        map_data = {
            "pickup_address": {
                "street_address": f"{location.address.street_number} {location.address.street_name}",
                "city": location.address.city,
                "state": location.address.state,
                "postal_code": location.address.zip,
                "country": location.address.country
            },
            "pickup_customer_name": restaurant.name,
            "drop_off_address": {
                "street_address": f"{drop_off.get('street_number')} {drop_off.get('street_name')}",
                "city": drop_off.get("city"),
                "state": drop_off.get("state"),
                "postal_code": drop_off.get("zip"),
                "country": drop_off.get("country")
            },
            "drop_off_phone": data.get("dropoff_phone_number"),
            "pickup_latitude": restaurant.latitude,
            "pickup_longitude": restaurant.longitude,
            "drop_off_latitude": data.get("lat"),
            "drop_off_longitude": data.get("lng")
        }

        print(map_data, 'Delivery check payload ------------------------>')

        # url = "https://raider.api.chatchefs.com/delivery/api/v1/check-address/"
        url = "http://127.0.0.1:9000/delivery/api/v1/check-address/"
        try:
            res = requests.post(
                url,
                headers=self.get_header(),
                json=map_data,
                allow_redirects=False
            )

            print("res-before-error----->", res, map_data)

            if res.status_code == 200:
                response = res.json()
                response['status_code'] = res.status_code
                return response
            else:
                
                return "We can not deliver to this address-2!"

        except requests.exceptions.HTTPError as err:
            print("HTTP error occurred:", err)
        except Exception as e:
            print("An error occurred:", e)