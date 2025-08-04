from rest_framework.exceptions import APIException

from billing.api.base.views import (BaseDoordashCreateQuoteAPIView,
                                    BaseOrderRetrieveUpdateDestroyAPIView,
                                    BaseOTPSendAPIView,
                                    BaseStripePaymentAPIView,
                                    BaseVerifyOTPAPIView,BaseCostCalculationAPIView,BaseRemotekitchenOrderAPIView)
from billing.api.v2.serializers import (CreateQuoteSerializer, OrderSerializer,
                                        PhoneVerifySerializer,
                                        StripePaymentSerializer)
from billing.models import Order
from billing.utilities.delivery_manager import DeliveryManager
from rest_framework.response import Response
from rest_framework import status
from billing.clients.raider_app import Raider_Client
from rest_framework.views import APIView
from billing.clients.doordash_client import DoordashClient
from rest_framework.exceptions import (APIException, NotFound, ParseError,
                                       PermissionDenied)
from billing.utilities.cost_calculation import CostCalculation
from billing.utilities.distance import calculate_haversine_distance
from accounts.models import Otp, RestaurantUser
from billing.tasks import order_reminder_setter
from billing.api.base.serializers import (
   
    BaseOrderGetSerializerWithModifiersDetails,OrderReminderSerializer)
from django.utils.timezone import make_aware
from reward.models import Reward, UserReward, LocalDeal
from marketing.models import Voucher
from hungrytiger.settings import ENV_TYPE, env
from datetime import datetime, timedelta
from analytics.api.base.utils import create_visitor_analytics
from marketing.utils.send_sms import send_sms_bd
from food.models import MenuItem, Restaurant, Location
from remotekitchen.utils import get_delivery_fee_rule
from billing.models import RestaurantContract
from core.utils import get_logger
import pytz
from django.utils import timezone
from rest_framework.permissions import  IsAuthenticated
from accounts.permissions import IsNotBlocked

logger = get_logger()

class CreateQuoteAPIView(BaseDoordashCreateQuoteAPIView):
    serializer_class = CreateQuoteSerializer

    # permission_classes = [IsAuthenticated]

    def get_quote_data(self, data):
        delivery_manager = DeliveryManager()
        print(data, 'data -------------------------------------> 26')
        create_quote = delivery_manager.create_quote(data=data)
        print(create_quote, 'create_quote -------------------------------------> 28')
        fee = create_quote.get("fee", 0)
        create_quote_data = create_quote.get("data")
        api_status = create_quote.get("status")
        create_quote_data["delivery_platform"] = create_quote["delivery_platform"]
        create_quote_data["fee"] = fee
        data["delivery_platform"] = create_quote["delivery_platform"]
        return create_quote, api_status

class RemoteKitchenCreateDeliveryAPIView(APIView):
    def post(self, request, *args, **kwargs):
        # Step 1: Validate the incoming data
        data = request.data
        print(data, 'data ------------------------> 19')
        
        serializer = OrderSerializer(data=data)
        if not serializer.is_valid():
            return Response({"errors": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        # Step 2: Save or fetch the Order instance
        try:
            print("Validated Data:", serializer.validated_data)
            order = serializer.save()
            print("Saved Order ID:", order.id)

            # Fetch the full order instance with related fields
            order = Order.objects.select_related(
                "restaurant", "pickup_address_details", "dropoff_address_details"
            ).get(id=order.id)
           
            print("Full Order Object:", order)
         
        except Exception as e:
            print("Order Save Error:", e)
            return Response({"errors": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Step 3: Call Raider_Client to create the delivery
        try:
            raider_client = Raider_Client()

            # Create the delivery
            print("Creating Delivery for Order:", order)
            response = raider_client.create_delivery(instance=order)
            print("Delivery Response Status:", response.status_code)
            print("Delivery Response Body:", response.json())

            # If Raider_Client returns a failure response
            if response.status_code != 200:
                return Response(
                    {"errors": "Failed to create delivery", "details": response.json()},
                    status=response.status_code
                )

            # Step 4: Update the Order instance with delivery details
            res_data = response.json()
            order.raider_id = res_data.get("uid")
            order.delivery_platform = Order.DeliveryPlatform.RAIDER_APP
            order.status = Order.StatusChoices.WAITING_FOR_DRIVER
            order.save(update_fields=["raider_id", "delivery_platform", "status"])

            # Step 5: Return a success response
            return Response(
                {"message": "Delivery created successfully", "delivery_details": res_data},
                status=status.HTTP_201_CREATED
            )
        except Exception as e:
            print("Delivery Creation Error:", e)
            return Response({"errors": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class StripePaymentAPIView(BaseStripePaymentAPIView):
    def get_delivery_fee(self, data):
        delivery_manager = DeliveryManager()
        create_quote = delivery_manager.create_quote(data=data)
        fee = int(create_quote.get("fee", 0)) / 100
        delivery_platform = create_quote["delivery_platform"]
        if create_quote.get('status') >= 400:
            raise APIException(create_quote.get('errors'),
                               code=create_quote.get('status'))
        return fee, delivery_platform

    def get_serializer_class(self):
        order_method = self.request.data.get("order_method", "delivery")
        if self.is_delivery(order_method):
            return StripePaymentSerializer
        return OrderSerializer


class OrderRetrieveUpdateDestroyAPIView(BaseOrderRetrieveUpdateDestroyAPIView):
    def create_delivery(self, instance: Order):
        """
            Overriding create delivery to use dynamic platform
        """
        print('Overriding create delivery to use dynamic platform')
        delivery_manager = DeliveryManager()
        # response = delivery_manager.create_quote(order=instance)
        # if response.get("status") >= 400:
        #     raise APIException(response.get('errors'), code=response.get('status'))
        delivery = delivery_manager.create_delivery(order=instance)

        if delivery and "status_code" in delivery and delivery.status_code >= 400:
            raise APIException(delivery.json(), code=delivery.status_code)
        if instance.delivery_platform == Order.DeliveryPlatform.UBEREATS:
            instance.extra.update(
                {
                    'uber_delivery_id': delivery.json().get('id')
                }
            )

    def cancel_delivery(self, instance: Order):
        delivery_manager = DeliveryManager()
        delivery_manager.cancel_delivery(instance=instance)




class CostCalculationAPIView(BaseCostCalculationAPIView):
    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = request.data

        order_method = data.get("order_method", "pickup")
        data["orderitem_set"] = data.get("items", [])
        print(order_method)

        if self.is_delivery(order_method) and not data.get("delivery_fee"):
            fee, distance = self.get_delivery_fee(data=data)
            data["delivery_fee"] = fee
            data["distance"] = distance

        print("line --> 928 ", data)

        calculator = CostCalculation()
        costs = calculator.get_updated_cost(
            order_list=data.get("items"),
            voucher=data.get("voucher"),
            location=data.get("location"),
            order_method=data.get("order_method", "delivery"),
            spend_x_save_y=data.get("spend_x_save_y"),
            delivery=data.get("delivery_fee", 0),
            is_bag=data.get("is_bag"),
            is_utensil=data.get("utensil_quantity"),
            tips_for_restaurant=data.get("tips_for_restaurant"),
            bogo_id=data.get("bogo"),
            bxgy_id=data.get("bxgy"),
            user=request.user,
            delivery_platform=data.get("delivery_platform", "uber"),
            on_time_guarantee_opted_in=data.get("on_time_guarantee_opted_in", False),
        )
        return Response(costs)

    def get_delivery_fee(self, data):
        if len(data.get("orderitem_set", [])) == 0:
            raise ParseError("Passing items required!")

        platform = data.get("delivery_platform", "uber")

        if platform == Order.DeliveryPlatform.RAIDER_APP:
            raider = DeliveryManager()
            quote = raider.create_quote(data=data)

            if quote.get("status_code") != 200:
                raise APIException(quote.json(), code=quote.get("status_code"))

            # distance = quote.get("data", {}).get("distance")
            # print("📏 Raider distance:", distance)

            # Restaurant coordinates
            restaurant_id = data.get("restaurant")
            restaurant = Restaurant.objects.get(id=restaurant_id)
            pickup_lat = restaurant.latitude
            pickup_lng = restaurant.longitude

            # User coordinates from query params
            # user_lat = self.request.query_params.get("lat")
            # user_lng = self.request.query_params.get("lng")
            user_lat = data.get("lat")
            user_lng = data.get("lng")


            if not user_lat or not user_lng:
                raise ParseError("Missing lat/lng in query parameters.")

            # Haversine distance same as restaurant list
            distance = calculate_haversine_distance(
                pickup_lat,
                pickup_lng,
                user_lat,
                user_lng
            )

            if distance is None:
                raise ParseError("Failed to calculate distance.")

            print("📏 Haversine distance:", distance)

            try:
                distance = float(distance)
                if distance <= 3:
                    fee = 0
                else:
                    fee = 25 + (distance - 3) * 12
                return round(fee, 2), round(distance, 2)
            except (TypeError, ValueError):
                raise ParseError("⚠️ Invalid delivery distance received for raider_app.")

        doordash = DoordashClient()
        quote = doordash.create_quote(data=data)

        if quote.status_code != 200:
            raise APIException(quote.json(), code=quote.status_code)

        fee = quote.json().get("fee", 0) / 100
        return round(fee, 2), None



class RemotekitchenOrderAPIView(BaseRemotekitchenOrderAPIView):
    permission_classes = [IsAuthenticated,IsNotBlocked]
    def post(self, request, *args, **kwargs):
        logger.info("Child view POST called")


        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = request.user
        # data = serializer.data.copy()
        data = {**request.data, **serializer.data}

        order_list = request.data.get("order_list", [])
        solid_voucher_code= request.data.get("voucher","")
        data["voucher"] = request.data.get("voucher", "")
        logger.info(f"order data {data}")
        print(request.data, '-----------------------------------> 7100')
        order_method = data.get("order_method", "delivery")
        # Only handle local deal orders here
        if order_method == "local_deal":
            return self.handle_local_deal_order(request, request.data)
          
        fee, delivery_platform = 0, Order.DeliveryPlatform.RAIDER_APP
        order_method = data.get("order_method", "delivery")
        restaurant_id = data.get("restaurant")
        restaurant = Restaurant.objects.get(id=restaurant_id)
        
        # Calculate restaurant_receive_time
        restaurant_timezone_offset = restaurant.timezone
        hours, minutes = map(int, restaurant_timezone_offset.split(":"))
        total_offset_in_minutes = hours * 60 + (minutes if hours >= 0 else -minutes)
        restaurant_timezone = pytz.FixedOffset(total_offset_in_minutes)
        restaurant_receive_time = timezone.now().astimezone(restaurant_timezone)
        
        print(data, '-----------------------------------> 7100')
        if self.is_delivery(order_method):
            fee, delivery_platform = self.get_delivery_fee(data=data, user=user)

        #  Add on-time guarantee fee separately
        on_time_guarantee_fee = 0
        if request.data.get("on_time_guarantee_opted_in") in ["true", True, "1", 1]:
            on_time_guarantee_fee = float(request.data.get("on_time_guarantee_fee", 6))
            data["on_time_guarantee_opted_in"] = True
            data["on_time_guarantee_fee"] = on_time_guarantee_fee
        else:
            data["on_time_guarantee_opted_in"] = False
            data["on_time_guarantee_fee"] = 0
        
        cost_fields = self.get_costs(
            data=data, delivery_fee=fee, delivery_platform=delivery_platform, on_time_guarantee_fee=on_time_guarantee_fee,user=user
        )
        
        data.update(cost_fields)
        voucher_code = data.get("voucher", "")
        print("voucher_code",voucher_code)

        # Check if the voucher code is already used
        if voucher_code:
            voucher = Voucher.objects.filter(voucher_code=voucher_code).first()
            print(voucher, 'voucher-----------------------------------> 7800000')

            if not voucher:
                return Response({"error": "Invalid voucher code."}, status=status.HTTP_400_BAD_REQUEST)

            # Check one-time use (for all users)
            if voucher.is_one_time_use:
                if Order.objects.filter(voucher=voucher).exists():
                    return Response({"error": "This voucher has already been used."}, status=status.HTTP_400_BAD_REQUEST)
            print(voucher.max_uses, 'voucher.max_uses-----------------------------------> 7800')
            # Check max use per user
            if voucher.max_uses and voucher.max_uses > 0:
                usage_count = Order.objects.filter(user=user, voucher=voucher).count()
                print(usage_count, 'usage_count-----------------------------------> 7800')
                if usage_count >= voucher.max_uses:
                    return Response({"error": "You have already used this voucher the maximum allowed times."}, status=status.HTTP_400_BAD_REQUEST)
        else:
            voucher = None  # No voucher provided

        # Apply special HungryTiger discount
        if voucher and voucher.is_ht_voucher:
            data["discount_hungrytiger"] = cost_fields.get("discount", 0)

        #  Auto-deduct any unclaimed On-Time Guarantee rewards
        auto_reward = (
            UserReward.objects
            .filter(
                user=user,
                is_claimed=False,
                expiry_date__gte=timezone.now().date(),
                reward__reward_group__name="On-Time Delivery Guarantee"
            )
            .order_by("created_date")
            .first()
        )

        auto_applied_reward_info = None


        if auto_reward:
            print(f" Found On-Time Guarantee reward ID={auto_reward.id}")

            reward = auto_reward.reward

            # Calculate reward amount
            if reward.offer_type == Reward.OfferType.FLAT:
                reward_amount = reward.amount
            elif reward.offer_type == Reward.OfferType.PERCENTAGE:
                reward_amount = (data["subtotal"] * reward.amount) / 100
            else:
                reward_amount = 0

            # Safeguard
            if reward_amount <= 0:
                print(" Reward amount was 0 or negative. Skipping.")
            else:
                # Deduct from total
                # old_total = data["total"]
                # data["total"] = max(old_total - reward_amount, 0)
                data["used_on_time_reward"] = reward_amount

                # Mark as claimed
                auto_reward.is_claimed = True
                auto_reward.save(update_fields=["is_claimed"])

                print(
                    f"Applied On-Time Guarantee reward: -{reward_amount} Tk. "
                    
                )

                auto_applied_reward_info = {
                    "amount": reward_amount,
                    "code": auto_reward.code,
                    "reward_id": auto_reward.id,
                    "message": "On-Time Delivery Guarantee reward applied automatically."
                }

                
        else:
            print(" No unclaimed On-Time Guarantee reward to auto-apply.")



        try:
            if "set_reminder" in data:
                reminder_data = data.get("set_reminder")
                sr = OrderReminderSerializer(data=reminder_data)
                sr.is_valid(raise_exception=True)
                obj = sr.save()
                data.pop("set_reminder")
                obj.order_data = {"data": data}
                obj.save()
                result = order_reminder_setter.delay(obj.id)
                
                return Response({"message": "reminder set successfully"})
            # test commit
            # checking scheduled order
            is_scheduled_order, data = self.check_scheduled_order(data)

            is_restaurant_closed = self.check_store(data)

            if (
                    ENV_TYPE != "DEVELOPMENT"
                    and is_restaurant_closed
                    and not is_scheduled_order
            ):
                return Response(
                    {"message": "store closed!, please try scheduled order"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if data.get('order_method') == "pickup":
                data.pop("delivery_platform")

            order_serializer = OrderSerializer(data=data, context={"request": request})            
            order_serializer.is_valid(raise_exception=True)
            order = order_serializer.save()
            print(order, 'order----------------------------------->687')
            _sr = BaseOrderGetSerializerWithModifiersDetails(order)
            print(_sr.data, '-----------------------------------> 7200')
            order_data = _sr.data.copy().get("orderitem_set")
            order.order_item_meta_data = order_data
            contract = RestaurantContract.objects.filter(restaurant_id=restaurant.id).first()
            order.commission_percentage = contract.commission_percentage if contract else 0
            commission_rate = float(contract.commission_percentage if contract else 0) / 100

            order.commission_amount = round(order.subtotal * commission_rate, 2)

            # order.restaurant_discount = 88

            def calculate_actual_item_price(item):
                base_price = item["menu_item"]["base_price"]
                quantity = item["quantity"]

                bogo_inflation_percent = item.get("bogo_details", {}).get("inflate_percent", 40)

                if item.get("is_bogo"):
                    actual_unit_price = base_price / (1 + (bogo_inflation_percent / 100))
                    actual_price = actual_unit_price * quantity
                    return round(actual_price, 2)

                return round(base_price * quantity, 2)

                total_item_price = 0
            
            base_price = 0
            total_item_price = 0

            for item in order.order_item_meta_data:
                base_price = item["menu_item"]["base_price"]
                quantity = item["quantity"]

                # If BOGO, count only half the quantity (since customer pays for one)
                if item.get("is_bogo"):
                    paid_quantity = quantity // 2
                else:
                    paid_quantity = quantity

                item_total = base_price * paid_quantity

                print(f"[DEBUG] Item: Base Price = {base_price}, Quantity = {quantity}, Paid Quantity = {paid_quantity}, Total = {item_total}")

                total_item_price += item_total



            total_actual_price = sum(calculate_actual_item_price(item) for item in order.order_item_meta_data)
            
            bogo_discount_loss_calculation =  total_actual_price - total_item_price

            if contract is not None:
                restaurant_bogo_bear = round(
                    bogo_discount_loss_calculation * float(contract.bogo_bear_by_restaurant or 0),
                    2
                )
            else:
            # handle missing contract scenario, e.g., set to 0
                restaurant_bogo_bear = 0

            restaurant_bogo_bear = round(
                bogo_discount_loss_calculation * (float(contract.bogo_bear_by_restaurant) if contract else 0),
                2
            )


            hungrytiger_bogo_bear = round(bogo_discount_loss_calculation - restaurant_bogo_bear, 2)
            bogo_loss = total_actual_price - total_item_price
            voucher_discount = order.discount - order.bogo_discount
            main_discount = voucher_discount + bogo_loss
            print("restaurant_discount_portion", main_discount, voucher_discount, bogo_loss)
                # order.subtotal
            ht_voucher = float(getattr(order, 'discount_hungrytiger', 0) or 0)
            restaurant_percent = float((contract.ht_voucher_percentage_borne_by_restaurant or 0) if contract else 0)

            restaurant_bears_ht_voucher = ht_voucher * restaurant_percent

            restaurant_discount_portion = (main_discount - ht_voucher) * float(contract.restaurant_discount_percentage if contract else 0)

            restaurant_discount = restaurant_discount_portion + restaurant_bears_ht_voucher

            ht_discount = main_discount - restaurant_discount

            restaurant_discount_result = restaurant_discount_portion + restaurant_bears_ht_voucher

            order.restaurant_discount = restaurant_discount_result
            order.solid_voucher_code = solid_voucher_code 
            print("result----new-field-----200", restaurant_discount_result, restaurant_discount_portion, restaurant_bears_ht_voucher)
        
            order.receive_date = make_aware(datetime.strptime(
                restaurant_receive_time.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
            order.payment_method = "cash"
            if request.user.is_authenticated:
              order.user = request.user
            order.is_new = True
            print(Voucher.objects.filter(
                      voucher_code=data["voucher"]
                    ), data["voucher"], '-----------------------------------> 7800'
                    )
            
            try:
                # voucher_code = data.get("voucher")
                # print("voucher_code", voucher_code)
                # if voucher_code:
                #     voucher_qs = Voucher.objects.filter(voucher_code=voucher_code)
                #     if user:
                #         voucher_qs = voucher_qs.filter(user=user)

                #     voucher = voucher_qs.first()

                if voucher:
                    order.voucher = voucher
                    print("Voucher data added")
                # if voucher.is_one_time_use:
                #     voucher.applied_users.add(order.user)

            except Exception as error:
                pass
            order.save()


            # "Traffic Monitoring"
            create_visitor_analytics(
                order.restaurant.id, order.location.id, source="na", count="order_confirm", user=order.user.id if order.user else None)
            
            order_items_info = ""
            print("order_item_meta_data:", order.order_item_meta_data)

            for item in order.orderitem_set.all():
                name = item.menu_item.name
                quantity = item.quantity
                order_items_info += f"name: {name}, quantity: {quantity}; "

            # Remove the trailing comma and space
            order_items_info = order_items_info.rstrip(", ")
            # phone = '01711690821'
            # phone_numbers =["01334923595","01711690821","01980796731"]
            phone_numbers =["01609620222"]
            text = (
                f"Dear Amena you have a new order in {restaurant}. "
                f"Birokto na hoye order delivery koren. "
                f"Name: {order.customer}, Phone: {order.dropoff_phone_number}, "
                f"Address: {order.dropoff_address}, Email: {order.user.email}, "
                f"Amount: {order.total}. Item: {order_items_info}"
            )

            sms_response = "SMS not sent (non-production)"

            if ENV_TYPE == "PRODUCTION":
                for phone in phone_numbers:
                    res = send_sms_bd(phone, text)
                    sms_response = res.json()
                    print("SMS- --- ---- ", res)
            # Create the payload dictionary first
            response_payload = {
                "status": "order placed!",
                "order": order_serializer.data,
                "sms_response": sms_response,
                "on_time_guarantee_fee": on_time_guarantee_fee,
            }

            # If auto-reward was applied, add it
            if auto_applied_reward_info:
                response_payload["auto_applied_on_time_reward"] = auto_applied_reward_info

            # Return the response
            return Response(
                response_payload,
                status=status.HTTP_201_CREATED
            )

           
        except Exception as e:
            return Response(str(e), status=403)
    
    
    def get_delivery_fee(self, data, user=None):
        
        restaurant_id = data.get("restaurant")
        restaurant = Restaurant.objects.get(id=restaurant_id)

        # Restaurant location
        pickup_lat = restaurant.latitude
        pickup_lng = restaurant.longitude

      #  USER coords from query params (just like restaurant list)
        # user_lat = self.request.query_params.get("lat")
        # user_lng = self.request.query_params.get("lng")
        user_lat = data.get("lat")
        user_lng = data.get("lng")

        if not user_lat or not user_lng:
            raise ParseError("Missing lat/lng in query parameters.")

        # ✅ Try distance-based logic first
        print("hello5")
        if len(data.get("orderitem_set")) == 0:
            raise ParseError("Passing items required!")
      
        print("hello5")
       
        print(data, '-----------------------------------> 7700')
        raider = DeliveryManager()
        create_quote = raider.create_quote(data=data)
        print(create_quote, '-----------------------------------> 7500')
        if create_quote.get('status_code') != 200:
            raise APIException(
                create_quote.json(),
                code=create_quote.get('status_code')
            )
        
        # distance = create_quote.get("data", {}).get("distance", None)
        distance = calculate_haversine_distance(  pickup_lat,
                pickup_lng,
                user_lat,
                user_lng)

        print("📏  Haversine distance:", distance)

          # total = int(self.calculate_amount(data.get('orderitem_set')).get('base_price__sum') * 100)
        if distance is not None:
            try:
                distance = float(distance)
                if distance <= 3:
                    fee = 0
                else:
                    fee = 25 + (distance - 3) * 12
                print(f"✅ Delivery fee (based on {distance:.2f} km) = {fee} Tk")
                return round(fee, 2),Order.DeliveryPlatform.RAIDER_APP.value
            except ValueError:
                print("⚠️ Invalid distance format.")

        
        delivery_fee_rule = int(get_delivery_fee_rule(user, restaurant))
        print("delivery_fee_rule", delivery_fee_rule)
        fee = (create_quote.get("fee", 0) / 100) + delivery_fee_rule
        return round(fee,2), Order.DeliveryPlatform.RAIDER_APP.value
      
  
  
    

class OTPSendAPIView(BaseOTPSendAPIView):
    pass


class VerifyOTPAPIView(BaseVerifyOTPAPIView):
    pass