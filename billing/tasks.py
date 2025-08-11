import datetime
import random
from django.utils.timezone import now, make_aware, is_naive
from celery import shared_task
from django.conf import settings
from django.db.models import Sum
from django.utils import timezone
from django_celery_beat.models import ClockedSchedule, PeriodicTask, IntervalSchedule, CrontabSchedule
from twilio.rest import Client
import json
import pytz
import logging
from datetime import timedelta
from django.core.cache import cache
from marketing.utils.send_sms import send_sms_bd
from billing.clients.uber_client import UberClient

from accounts.models import Otp
from billing.clients.stripe_client import StripeClient
from billing.models import (BillingProfile, Order, OrderItem, OrderReminder,
                            Payout)
from billing.utilities.generate_invoice import generate_invoices
from billing.utilities.payouts import send_payouts
from hungrytiger.celery import app
from communication.utils import Twilo
from core.utils import get_logger
from firebase.models import (CompanyPushToken, FirebasePushToken,
                             NotificationTemplate, Platform)
from firebase.utils.fcm_helper import FCMHelper
from food.utils import is_closed
from marketing.email_sender import send_email
from firebase.models import PromotionalCampaign
from firebase.utils.fcm_helper import send_push_notification
from firebase.models import TokenFCM
from food.models import Restaurant
from django.dispatch import receiver
from django.db.models.signals import post_save
from food.models import MenuItem
from random import randint
from django.db.models.signals import post_migrate
from accounts.models import UserEvent, DAURecord
logger = get_logger()


@app.task(name="chatchef.payout_to_user")
def payout_to_user():
    billing_profiles = BillingProfile.objects.all()
    for profile in billing_profiles:
        try:
            if profile.stripe_connect_account is not None:
                today = timezone.now().date()
                if today - profile.last_payout_date.date() >= datetime.timedelta(profile.payout_frequency):
                    # TODO Deduct chatchef commission if payout is daily
                    # chatchef_commission =
                    amount = Order.objects.filter(receive_date__gt=profile.last_payout_date).aggregate(
                        Sum('subtotal')
                    ).get(
                        'subtotal__sum'
                    )
                    stripe_client = StripeClient()
                    response = stripe_client.payout(
                        payment_id=profile.payout_account_id,
                        amount=amount,
                        currency=profile.currency
                    )
                    payout = Payout.objects.create(
                        uid=response.get('id'),
                        company=profile.company,
                        amount=amount,
                        details=response,
                    )
                    profile.last_payout_date = timezone.now()
                    profile.save(update_fields=['last_payout_date'])

        except Exception as e:
            logger.error(f'Payout error for company {profile.company.id}')


@app.task(name="chatchef.invoice_generator")
def invoice_generator():
    today = datetime.datetime.today()
    print(today)

    # Check if today is Thursday (weekday() returns 3 for Thursday)
    if today.weekday() == 3:
        end_date = today.date()

        start_date = end_date - datetime.timedelta(days=7)

        start_date = f'{start_date}T00:00:00-07:00'
        end_date = f'{end_date}T23:59:00-07:00'
        print('start and end date', start_date, end_date)
        obj_list = generate_invoices(start_date, end_date)
        send_payouts(obj_list=obj_list)
        #schedule email to be sent 24 hour after invoice generation 
        for reminder_obj in obj_list:
          if reminder_obj.is_paid:  # Send only if is_paid is True
                send_invoice_email_reminder.apply_async(
                    (reminder_obj, start_date, end_date),
                    eta=datetime.datetime.now() + datetime.timedelta(hours=24)
                )
        return 'invoice generated and email scheduled'
    else:
        return 'today is not week day'
      
def send_invoice_email_reminder(reminder_obj, start_date, end_date):
  #  Format dates for the email template
  start_date_str = start_date.strftime('%B %d, %Y')
  end_date_str = end_date.strftime('%B %d, %Y')
  subject = 'Reminder: Your Invoice Has Been Generated'
  template = 'email/invoice_reminder.html'
  context={
        'start_date': start_date_str,
        'end_date': end_date_str,
        'obj': reminder_obj
    },
  send_email(
        subject,
        template,
        context,
        [reminder_obj.email], 
        reminder_obj.restaurant.id 
    )


def sms_sender(reminder_obj, body):
    account_sid = Twilo['account_sid']
    auth_token = Twilo['account_token']
    client = Client(account_sid, auth_token)
    message = client.messages.create(
        body=body,
        from_='+18149046396',
        to=f'{reminder_obj.phone}'
    )

    return message


@app.task(name="chatchef.order_reminder")
def order_reminder(pk):
    try:
        if not OrderReminder.objects.filter(id=f'{pk}').exists():
            return 'invalid reminder id!'

        reminder_obj = OrderReminder.objects.get(id=f'{pk}')
        order_data = reminder_obj.order_data

        # sending mail & sms
        send_email(
            'It\'s time to place your order',
            'email/order_reminder.html',
            {
                'user': order_data.get('data').get(
                    'customer'
                ), 'obj': reminder_obj
            },
            [f'{reminder_obj.email}'],
            reminder_obj.restaurant.id
        )

        message = sms_sender(
            reminder_obj,
            f'Hi ,Your order at {reminder_obj.restaurant.name} is confirmed and scheduled to be delivered/picked up '
            f'on {reminder_obj.reminder_time}.For any changes needed, pls contact us at restaurants '
            f'{reminder_obj.restaurant.phone}.'
        )

        print(message.status)

        reminder_obj.is_mailed = True
        reminder_obj.is_sms_send = True
        reminder_obj.save()
        print('order reminder completed')

    except Exception as error:
        logger.error(f'error in order reminder\n', error)
        return


@app.task(name="chatchef.order_reminder_setter")
def order_reminder_setter(pk):
    try:
        if not OrderReminder.objects.filter(id=pk).exists():
            return 'invalid reminder id!'

        reminder_obj = OrderReminder.objects.get(id=pk)
        order_data = reminder_obj.order_data

        # sending mail & sms
        send_email(
            'Your order reminder set successfully', 'email/order_reminder_set.html', {
                'user': order_data.get('data').get('customer'), 'obj': reminder_obj
            }, [f'{reminder_obj.email}'], reminder_obj.restaurant.id
        )

        message = sms_sender(
            reminder_obj,
            f"Hi {order_data.get('data').get('customer')}, your order reminder is set for "
            f"{reminder_obj.reminder_time}. We will remind you to place an order."
        )

        print('message status', message.status)

        # creating celery task
        scheduled_obj = ClockedSchedule.objects.create(
            clocked_time=reminder_obj.reminder_time
        )

        PeriodicTask.objects.create(
            name=f'{reminder_obj.email} order --> {pk}',
            task="chatchef.order_reminder",
            args=[pk, ],
            clocked=scheduled_obj,
            one_off=True
        )
        print('order reminder set')

    except Exception as error:
        logger.error(f'error in order reminder\n', error)
        return


@app.task(name="chatchef.schedule_order_restaurant_notification_task")
def schedule_order_restaurant_notification_task(pk):
    if not Order.objects.filter(id=pk).exists():
        return 'invalid order id!'

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
                        "sound": "custom_sound.mp3",
                        
                    },
                },
                "data": {
                    'order': str(order.id),  # key value must be string
                    'location': str(order.location.id)
                },
                # 'topic': 'order_status',
                "token": token,
            }
        }

        fcm_helper = FCMHelper()
        fcm_helper.send_notification(fcm_payload)

    def check_store(order: Order):
        is_restaurant_closed = False
        menus = []

        for order_item in OrderItem.objects.filter(order=order.id):
            menus.append(order_item.menu_item.menu)

        for menu in menus:
            is_restaurant_closed = is_closed(menu)
        return is_restaurant_closed

    # send notification to restaurant
    print('sending notifications')
    order = Order.objects.get(id=pk)
    is_closed = check_store(order)
    if is_closed:
        order.status = Order.StatusChoices.MISSING
        order.save()
        return 'store closed'

    if order.restaurant is not None:
        for token in CompanyPushToken.objects.filter(company=order.restaurant.company):
            try:
                send_new_order_notification_helper(
                    order=order, token=token.push_token, platform=token.platform
                )
            except Exception as e:
                logger.error(
                    f'Scheduled order notification error for token {e}'
                )
        # try:
        #     send_notification(
        #         pk, token, platform=FirebasePushToken.Platform.WEB)
        # except Exception as error:
        #     pass


# remove junk orders
@app.task(name="chatchef.remove_junk_order")
def remove_junk_order_task():
    try:
        current_datetime = timezone.now()
        start_time = current_datetime - datetime.timedelta(days=1)
        orders = Order.objects.filter(
            is_paid=False, receive_date__lte=start_time
        )

        if not orders.exists():
            return f"no junk orders found"

        orders.delete()
        return f"junk order removed"
    except Exception as error:
        return f"error --> {error}"


@shared_task
def send_otp(phone):

    client = Client(settings.TWILIO_ACCOUNT_SID,
                    settings.TWILIO_AUTH_TOKEN)

    otp_obj = Otp.objects.create(otp=random.randint(1000, 9999), phone=phone)
    message = client.messages.create(
        body=f'Your OTP is {str(otp_obj.otp)}',
        from_=settings.TWILIO_FROM_NUMBER,
        to=f'{otp_obj.phone}'
    )

    return {"task_id": message.sid}


@app.task(name="chatchef.order_checker")
def complete_remove_pending_order(**kwargs):
      pk = kwargs.get('pk')
      order = Order.objects.get(id=pk)
      is_paid = order.is_paid 
      is_cash_order = order.payment_method == Order.PaymentMethod.CASH
      
      # Handle scheduled orders
      if order.scheduled_time:
          # Make the scheduled_time timezone-aware if it is naive
          if is_naive(order.scheduled_time):
              scheduled_time_aware = make_aware(order.scheduled_time)
          else:
              scheduled_time_aware = order.scheduled_time

          # Compare with current time
          if scheduled_time_aware > now():
              print("Scheduled order, no changes made.")
              return

      # Handle unpaid non-cash orders
      if not is_paid and not is_cash_order:
          order.delete()
          print("Stripe not paid orders. Order deleted")
          return

      # Handle paid or cash orders
      print("Stripe paid order")
      if order.status not in [Order.StatusChoices.CANCELLED, Order.StatusChoices.REJECTED]:
          print("Order status needs to be changed")
          order.status = Order.StatusChoices.COMPLETED
          order.save()
          print("Order status updated!")
      else:
          print("Order is rejected or canceled")
      return



@shared_task
def send_scheduled_notifications():
    """Check all restaurant campaigns and send scheduled notifications."""
    
    now_utc = timezone.now()
    bdt = pytz.timezone("Asia/Dhaka")
    now_bdt = now_utc.astimezone(bdt)

    today_str = now_bdt.strftime("%Y-%m-%d")
    current_minute = now_bdt.strftime("%H:%M")  # Get HH:MM format

    print(f"✅ Checking campaigns for {today_str} at {current_minute} (BDT)")

    # Loop through all restaurants
    restaurants = Restaurant.objects.all()
    fcmTokens = TokenFCM.objects.all()
    tokens = [token_obj.token for token_obj in fcmTokens]

    # print("fcmTokens ------ 100", tokens)
    print("restaurants", restaurants)
    for restaurant in restaurants:

        campaigns = PromotionalCampaign.objects.filter(restaurant=restaurant, is_active=True)
        print("campaigns", campaigns)
        for campaign in campaigns:
            schedule = campaign.schedule_times  # Assume this is a dictionary { "YYYY-MM-DD": ["HH:MM"] }

            if today_str in schedule:
                for scheduled_time in schedule[today_str]:
                    if scheduled_time[:5] == current_minute:
                        # print(f"🚀 Sending notification for {campaign.campaign_name} at {scheduled_time} (BDT)")
                        recipients = tokens
                        data = {
                                "restaurant_name": getattr(restaurant, "name", None),
                                "campaign_restaurant": restaurant if restaurant else None,
                                "campaign_title": getattr(campaign, "title", None),
                                "campaign_message": getattr(campaign, "message", None),
                                "campaign_image": getattr(campaign, "campaign_image", None),
                                "campaign_category": getattr(campaign, "category", None),
                                "campaign_is_active": getattr(campaign, "is_active", None),
                                "screen": "restaurant",
                                "id": restaurant.id if restaurant else None  
                            }
                        
                        print("recipients-----99", recipients)
                        recipients1 = ["f07sDF18b7OCMt2AT_46XM:APA91bH2db856JXIhDJZBbyTNzp0A0TvZvmL9W4ACBSYYZuRqb31ecf3BYfq5BgnPM_bOqMCdey2qvHj7PXRXVQB_bxdVTOPTpvgFTFTDf_Z8xenVBIEsqk","eRA51UR8ZIzK4td8WalqHK:APA91bGreC0ut6AgCJwbtFNeS2GbIG5lY2muGVLFXFbOrK2ucp2Z-ONTMjP9ucMsRjlrVTEShAKDvxYncNwGoEnSTEpD9tZQVMC-qc1bK2ep7JUkzLBRKho"]
                        send_push_notification(recipients1, data)

    print("✅ Notification task completed successfully!@@")


@shared_task
def send_scheduled_notifications():
    # Prevent overlapping task runs
    lock_id = "send_scheduled_notifications_lock"
    lock_expire = 60  # seconds

    # Try to acquire lock
    acquire_lock = cache.add(lock_id, "locked", lock_expire)
    if not acquire_lock:
        print("⛔ Task is already running. Skipping this round.")
        return

    try:
        # Your existing code here...
        print("✅ Executing scheduled notifications")

        now_utc = timezone.now()
        bdt = pytz.timezone("Asia/Dhaka")
        now_bdt = now_utc.astimezone(bdt)

        today_str = now_bdt.strftime("%Y-%m-%d")
        current_minute = now_bdt.strftime("%H:%M")

        restaurants = Restaurant.objects.all()
        fcmTokens = TokenFCM.objects.all()
        tokens = [token_obj.token for token_obj in fcmTokens]
        print("restaurants", restaurants)
        for restaurant in restaurants:
            print("im inside restaurant")
            campaigns = PromotionalCampaign.objects.filter(restaurant=restaurant, is_active=True)

            for campaign in campaigns:
                schedule = campaign.schedule_times
                if today_str in schedule:
                    for scheduled_time in schedule[today_str]:
                        if scheduled_time[:5] == current_minute:
                            data = {
                                "restaurant_name": restaurant.name,
                                "campaign_restaurant": restaurant,
                                "campaign_title": campaign.title,
                                "campaign_message": campaign.message,
                                "campaign_image": str(campaign.campaign_image) if campaign.campaign_image else "",
                                "campaign_category": campaign.category,
                                "campaign_is_active": campaign.is_active,
                                "screen": "restaurant",
                                "id": restaurant.id
                            }
                            tokens1 =["c90A-dzlu9rNo6k1oRCkdj:APA91bFGXBZ7FZquGHocGTArobc7R3gZVZJuAQGxL6kfaH0VRY_76fmuXbE6ehyT9aD4D9xnOJgwNOlk1Nf4soEGE5jmgWgA2UgZL79s3x6dP-ri-Wl8wdM"]
                            print("tokens1",tokens1)
                            send_push_notification(tokens1, data)
        print("✅ Notification task completed successfully!!!")
    finally:
        cache.delete(lock_id)

@receiver(post_save, sender=PromotionalCampaign)
def setup_periodic_tasks(sender, instance, created, **kwargs):
    """Set up the periodic task to run every minute."""
    schedule, created = CrontabSchedule.objects.get_or_create(
        minute="*", hour="*", day_of_month="*", month_of_year="*", day_of_week="*"
    )

    task_name = "Send Scheduled Notifications Every Minute"
    task_path = "billing.tasks.send_scheduled_notifications"  # Ensure this is correct

    # Check if the periodic task already exists
    task_exists = PeriodicTask.objects.filter(name=task_name).exists()
    print("task_exists", task_exists)
    if not task_exists:
        PeriodicTask.objects.create(
            crontab=schedule,
            name=task_name,
            task=task_path,  # Ensure it points to the correct task
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name}' running every minute!")
    else:
        task = PeriodicTask.objects.get(name=task_name)
        if task.task != task_path:
            task.task = task_path
            task.save()
            # print(f"⚠️ Updated task path to '{task_path}'")
        else:
            print(f"⚠️ Periodic task '{task_name}' already exists with the correct task path.")




@shared_task
def check_order_acceptance(order_id, attempt=1, max_attempts=30):
    try:
        order = Order.objects.get(id=order_id)
        logger.info(f"Checking order {order_id} acceptance - attempt {attempt}/{max_attempts}")
        logger.info(f"Order status: {order.status}")
        
        if order.status == 'pending':
            logger.warning(f"Order {order_id} still pending after {attempt} minute(s). Sending notification.")
            send_push_notification_to_amena(order)
            
            if attempt < max_attempts:
                check_order_acceptance.apply_async(
                    args=[order_id], 
                    kwargs={'attempt': attempt + 1, 'max_attempts': max_attempts}, 
                    countdown=60
                )
                logger.info(f"Scheduled next check (attempt {attempt + 1}) in 1 minute")
            else:
                logger.warning(f"Reached maximum of {max_attempts} notification attempts for order {order_id}. Stopping checks.")
        else:
            logger.info(f"Order {order_id} status is '{order.status}'. No further notifications needed.")

    except Order.DoesNotExist:
        logger.error(f"Order with ID {order_id} not found.")

def send_push_notification_to_amena(order):
    try:
        restaurant_name = order.restaurant.name if order.restaurant else "Unknown Restaurant"
        order_id_display = str(order.order_id)[:8] if order.order_id else str(order.id)
        now_utc = timezone.now()
        
        BDT_TZ = pytz.timezone('Asia/Dhaka')
        if order.receive_date:
            reference_time_bdt = order.receive_date.astimezone(BDT_TZ)
        else:
            reference_time_bdt = timezone.now().astimezone(BDT_TZ)

        time_diff = timezone.now() - order.receive_date if order.receive_date else timezone.now()  

        seconds_pending = time_diff.total_seconds()
        minutes_pending = seconds_pending // 60
        hours_pending = minutes_pending // 60

        if order.receive_date:
            time_diff = timezone.now() - order.receive_date  # both in UTC
        else:
            time_diff = timedelta(seconds=0)

        seconds = int(time_diff.total_seconds())
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24


        if seconds < 60:
            time_description = f"{seconds} seconds"
        elif minutes < 60:
            time_description = f"{minutes} minutes"
        elif hours < 24:
            time_description = f"{hours} hours"
        else:
            time_description = f"{days} days"
        
        title = f"Restaurant Action Required: {restaurant_name}"
        message = f"Order #{order_id_display} from {restaurant_name} has been pending for {time_description}. Restaurant has not accepted the order yet."
        
        logger.info(f"Sending push notification for Order ID: {order.id} - {restaurant_name}")
        
        fcmTokens = TokenFCM.objects.filter(user__super_power=True).select_related('user')
        tokens = [token_obj.token for token_obj in fcmTokens]
        recipients = tokens
        data = {
            "campaign_title": title,
            "campaign_message": message,
            "restaurant_name": restaurant_name,
            "order_id": str(order.id),
            "order_uuid": str(order.order_id),
            "status": order.status,
            "type": "pending_order_alert",
            "screen": "restaurant",
            "id": 90  
        }
        
        send_push_notification(recipients, data)
        
        print(f"Push notification sent for pending order at {restaurant_name} (ID: {order.id})")
        
    except Exception as e:
        logger.error(f"Failed to send notification for order {order.id}: {str(e)}")







@shared_task
def update_menuitem_availability():    
    # Prevent multiple tasks running at same time
    lock_id = "menuitem_availability_lock"
    lock_expire = 300  # 5 minutes
    
    if not cache.add(lock_id, "locked", lock_expire):
        print("⛔ MenuItem availability task already running. Skipping.")
        return "Task already running"
    
    try:
        print("🚀 Starting MenuItem availability update...")
        
        # Get current Bangladesh time
        bdt = pytz.timezone("Asia/Dhaka")
        now_bdt = timezone.now().astimezone(bdt)
        current_time = now_bdt.time()
        
        print(f"📅 Current BDT time: {current_time.strftime('%H:%M:%S')}")
        
        # Get all menu items with time restrictions
        menu_items = MenuItem.objects.filter(
            available_start_time__isnull=False,
            available_end_time__isnull=False,
        )
        
        print(f"📋 Found {menu_items.count()} items with time restrictions")
        
        # Counters for reporting
        total_processed = 0
        total_updated = 0
        enabled_items = []
        disabled_items = []
        
        # Process each menu item
        for item in menu_items:
            total_processed += 1
            start_time = item.available_start_time
            end_time = item.available_end_time
            
            # Determine if item should be available RIGHT NOW
            if start_time <= end_time:
                # SAME DAY: e.g., 09:00 AM to 17:00 PM
                should_be_available = start_time <= current_time <= end_time
                period_type = "same-day"
            else:
                # OVERNIGHT: e.g., 22:00 PM to 06:00 AM (next day)
                should_be_available = current_time >= start_time or current_time <= end_time
                period_type = "overnight"
            
            # Update only if status needs to change
            if item.is_available != should_be_available:
                old_status = "Available" if item.is_available else "Not Available"
                new_status = "Available" if should_be_available else "Not Available"
                
                # Update the database
                item.is_available = should_be_available
                item.save(update_fields=['is_available'])
                
                total_updated += 1
                
                # Track for reporting
                if should_be_available:
                    enabled_items.append({
                        'name': item.name,
                        'id': item.id,
                        'window': f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}",
                        'type': period_type
                    })
                    print(f"✅ ENABLED: {item.name} ({period_type}: {start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')})")
                else:
                    disabled_items.append({
                        'name': item.name,
                        'id': item.id,
                        'window': f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}",
                        'type': period_type
                    })
                    print(f"❌ DISABLED: {item.name} ({period_type}: {start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')})")
        
        # Final report
        print("=" * 60)
        print("📊 FINAL REPORT:")
        print(f"   Total items processed: {total_processed}")
        print(f"   Items updated: {total_updated}")
        print(f"   Items enabled: {len(enabled_items)}")
        print(f"   Items disabled: {len(disabled_items)}")
        print(f"   Timestamp: {now_bdt.strftime('%Y-%m-%d %H:%M:%S')} BDT")
        print("=" * 60)
        
        return {
            "status": "success",
            "timestamp": now_bdt.strftime('%Y-%m-%d %H:%M:%S'),
            "total_processed": total_processed,
            "total_updated": total_updated,
            "enabled_count": len(enabled_items),
            "disabled_count": len(disabled_items),
            "enabled_items": enabled_items,
            "disabled_items": disabled_items
        }
        
    except Exception as e:
        error_msg = f"❌ ERROR in MenuItem availability task: {str(e)}"
        print(error_msg)
        return {"status": "error", "message": str(e)}
    
    finally:
        # Always release the lock
        cache.delete(lock_id)




@receiver(post_save, sender=MenuItem)
def setup_periodic_tasks(sender, instance, created, **kwargs):
    """Set up the periodic task to run every minute."""
    schedule, created = CrontabSchedule.objects.get_or_create(
        minute="*", hour="*", day_of_month="*", month_of_year="*", day_of_week="*"
    )

    task_name = "update menuitem availability"
    task_path = "billing.tasks.update_menuitem_availability"  

    task_exists = PeriodicTask.objects.filter(name=task_name).exists()
    print("task_exists", task_exists)
    if not task_exists:
        PeriodicTask.objects.create(
            crontab=schedule,
            name=task_name,
            task=task_path,  
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name}' running every minute!")
    else:
        task = PeriodicTask.objects.get(name=task_name)
        if task.task != task_path:
            task.task = task_path
            task.save()
        else:
            print(f"⚠️ Periodic task '{task_name}' already exists with the correct task path.")


@shared_task
def increment_boosted_restaurant_metrics():
    # Get all remote kitchen restaurants
    restaurants = Restaurant.objects.filter(is_remote_Kitchen=True)

    for restaurant in restaurants:
        print("restaurant----", restaurant)
        
        if restaurant.priority == 1:
            increment = randint(3, 4)  
        elif restaurant.priority == 2:
            increment = randint(1, 2)
        else:
            increment = 0  

        restaurant.boosted_monthly_sales_count += increment
        restaurant.boosted_total_sales_count += increment

        # Randomize the average ticket size
        avg_ticket = randint(100, 800)
        restaurant.boosted_average_ticket_size = avg_ticket
        restaurant.boosted_total_gross_revenue += avg_ticket * increment

        # Save the updated restaurant data
        restaurant.save()

    return f"Updated {restaurants.count()} restaurants"


@receiver(post_migrate)
def setup_hourly_boost_task(sender, **kwargs):

    schedule, _ = IntervalSchedule.objects.get_or_create(
        every=60, 
        period=IntervalSchedule.MINUTES, 
    )

    task_name = "Boosted Restaurant Metrics Every Hour"
    task_path = "billing.tasks.increment_boosted_restaurant_metrics"  


    task_exists = PeriodicTask.objects.filter(name=task_name).exists()
    if not task_exists:
        PeriodicTask.objects.create(
            interval=schedule,  
            name=task_name,
            task=task_path,
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name}' to run every hour.")
    else:
        print(f"⚠️ Periodic task '{task_name}' already exists.")


# @receiver(post_migrate)
# def setup_daily_boost_task(sender, **kwargs):
#     """Ensure the daily Celery task to boost restaurant stats is created once."""

#     schedule, _ = CrontabSchedule.objects.get_or_create(
#         minute="0",
#         hour="8",
#         day_of_month="*",
#         month_of_year="*",
#         day_of_week="*",
#     )

#     task_name = "Daily Boosted Restaurant Metrics"
#     task_path = "billing.tasks.increment_boosted_restaurant_metrics"  # update this path

#     task_exists = PeriodicTask.objects.filter(name=task_name).exists()
#     if not task_exists:
#         PeriodicTask.objects.create(
#             crontab=schedule,
#             name=task_name,
#             task=task_path,
#             args=json.dumps([]),
#             kwargs=json.dumps({}),
#             one_off=False,
#         )
#         print(f"✅ Created periodic task '{task_name}' to run daily at 8:00 AM.")
#     else:
#         print(f"⚠️ Periodic task '{task_name}' already exists.")




@shared_task
def calculate_daily_active_users():
    today = timezone.now().date()
    event_names = ["app_open", "order_completed"]

    for event_name in event_names:
        count = UserEvent.objects.filter(
            event_name=event_name,
            event_time__date=today
        ).values("user_id").distinct().count()

        DAURecord.objects.update_or_create(
            date=today,
            event_name=event_name,   
            defaults={"count": count}
        )

        print(f"✅ DAU calculated: {count} users on {today} for '{event_name}'")

@receiver(post_migrate)
def setup_daily_dau_tasks(sender, **kwargs):
    # 1:00 AM Schedule
    schedule_am, _ = CrontabSchedule.objects.get_or_create(
        minute="0", hour="1", day_of_month="*", month_of_year="*", day_of_week="*"
    )
    # 1:00 PM Schedule
    schedule_pm, _ = CrontabSchedule.objects.get_or_create(
        minute="0", hour="13", day_of_month="*", month_of_year="*", day_of_week="*"
    )

    task_path = "billing.tasks.calculate_daily_active_users"

    # Create or check 1 AM Task
    task_name_am = "Calculate Daily Active Users - 1AM"
    if not PeriodicTask.objects.filter(name=task_name_am).exists():
        PeriodicTask.objects.create(
            crontab=schedule_am,
            name=task_name_am,
            task=task_path,
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name_am}'.")
    else:
        print(f"⚠️ Periodic task '{task_name_am}' already exists.")

    # Create or check 1 PM Task
    task_name_pm = "Calculate Daily Active Users - 1PM"
    if not PeriodicTask.objects.filter(name=task_name_pm).exists():
        PeriodicTask.objects.create(
            crontab=schedule_pm,
            name=task_name_pm,
            task=task_path,
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name_pm}'.")
    else:
        print(f"⚠️ Periodic task '{task_name_pm}' already exists.")






@shared_task
def check_ubereats_stuck_orders():
    stuck_orders = Order.objects.filter(
        delivery_platform=Order.DeliveryPlatform.UBEREATS,
        status__in=[
            Order.StatusChoices.PENDING,
            Order.StatusChoices.ACCEPTED,
            Order.StatusChoices.WAITING_FOR_DRIVER
        ],
        ubereats_requested_time__isnull=False,
        ubereats_requested_time__lte=now() - timedelta(minutes=15)
    )

    uber = UberClient()
    print("stuck_orders", stuck_orders)
    for order in stuck_orders:
        if not order.uber_delivery_id:
            continue

        try:
            response = uber.get_delivery(order.uber_delivery_id)
            uber_status = response.json().get("status")

            if uber_status in ["pending", "requesting"]:
                # Mark as stuck and notify sales
                phone_numbers = ["01980796731"]
                for phone in phone_numbers:
                    text = f"[URGENT] Order {order.id} is stuck on Uber: {uber_status}. Please assign a rider manually."
                    send_sms_bd(phone, text)
        except Exception as e:
            print(f"Uber API error for order {order.id}: {e}")



@receiver(post_migrate)
def setup_ubereats_stuck_order_check(sender, **kwargs):
    # Run every 1 minute
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="*",  # every minute
        hour="*", 
        day_of_month="*",
        month_of_year="*",
        day_of_week="*"
    )

    task_path = "billing.tasks.check_ubereats_stuck_orders"
    task_name = "Check UberEats Stuck Orders Every Minute"

    if not PeriodicTask.objects.filter(name=task_name).exists():
        PeriodicTask.objects.create(
            crontab=schedule,
            name=task_name,
            task=task_path,
            args=json.dumps([]),
            kwargs=json.dumps({}),
            one_off=False,
        )
        print(f"✅ Created periodic task '{task_name}'.")
    else:
        print(f"⚠️ Periodic task '{task_name}' already exists.")





from lark_automation.sync_consumer import sync_customers
@shared_task
def sync_customers_to_lark():
    sync_customers()  # ✅ Fix: must call the function


@receiver(post_migrate)
def create_sync_task_after_migrate(sender, **kwargs):
    if sender.name != "billing":
        return
    setup_periodic_task()


def setup_periodic_task():
    task_name = 'Sync Customers to Lark'

    # Crontab: run every day at 12 AM Bangladesh time (18:00 UTC)
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute='0',
        hour='18',
        day_of_week='*',
        day_of_month='*',
        month_of_year='*',
    )

    task, created = PeriodicTask.objects.update_or_create(
        name=task_name,
        defaults={
            'task': 'billing.tasks.sync_customers_to_lark',  # must match full dotted path
            'crontab': schedule,
            'args': json.dumps([]),
            'kwargs': json.dumps({}),
            'enabled': True,
            'description': 'Automatically syncs Django users to Lark Base every night',
        }
    )

    if created:
        print(f"✅ Created periodic task: {task_name}")
    else:
        print(f"🔁 Periodic task '{task_name}' already exists and was updated.")