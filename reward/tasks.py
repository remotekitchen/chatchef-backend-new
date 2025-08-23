from datetime import timedelta
from django.utils import timezone
from django.db.models import Q
from celery import shared_task
from accounts.models import User
from billing.models import Order
from reward.models import Reward, UserReward, AdditionalCondition, RetentionConfig,RewardGroup,NotificationLog
from firebase.models import TokenFCM
from marketing.email_sender import send_email
from firebase.utils.fcm_helper import send_push_notification
from django.conf import settings
import uuid
from marketing.utils.send_sms import send_sms_bd

from marketing.models import Voucher, PlatformCouponExpiryLog
from reward.utils.expiry_helpers import get_voucher_expiry
from marketing.models import Voucher
import logging

logger = logging.getLogger(__name__)





@shared_task(name="chatchef.send_on_time_reward_notification")
def send_on_time_reward_notification(user_id, reward_amount, code=None, expiry_date=None):
    """
    Send push and email notification after issuing an On-Time Delivery Guarantee reward.
    """
    try:
        user = User.objects.get(id=user_id)
        tokens = TokenFCM.objects.filter(user_id=user_id).values_list("token", flat=True)

        # FCM payload
        data = {
            "campaign_title": "🎁 Delivery Delay Reward!",
          "campaign_message": (
                f"৳{reward_amount} has been credited to your account due to a delivery delay. "
                "It will be automatically deducted from your next order!"
            ),
            "screen": "rewards",
            "type": "on_time_reward",
            "id": str(user_id),
        }

        if code:
            data["coupon_code"] = code

        if tokens:
            send_push_notification(tokens, data)
            logger.info(f"✅ Push sent to user {user_id}")
        else:
            logger.warning(f"⚠️ No FCM tokens found for user {user_id}")

        # Optional email
        # if user.email:
        #     subject = "You've received a delivery delay reward!"
        #     template = "email/on_time_coupon.html"  # Create this if you want
        #     context = {
        #         "user": user,
        #         "discount_amount": reward_amount,
        #         "coupon_code": code,
        #         "expiry_date": expiry_date,
        #     }

        #     send_email(subject, template, context, [user.email], from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL)
        #     logger.info(f"✅ Email sent to {user.email}")

    except User.DoesNotExist:
        logger.error(f"❌ User with ID {user_id} not found.")
    except Exception as e:
        logger.exception(f"❌ Failed to send on-time reward notification: {str(e)}")




@shared_task(name="chatchef.send_retention_coupons")
def send_retention_coupons():
    print("🚀 Starting Tiered Retention Coupon Task")

    tiers = [
        {
            "name": "Tier4",
            "min_days": 60,
            "amount": 50,
            "min_spend": 149,
            "code": "WEWANTYOU50",
        },
        {
            "name": "Tier3",
            "min_days": 30,
            "amount": 30,
            "min_spend": 129,
            "code": "MISSEDYOU30",
        },
        {
            "name": "Tier2",
            "min_days": 15,
            "amount": 20,
            "min_spend": 99,
            "code": "COMEBACK20",
        },
    ]

    try:
        now = timezone.now()
        batch_size = 1000
        last_user_id = 0

        while True:
            # Get users in batches
            users = User.objects.filter(
                id__gt=last_user_id,
                is_active=True
            ).order_by("id")[:batch_size]

            if not users:
                break

            user_ids = list(users.values_list("id", flat=True))

            # Users active in last 15 days
            active_user_ids = set(
                Order.objects.filter(
                    user_id__in=user_ids,
                    receive_date__gte=now - timedelta(days=15)
                ).values_list("user_id", flat=True)
            )

            for user in users:
                if user.id in active_user_ids:
                    continue

                # When was their last order?
                last_order = (
                    Order.objects.filter(user=user)
                    .order_by("-receive_date")
                    .first()
                )
                last_date = last_order.receive_date if last_order else user.date_joined
                days_inactive = (now - last_date).days

                # Determine the tier
                tier = next(
                    (t for t in tiers if days_inactive >= t["min_days"]),
                    None,
                )

                if not tier:
                    print(f"❌ User {user.email} is not eligible (only {days_inactive} days inactive).")
                    continue

                # Rate limiting: was user notified in last 7 days?
                recent_notification = NotificationLog.objects.filter(
                    user=user,
                    tier=tier["code"],
                    sent_at__gte=now - timedelta(days=7)
                ).exists()
                if recent_notification:
                    print(f"⏳ Skipping {user.email}: already notified in last 7 days for {tier['code']}.")
                    continue

                # Check if user already has this coupon
                existing_coupon = UserReward.objects.filter(
                    user=user,
                    code__startswith=tier["code"]
                ).exists()
                if existing_coupon:
                    print(f"🎁 User {user.email} already has coupon {tier['code']}. Skipping.")
                    continue

                # Create RewardGroup if needed
                reward_group, _ = RewardGroup.objects.get_or_create(
                    name="Retention Campaign",
                    defaults={
                        "validity_type": RewardGroup.ValidityType.DAYS_AFTER_REWARDED,
                        "validity_days": 3,
                    },
                )

                # Create Reward
                reward = Reward.objects.create(
                    reward_group=reward_group,
                    reward_type=Reward.RewardType.COUPON,
                    offer_type=Reward.OfferType.FLAT,
                    amount=tier["amount"],
                )

                # Generate unique code
                unique_code = f"{tier['code']}-{uuid.uuid4().hex[:6].upper()}"
                expiry = now.date() + timedelta(days=3)

                # Create UserReward
                UserReward.objects.create(
                    user=user,
                    reward=reward,
                    code=unique_code,
                    amount=tier["amount"],
                    is_claimed=False,
                    expiry_date=expiry,
                    given_for_not_order_last_x_days=True
                )

                # Create Voucher
                Voucher.objects.create(
                    reward=reward,
                    voucher_code=unique_code,
                    amount=tier["amount"],
                    minimum_spend=tier["min_spend"],
                    max_redeem_value=tier["amount"],
                    is_one_time_use=True,
                    is_global=False,
                )

                # # Send Email
                subject = f"Here's ৳{tier['amount']} Off for You!"
                context = {
                    "user": user,
                    "coupon_code": unique_code,
                    "discount_amount": tier["amount"],
                    "minimum_spend": tier["min_spend"],
                }
                send_email(
                    subject,
                    "email/retention_coupon.html",
                    context,
                    [user.email],
                    from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL,
                )

                # Send SMS
                if user.phone:
                    text = (
                        f"You got ৳{tier['amount']} off! Use code {unique_code} on your next order over ৳{tier['min_spend']}. Valid 3 days only!"
                    )
                    send_sms_bd(user.phone, text)

                    NotificationLog.objects.create(
                        user=user,
                        tier=tier["code"],
                        channel="sms",
                        message_content=text,
                        sent_at=now,
                        status="sent",
                    )

                # Send Push Notification
                tokens = list(
                    TokenFCM.objects.filter(user=user).values_list("token", flat=True)
                )
                if tokens:
                    push_data = {
                        "campaign_title": "We saved a coupon for you 🎁",
                        "campaign_message": f"Use code {unique_code} to get ৳{tier['amount']} OFF (Min ৳{tier['min_spend']}). Tap to claim!",
                        "screen": "coupon",
                        "type": "retention_coupon",
                        "id": user.id,
                    }
                    send_push_notification(tokens, push_data)

                    NotificationLog.objects.create(
                        user=user,
                        tier=tier["code"],
                        channel="push",
                        message_content=push_data["campaign_message","No message"],
                        sent_at=now,
                        status="sent",
                    )

            last_user_id = user_ids[-1]
            print(f"✅ Processed up to user ID {last_user_id}")

        print("✅ All retention coupons issued!")

    except Exception as e:
        print(f"❌ Retention coupon task failed: {str(e)}")




import uuid
from datetime import datetime, timedelta, time

from django.conf import settings
from django.utils import timezone
from zoneinfo import ZoneInfo

# ----- Bangladesh time helpers -----
BD_TZ = ZoneInfo("Asia/Dhaka")

def bd_now():
    """Return tz-aware Bangladesh local time (Django will store it in UTC)."""
    return timezone.now().astimezone(BD_TZ)

def bd_day_bounds(dt_bd):
    """Return (start_utc, end_utc) for a BD local calendar day."""
    start_bd = datetime.combine(dt_bd, time.min, tzinfo=BD_TZ)
    end_bd   = datetime.combine(dt_bd, time.max, tzinfo=BD_TZ)
    return start_bd.astimezone(timezone.utc), end_bd.astimezone(timezone.utc)


@shared_task(name="chatchef.send_retention_coupons")
def send_retention_coupons():
    print("🚀 Starting Tiered Retention Coupon Task")

    # Segments
    tiers = [
        {"name": "Tier4", "min_days": 60, "amount": 50, "min_spend": 149, "code": "WEWANTYOU50"},
        {"name": "Tier3", "min_days": 30, "amount": 30, "min_spend": 129, "code": "MISSEDYOU30"},
        {"name": "Tier2", "min_days": 15, "amount": 20, "min_spend":  99, "code": "COMEBACK20"},
        {"name": "Tier1", "min_days":  7, "amount": 10, "min_spend":  79, "code": "HELLOAGAIN10"},
    ]

    try:
        now_utc = timezone.now()
        now_bd = bd_now()
        today_bd = now_bd.date()
        bd_day_start_utc, bd_day_end_utc = bd_day_bounds(today_bd)
        week_window_start_utc = (now_bd - timedelta(days=7)).astimezone(timezone.utc)

        batch_size = 1000
        last_user_id = 0

        while True:
            users = (
                User.objects.filter(id__gt=last_user_id, is_active=True)
                .order_by("id")[:batch_size]
            )
            if not users:
                break

            user_ids = list(users.values_list("id", flat=True))

            # Keep 7–14 eligible → exclude only last 7 days’ orders
            active_user_ids = set(
                Order.objects.filter(
                    user_id__in=user_ids,
                    receive_date__gte=now_utc - timedelta(days=7)
                ).values_list("user_id", flat=True)
            )

            for user in users:
                if user.id in active_user_ids:
                    continue

                # Last activity = last order; fallback to joined date
                last_order = (
                    Order.objects.filter(user=user)
                    .order_by("-receive_date")
                    .first()
                )
                last_date = last_order.receive_date if last_order else user.date_joined
                days_inactive = (now_utc - last_date).days

                tier = next((t for t in tiers if days_inactive >= t["min_days"]), None)
                if not tier:
                    continue

                # --- Rate limits (BD-local windows) ---
                push_sent_today = NotificationLog.objects.filter(
                    user=user, tier=tier["code"], channel="push",
                    sent_at__gte=bd_day_start_utc, sent_at__lte=bd_day_end_utc
                ).exists()

                sms_sent_week = NotificationLog.objects.filter(
                    user=user, tier=tier["code"], channel="sms",
                    sent_at__gte=week_window_start_utc
                ).exists()

                # --- Find or (re)issue coupon (unique per user) ---
                # Reuse active coupon if present; else create a new 7-day coupon
                user_reward = (
                    UserReward.objects
                    .filter(user=user, code__startswith=tier["code"])
                    .order_by("-id")
                    .first()
                )

                # A coupon is considered active if not claimed and not expired
                def is_active_reward(ur):
                    return ur and (not ur.is_claimed) and (ur.expiry_date is None or ur.expiry_date >= today_bd)

                if not is_active_reward(user_reward):
                    # Create / ensure 7-day validity group
                    reward_group, _ = RewardGroup.objects.get_or_create(
                        name="Retention Campaign",
                        defaults={
                            "validity_type": RewardGroup.ValidityType.DAYS_AFTER_REWARDED,
                            "validity_days": 7,  # exactly 7 days
                        },
                    )

                    reward = Reward.objects.create(
                        reward_group=reward_group,
                        reward_type=Reward.RewardType.COUPON,
                        offer_type=Reward.OfferType.FLAT,
                        amount=tier["amount"],
                    )

                    unique_code = f"{tier['code']}-{uuid.uuid4().hex[:6].upper()}"
                    expiry_date = today_bd + timedelta(days=7)  # 7 days from today (BD)

                    user_reward = UserReward.objects.create(
                        user=user,
                        reward=reward,
                        code=unique_code,
                        amount=tier["amount"],
                        is_claimed=False,
                        expiry_date=expiry_date,
                        given_for_not_order_last_x_days=True
                    )

                    # --- inside your loop, when creating the voucher ---
                    voucher = Voucher.objects.create(
                        reward=reward,
                        voucher_code=unique_code,
                        amount=tier["amount"],
                        minimum_spend=tier["min_spend"],
                        max_redeem_value=tier["amount"],
                        max_uses=1,                 # single redemption
                        is_one_time_use=False,      # as requested
                        is_global=False,
                      
                        ht_voucher_percentage_borne_by_restaurant=0,  # explicit default
                        notification_sent=False,
                        last_notification_sent_at=None,
                    )

                else:
                    # Reuse existing active coupon
                    unique_code = user_reward.code
                    reward = user_reward.reward
                    voucher = Voucher.objects.filter(voucher_code=unique_code).first()

                # --- Email (optional) ---
                if user.email:
                    subject = f"Here's ৳{tier['amount']} Off for You!"
                    context = {
                        "user": user,
                        "coupon_code": user_reward.code,
                        "discount_amount": tier["amount"],
                        "minimum_spend": tier["min_spend"],
                        "deep_link": "chatchef://open?target_screen=coupons",
                    }
                    # try:
                    #     send_email(
                    #         subject,
                    #         "email/retention_coupon.html",
                    #         context,
                    #         [user.email],
                    #         from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL,
                    #     )
                    # except Exception as e:
                    #     print(f"✉️ Email failed for {user.email}: {e}")

                # --- SMS (≤1 per 7 BD days) ---
                if user.phone and not sms_sent_week:
                    # sms_text = (
                    #     f"You got ৳{tier['amount']} off! Code {user_reward.code} "
                    #     f"(Min ৳{tier['min_spend']}). Expires in 7 days. "
                    #     f"Open: chatchef://open?target_screen=coupons"
                    # )
                    # try:
                    #     sms_msg_id = send_sms_bd(user.phone, sms_text)  # provider id if available
                    #     sms_ok = True
                    # except Exception as e:
                    #     print(f"📱 SMS failed for {user.phone}: {e}")
                    #     sms_msg_id = None
                    #     sms_ok = False

                    sent_bd = bd_now()
                    # NotificationLog.objects.create(
                    #     user=user,
                    #     tier=tier["code"],
                    #     channel="sms",
                    #     message_content=sms_text,
                    #     sent_at=sent_bd,      # BD time (tz-aware)
                    #     status="sent" if sms_ok else "failed",
                    # )

                    # if sms_ok and voucher:
                    #     voucher.notification_sent = True
                    #     voucher.last_notification_sent_at = sent_bd
                    #     voucher.save(update_fields=["notification_sent", "last_notification_sent_at"])

                # --- Push (1 per BD day) ---
                tokens = list(TokenFCM.objects.filter(user=user).values_list("token", flat=True))
                if tokens and not push_sent_today:
                    push_data = {
                        "campaign_title": "We saved a coupon for you 🎁",
                        "campaign_message": (
                            f"Use code {user_reward.code} to get ৳{tier['amount']} OFF "
                            f"(Min ৳{tier['min_spend']}). Tap to claim!"
                        ),
                        "screen": "coupons",        # your sender passes this through to the client
                        "type": "retention_coupon",
                        "coupon_code": user_reward.code,
                        "id": user.id,
                    }
                    try:
                        send_results = send_push_notification(tokens, push_data)  # returns a dict
                        push_ok = (send_results.get("successful", 0) > 0)
                    except Exception as e:
                        print(f"🔔 Push failed for user {user.id}: {e}")
                        push_ok = False

                    sent_bd = bd_now()
                    NotificationLog.objects.create(
                        user=user,
                        tier=tier["code"],
                        channel="push",
                        message_content=push_data.get("campaign_message", "No message"),
                        sent_at=sent_bd,      # BD time (tz-aware)
                        status="sent" if push_ok else "failed",
                    )

                    if push_ok and voucher:
                        voucher.notification_sent = True
                        voucher.last_notification_sent_at = sent_bd
                        voucher.save(update_fields=["notification_sent", "last_notification_sent_at"])

            last_user_id = user_ids[-1]
            print(f"✅ Processed up to user ID {last_user_id}")

        print("✅ All retention coupons processed!")

    except Exception as e:
        print(f"❌ Retention coupon task failed: {str(e)}")



@shared_task
def send_voucher_reminder_notifications():
    today = timezone.now().date()
    target_expiry_date = today + timedelta(days=2)
    now = timezone.now()

    vouchers = Voucher.objects.filter(
        is_ht_voucher=True
    )

    for v in vouchers:
        # Skip if redeemed
        if v.applied_users.exists():
            continue

        expiry = get_voucher_expiry(v)
        is_unlimited = expiry is None

        send_reminder = False

        if not is_unlimited:
            # Limited: send only if expires in 2 days and not notified
            if expiry == target_expiry_date and not v.notification_sent:
                send_reminder = True
        else:
            # Unlimited: send every 2 days
            if not v.last_notification_sent_at:
                send_reminder = True
            else:
                last_sent = v.last_notification_sent_at.date()
                if (today - last_sent).days >= 2:
                    send_reminder = True

        if not send_reminder:
            continue

        # Select active users (you can refine this to specific users if needed)
        users = User.objects.filter(is_active=True)

        tokens = []
        user_map = {}

        for user in users:
            user_tokens = TokenFCM.objects.filter(user=user).values_list("token", flat=True)
            if not user_tokens:
                continue

            for token in user_tokens:
                tokens.append(token)
                user_map[token] = user

        if not tokens:
            continue

        # Prepare your data dictionary matching send_push_notification()
        data = {
            "campaign_title": "⏰ 48 Hours Left!",
            "campaign_message": (
                f"Your platform coupon '{v.voucher_code}' (৳{v.amount}) expires in 2 days. Tap to use it now!"
                if not is_unlimited
                else f"Don't forget to use your platform coupon '{v.voucher_code}' (৳{v.amount}). Tap to use it!"
            ),
            "campaign_image": "",  # You can set an image URL
            "campaign_category": "coupon",
            "campaign_is_active": "true",
            "restaurant_name": "",
            "screen": "coupons",
            "id": str(v.id)
        }

        # Call your notification function
        resp = send_push_notification(tokens, data)

        # Update Voucher fields
        if not is_unlimited and resp.get("successful", 0) > 0:
            v.notification_sent = True

        v.last_notification_sent_at = now
        v.save(update_fields=["notification_sent", "last_notification_sent_at"])

        # Log per user
        for token in tokens:
            user = user_map[token]
            PlatformCouponExpiryLog.objects.create(
                user=user,
                voucher=v,
                coupon_code=v.voucher_code,
                coupon_value=v.amount,
                expiry_date=expiry,
                sent_at=now,
                status="success" if token not in resp.get("invalid_tokens", []) else "failed",
                source="platform"
            )