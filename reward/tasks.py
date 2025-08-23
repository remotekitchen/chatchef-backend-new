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

import uuid
from datetime import datetime, timedelta, time

from django.conf import settings
from django.utils import timezone
from zoneinfo import ZoneInfo




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


# --- Helpers ---------------------------------------------------------------

BD_TZ = ZoneInfo("Asia/Dhaka")

def bd_now():
    """Bangladesh local time (tz-aware)."""
    return timezone.now().astimezone(BD_TZ)

def friendly_inactivity_blurb(days: int) -> str:
    if days >= 60:
        return "It’s been a while — we really miss you!"
    if days >= 30:
        return "Long time no see 👋"
    if days >= 14:
        return "Almost two weeks — drop by soon?"
    if days >= 7:
        return "It’s been about a week!"
    return "We miss you!"


# --- Task ------------------------------------------------------------------

@shared_task(name="chatchef.send_retention_coupons")
def send_retention_coupons():
    print("🚀 Starting Tiered Retention Coupon Task")

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

        batch_size = 1000
        last_user_id = 0

        # Treat these as "completed" orders (adjust to your enums if needed)
        COMPLETED_STATUSES = ["delivered", "completed", "fulfilled"]

        while True:
            users = (
                User.objects.filter(id__gt=last_user_id, is_active=True)
                .order_by("id")[:batch_size]
            )
            if not users:
                break

            user_ids = list(users.values_list("id", flat=True))

            # Users with a completed order in the last 15 days are "active"
            active_user_ids = set(
                Order.objects.filter(
                    user_id__in=user_ids,
                    status__in=COMPLETED_STATUSES,
                    receive_date__gte=now_utc - timedelta(days=15),
                ).values_list("user_id", flat=True)
            )

            # Ensure the RewardGroup exists and applies to both pickup & delivery
            reward_group, _ = RewardGroup.objects.get_or_create(
                name="Retention Campaign",
                defaults={
                    "validity_type": RewardGroup.ValidityType.DAYS_AFTER_REWARDED,
                    "validity_days": 3,
                },
            )
            target = {"delivery", "pickup"}
            changed = False
            try:
                # Array/list field
                current = set(reward_group.applies_for or [])
                if not target.issubset(current):
                    reward_group.applies_for = list(current | target)
                    reward_group.save(update_fields=["applies_for"])
                    changed = True
            except TypeError:
                # Comma-separated char field
                current = set((reward_group.applies_for or "").split(",")) - {""}
                if not target.issubset(current):
                    reward_group.applies_for = ",".join(sorted(current | target))
                    reward_group.save(update_fields=["applies_for"])
                    changed = True
            if changed:
                print("Retention Campaign 'applies_for' normalized to include delivery & pickup.")

            for user in users:
                if user.id in active_user_ids:
                    continue

                # last completed order (fallback: joined date)
                last_order = (
                    Order.objects.filter(user=user, status__in=COMPLETED_STATUSES)
                    .order_by("-receive_date")
                    .first()
                )
                last_date = last_order.receive_date if last_order else user.date_joined
                days_inactive = (now_utc - last_date).days

                # pick tier
                tier = next((t for t in tiers if days_inactive >= t["min_days"]), None)
                if not tier:
                    print(f"❌ {user.email}: not eligible (only {days_inactive} inactive days).")
                    continue

                # skip if an ACTIVE coupon for this tier already exists (unclaimed + not expired)
                active_coupon_exists = (
                    UserReward.objects.filter(
                        user=user,
                        code__startswith=tier["code"],
                        is_claimed=False,
                    )
                    .filter(Q(expiry_date__isnull=True) | Q(expiry_date__gte=today_bd))
                    .exists()
                )
                if active_coupon_exists:
                    print(f"🎁 {user.email} already has an active {tier['code']} coupon. Skipping.")
                    continue

                # create reward + user reward + voucher
                reward = Reward.objects.create(
                    reward_group=reward_group,
                    reward_type=Reward.RewardType.COUPON,
                    offer_type=Reward.OfferType.FLAT,
                    amount=tier["amount"],
                )

                unique_code = f"{tier['code']}-{uuid.uuid4().hex[:6].upper()}"
                # store expiry as BD-local date (3 days from now in BD)
                expiry_bd_date = (now_bd + timedelta(days=3)).date()

                UserReward.objects.create(
                    user=user,
                    reward=reward,
                    code=unique_code,
                    amount=tier["amount"],
                    is_claimed=False,
                    expiry_date=expiry_bd_date,
                    given_for_not_order_last_x_days=True,
                )

                Voucher.objects.create(
                    reward=reward,
                    voucher_code=unique_code,
                    amount=tier["amount"],
                    minimum_spend=tier["min_spend"],
                    max_redeem_value=tier["amount"],
                    max_uses=1,
                    is_one_time_use=False,
                    is_global=False,
                    ht_voucher_percentage_borne_by_restaurant=0,
                    notification_sent=False,
                    last_notification_sent_at=None,
                )

                # ----- Dynamic, BD-aware messaging -----
                blurb = friendly_inactivity_blurb(days_inactive)
                amount = int(tier["amount"])
                min_spend = int(tier["min_spend"])
                expiry_text = expiry_bd_date.strftime("%d %b")  # e.g., "07 Sep"
                valid_days = max((expiry_bd_date - bd_now().date()).days, 0) or 1  # never 0

                # Email
                if user.email:
                    subject = f"{blurb} — here’s ৳{amount} off!"
                    context = {
                        "user": user,
                        "coupon_code": unique_code,
                        "discount_amount": amount,
                        "minimum_spend": min_spend,
                        "expiry_date_bd": expiry_text,  # BD calendar date
                        "valid_days": valid_days,       # dynamic
                        "days_inactive": days_inactive,
                    }
                    # send_email(
                    #     subject,
                    #     "email/retention_coupon.html",
                    #     context,
                    #     [user.email],
                    #     from_email=settings.DEFAULT_HUNGRY_TIGER_EMAIL,
                    # )

                # SMS
                if getattr(user, "phone", None):
                    sms_text = (
                        f"{blurb} Get ৳{amount} off! Code {unique_code} (Min ৳{min_spend}). "
                        f"Valid {valid_days} day(s) — until {expiry_text} (BD)."
                    )
                    # send_sms_bd(user.phone, sms_text)
                    NotificationLog.objects.create(
                        user=user,
                        tier=tier["code"],
                        channel="sms",
                        message_content=sms_text,
                        sent_at=now_utc,
                        status="sent",
                    )

                # Push
                tokens = list(TokenFCM.objects.filter(user=user).values_list("token", flat=True))
                if tokens:
                    push_message = (
                        f"{blurb} Use {unique_code} for ৳{amount} OFF (Min ৳{min_spend}). "
                        f"Valid {valid_days} day(s) — until {expiry_text} (BD)."
                    )
                    push_data = {
                        "campaign_title": "We saved a coupon for you 🎁",
                        "campaign_message": push_message,
                        "screen": "coupons",
                        "type": "retention_coupon",
                        "coupon_code": unique_code,
                        "id": user.id,
                    }
                    # send_push_notification(tokens, push_data)
                    NotificationLog.objects.create(
                        user=user,
                        tier=tier["code"],
                        channel="push",
                        message_content=push_message,
                        sent_at=now_utc,
                        status="sent",
                    )

            last_user_id = user_ids[-1]
            print(f"✅ Processed up to user ID {last_user_id}")

        print("✅ All retention coupons issued!")

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