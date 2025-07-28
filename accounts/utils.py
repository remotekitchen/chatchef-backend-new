import uuid

from django.contrib.auth import get_user_model

from marketing.email_sender import send_email
from accounts.models import  Otp
from django.utils.crypto import get_random_string
from django.utils import timezone
from datetime import timedelta
from django.utils.timezone import now
import pytz
from django.http import JsonResponse
from django.shortcuts import redirect
from accounts.models import QRScan
from django.conf import settings
User = get_user_model()


def get_verify_uid():
    while True:
        uid = uuid.uuid4()
        if User.objects.filter(uid=uid).exists():
            continue
        return uid


def send_verify_email(user):
    # Only assign a new UID if user.uid is empty or None
    if not user.uid:
        user.uid = get_verify_uid()
        user.save(update_fields=["uid"])
        
    verify_link = f'https://order.chatchefs.com/accounts/verify?code={user.uid}&hash={uuid.uuid4()}'
    subject = 'Verify your account'
    html_path = 'email/verify_email.html'
    print(verify_link)

    send_email(
        subject,
        html_path,
        context={
            'verify_link': verify_link,
            'user': user.get_full_name()
        },
        to_emails=[f'{user.email}']
    )
    return


# send email verification
def send_email_verification_otp(user):
    """
    Sends a numeric OTP to the user's email for account verification.
    """
    otp_code = get_random_string(length=6, allowed_chars='0123456789')

    # Create OTP with expiry (e.g., 10 minutes from now)
    Otp.objects.create(
        user=user,
        email=user.email,
        otp=int(otp_code),
        is_used=False,
        expires_at=timezone.now() + timedelta(minutes=10)
    )

    subject = 'Verify your HungryTiger email address'
    html_path = 'email/identity_verification.html'

    print(f"[Email Verification] OTP for {user.email}: {otp_code}")

    send_email(
        subject,
        html_path,
        context={
            'user': user.get_full_name(),
            'otp_code': otp_code
        },
        to_emails=[user.email],
        from_email = settings.DEFAULT_HUNGRY_TIGER_EMAIL 
    )




# send reset OTP to user
def send_password_reset_otp_email(user, otp_code):
    """
    Sends a password reset OTP to the user's email using a styled HTML template.
    """
    subject = 'Reset your HungryTiger password with OTP'
    html_path = 'email/password_reset_otp.html'  # Create this template in your templates directory

    print(f"[Password Reset] Sending OTP {otp_code} to {user.email}")

    send_email(
        subject,
        html_path,
        context={
            'user': user.get_full_name(),
            'otp_code': otp_code,
        },
        to_emails=[user.email],
        from_email = settings.DEFAULT_HUNGRY_TIGER_EMAIL 
    )


def get_bdt_time():
    bdt = pytz.timezone("Asia/Dhaka")
    return now().astimezone(bdt)


def get_client_ip(request):
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    return x_forwarded_for.split(",")[0] if x_forwarded_for else request.META.get("REMOTE_ADDR")

def scan_qr(request):
    device_id = request.GET.get("device_id")
    ref = request.GET.get("ref")  # Optional
    ip = get_client_ip(request)
    user_agent = request.META.get("HTTP_USER_AGENT", "")
    timestamp = get_bdt_time()

    if not device_id:
        return JsonResponse({"error": "Missing device_id"}, status=400)

    if not QRScan.objects.filter(device_id=device_id).exists():
        QRScan.objects.create(
            device_id=device_id,
            ip_address=ip,
            user_agent=user_agent,
            ref=ref,
            timestamp=timestamp
        )
        print(f"✅ New scan from {device_id}")
    else:
        print(f"⚠️ Duplicate scan from {device_id} ignored")

    return redirect("https://yourdomain.com/thank-you")

