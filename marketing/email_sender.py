from django.conf import settings
from django.core.mail import send_mail
from django.shortcuts import get_object_or_404
from django.template.loader import render_to_string
from django.utils import timezone
import base64
from food.models import Restaurant
from marketing.models import EmailConfiguration, EmailHistory
import sendgrid
from sendgrid.helpers.mail import Mail,Attachment, FileContent, FileName, FileType, Disposition


# def send_email(subject, html_path, context={}, to_emails=[], restaurant=0):
#     html_message = render_to_string(html_path, context)
#     print(f"EMAIL_HOST: {settings.EMAIL_HOST}")
#     print(f"EMAIL_PORT: {settings.EMAIL_PORT}")
#     print(f"EMAIL_USE_TLS: {settings.EMAIL_USE_TLS}")
#     print(f"EMAIL_USE_SSL: {settings.EMAIL_USE_SSL}")
#     print(f"EMAIL_HOST_USER: {settings.EMAIL_HOST_USER}")

#     original_email_host = settings.EMAIL_HOST
#     original_email_port = settings.EMAIL_PORT
#     original_email_use_tls = settings.EMAIL_USE_TLS
#     original_email_host_user = settings.EMAIL_HOST_USER
#     original_email_host_password = settings.EMAIL_HOST_PASSWORD
#     original_default_from_email = settings.DEFAULT_FROM_EMAIL

#     try:
#         if EmailConfiguration.objects.filter(restaurant=restaurant).exists():
#             email_configs = EmailConfiguration.objects.get(
#                 restaurant=restaurant)

#             settings.EMAIL_HOST = f'{email_configs.email_host}'
#             settings.EMAIL_PORT = email_configs.email_port
#             settings.EMAIL_USE_TLS = email_configs.email_use_tls
#             settings.EMAIL_HOST_USER = email_configs.email_host_user
#             settings.EMAIL_HOST_PASSWORD = email_configs.email_host_password
#             settings.DEFAULT_FROM_EMAIL = email_configs.email_host_user

#         print(f"sending email")
#         send_mail(
#               subject,
#               html_message,
#               settings.EMAIL_HOST_USER,
#               to_emails,  # Send to the current batch of recipients
#               fail_silently=False,
#               html_message=html_message,
#           )

#     finally:
#         settings.EMAIL_HOST = original_email_host
#         settings.EMAIL_PORT = original_email_port
#         settings.EMAIL_USE_TLS = original_email_use_tls
#         settings.EMAIL_HOST_USER = original_email_host_user
#         settings.EMAIL_HOST_PASSWORD = original_email_host_password
#         settings.DEFAULT_FROM_EMAIL = original_default_from_email

# test

def send_email(subject, html_path, context={}, to_emails=[], restaurant=0, from_email=None,  attachment=None):
    """Send an email using SendGrid API with a static email configuration."""
    
    # Render the HTML message with the provided context
    html_message = render_to_string(html_path, context)

    try:
        print("Sending email via SendGrid...")

        # Initialize SendGrid client with API Key
        sg = sendgrid.SendGridAPIClient(settings.SENDGRID_API_KEY)
        
        # Create email object
        email = Mail(
            from_email=from_email or settings.DEFAULT_FROM_EMAIL,  # Your verified sender email (e.g., info@chatchefs.com)
            to_emails=to_emails,  # List of recipients
            subject=subject,  # Subject of the email
            html_content=html_message,  # HTML email content
        )

        if attachment:
            encoded_file = base64.b64encode(attachment["content"]).decode()
            attachedFile = Attachment(
                FileContent(encoded_file),
                FileName(attachment["filename"]),
                FileType(attachment["mimetype"]),
                Disposition("attachment")
            )
            email.attachment = attachedFile

        # Send email
        response = sg.send(email)
        
        print(f"Email sent! Status: {response.status_code}")
        return response.status_code

    except Exception as e:
        print(f"Failed to send email: {e}")
        return None