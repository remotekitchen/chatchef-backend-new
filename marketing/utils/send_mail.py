from marketing.email_sender import send_email
from marketing.models import SalesMail



def sendmail(instance):
    emails = [f"{email.email}" for email in SalesMail.objects.all()]
    context = {"instance": instance}
    send_email(
        f'{instance.name} is trying to get a demo from us.',
        "email/marketing.html",
        context,
        emails,
    )
    sendmail_to_customer(instance)
    print('Mail send')
    return


def sendmail_to_customer(instance):
    email = [instance.email]
    context = {"instance": instance}
    send_email(
        f'{instance.name} is trying to get a demo from us.',
        "email/confirm_marketing.html",
        context,
        email,
    )
    print('Mail send')
    
    
def send_email_to_receiver(sender, receiver, amount, restaurant):
    # Determine if the receiver is registered or unregistered
    is_registered = hasattr(receiver, 'user')  # Check if receiver has a user attribute
    
    print('is_registered:', is_registered, receiver)

    # Prepare email subject
    email_subject = "You've received a gift card!"

    # Prepare context based on whether the user is registered or not
    if is_registered:
        context = {
            "receiver_name": receiver.user.first_name or receiver.user.email,
            "sender_name": sender.user.first_name or sender.user.email,
            "amount": amount,
            "restaurant_name": restaurant.name,
            "receiver_email": receiver.user.email,
            "is_registered": True,
        }
    else:
        context = {
            "receiver_name": receiver,  # Use the email directly for unregistered users
            "sender_name": sender.user.first_name or sender.user.email,
            "amount": amount,
            "restaurant_name": restaurant.name,
            "receiver_email": receiver,  # Use the provided email for unregistered users
            "is_registered": False,
        }

    # Email template path
    html_path = "email/gift_card_email.html"  # Update this to your actual template path

    # Send email
    send_email(
        subject=email_subject,
        html_path=html_path,
        context=context,
        to_emails=[context["receiver_email"]],
        restaurant=restaurant.id,
    )

    print('Mail sent to', context["receiver_email"], 'with context:', context)

