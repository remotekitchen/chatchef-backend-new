from allauth.account.utils import perform_login
from allauth.socialaccount.signals import pre_social_login
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.db.models.signals import post_save
from django.dispatch import receiver

from accounts.models import Company, RestaurantUser, User
from billing.models import BillingProfile
from core.utils import get_logger
from marketing.models import FissionCampaign
from billing.models import UnregisteredGiftCard, Transactions
from billing.utiils import (MakeTransactions)
# from marketing.utils.create_voucher import create_user_rewards_for_audience
# from django.core.exceptions import PermissionDenied

logger = get_logger()


@receiver(post_save, sender=Company)
def create_billing_profile(sender, instance: Company, created, **kwargs):
    if not created:
        return
    try:
        BillingProfile.objects.create(company=instance)
    except Exception as e:
        logger.error(
            f'Billing profile create error for company {instance.id}::{e}')


@receiver(post_save, sender=RestaurantUser)
def check_new_login_rewards(sender, instance: RestaurantUser, created, **kwargs):
    try:
        # Handling fission campaigns
        allowed_availabilities = [
            FissionCampaign.Availability.ONCE_EVERY_USER,
            FissionCampaign.Availability.AFTER_SIGN_UP,
            FissionCampaign.Availability.AFTER_EVERY_ORDER
        ]
        q_exp = Q(availability__in=allowed_availabilities)
        q_exp &= Q(restaurant=instance.restaurant)
        campaign = FissionCampaign.objects.filter(q_exp).first()
        if campaign is not None:
            instance.available_lucky_draws.add(campaign)
    except Exception as e:
        logger.error(f'Login reward error: {e}')


User = get_user_model()


@receiver(pre_social_login)
def link_to_local_user(sender, request, sociallogin, **kwargs):
    email_address = sociallogin.account.extra_data.get('email')
    if email_address:
        users = User.objects.filter(email=email_address)
        if users:
            perform_login(request, users[0], email_verification='optional')



@receiver(post_save, sender=RestaurantUser)
def claim_pending_gift_cards(sender, instance: RestaurantUser, created, **kwargs):
    if not created:
        return

    print(sender, 'sender --> 63')
    
    # Fetch unclaimed gift cards for the user's email
    unclaimed_gifts = UnregisteredGiftCard.objects.filter(email=instance.user.email)
    print(unclaimed_gifts, 'unclaimed_gifts --> 67')

    for gift in unclaimed_gifts:
        # Add the gift card amount to the user's wallet
        wallet = MakeTransactions.get_wallet(instance.user.id, gift.restaurant.id)
        wallet.balance += gift.amount
        wallet.save()
        print(gift, 'gift --> 73')
        
        # Create the transaction
        transactions = Transactions.objects.create(
            wallet=wallet,
            user=instance.user,  # The recipient user (now registered)
            amount=gift.amount,
            currency=gift.currency,
            type=Transactions.TransactionType.IN,
            status=Transactions.TransactionStatus.PENDING,
            used_for=Transactions.UsedFor.GIFT,
            gift_user=None,  # This will remain None if the receiver was unauthorized
            gift_by=instance,  # Always the RestaurantUser who gifted
            sender_name=instance.user.first_name,  # Sender's name from the registered user
            gateway=Transactions.PaymentGateway.UNKNOWN,
            restaurant=gift.restaurant  # Pass the restaurant instance
        )

        # If gift_user is unauthorized, leave it None or log it
        if not instance.user.is_authenticated:
            print(f"Unauthorized user received gift: {gift.email}")
        

# @receiver(post_save, sender=User)
# def create_rewards_for_new_user(sender, instance, created, **kwargs):
#     print(sender, 'sender --> 98')
#     if not created:
#         return
      
#     print(instance, 'instance --> 102')

#     user = sender.objects.get(id=instance.id)
#     # restaurant_id = instance.restaurant.id
#     # location_id = instance.restaurant.location

#     try:
#         # Attempt to create rewards
#         created_rewards = create_user_rewards_for_audience(user)
#         for reward in created_rewards:
#             print(f"Created reward: {reward.code} for user: {user.email}")
#     except PermissionDenied as e:
#         print(f"Could not create rewards for user {user.email}: {e}")
