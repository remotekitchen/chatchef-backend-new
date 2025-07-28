from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from accounts.models import RestaurantUser
from core.utils import get_logger
from marketing.models import DemoData, FissionCampaign, Review, AutoReplyToComments
from marketing.utils.send_mail import sendmail
from marketing.email_sender import send_email
from marketing.utils.create_voucher import create_reward_group_and_user_reward
from food.models import Restaurant, Location

logger = get_logger()


@receiver(post_save, sender=FissionCampaign)
def check_new_fission_rewards(sender, instance: FissionCampaign, created, **kwargs):
    try:
        # Check if the fission campaign type is ONCE_EVERY_USER and add lucky draw to user
        if instance.availability != FissionCampaign.Availability.ONCE_EVERY_USER and instance.availability != \
                FissionCampaign.Availability.AFTER_EVERY_ORDER:
            return

        if created:
            users = RestaurantUser.objects.filter(
                restaurant=instance.restaurant)
            for user in users:
                user.available_lucky_draws.add(instance)

    except Exception as e:
        logger.error(f'Login reward error: {e}')


@receiver(post_save, sender=DemoData)
def demo_mail_to_sales(sender, instance: DemoData, created, **kwargs):
    if not created:
        return
    sendmail(instance)
    return
  
  
@receiver(post_save, sender=Review)
def send_review_email(sender, instance:Review, created, **kwargs):
    print('Review created')
    if created:  # Only trigger on creation, not updates
        logger.debug(f"New review created: {instance.id} by user {instance.user.email}")
        print(f"New review created: {instance.id} by user {instance.user.email}")
        print(f"Rating: {instance.restaurant_id}")

        try:
            print("looking for restaurant id" , instance.restaurant_id)
            # Get related AutoReplyToComments settings
            auto_reply_settings = AutoReplyToComments.objects.filter(
                restaurant=instance.restaurant_id
            ).first()

            logger.debug(f"AutoReplyToComments settings: {auto_reply_settings}")
            print(f"AutoReplyToComments settings: {auto_reply_settings}")

            if not auto_reply_settings:
                logger.debug(f"No auto-reply settings found for review ID: {instance.id}")
                return

            # Determine if the review is "good" or "bad"
            is_good_review = instance.rating >= 4  
            is_bad_review = instance.rating <= 3 

            logger.debug(f"Review ID: {instance.id}, is_good_review: {is_good_review}, is_bad_review: {is_bad_review}")
            print(f"Review ID: {instance.id}, is_good_review: {is_good_review}, is_bad_review: {is_bad_review}")

            # Send emails based on settings
            if is_good_review and auto_reply_settings.auto_reply_to_good_comments:
                print('sending good review email')
                send_auto_reply_email(instance, is_positive=True, auto_reply_settings=auto_reply_settings)
            elif is_bad_review and auto_reply_settings.auto_reply_to_bad_comments:
                print('sending bad review email')
                send_auto_reply_email(instance, is_positive=False , auto_reply_settings=auto_reply_settings)

        except Exception as e:
            logger.error(f"Error processing review ID {instance.id}: {e}")


def send_auto_reply_email(review, is_positive, auto_reply_settings):
    try:
        subject = "Thank You for Your Feedback!" if is_positive else "We Appreciate Your Feedback"
        html_path = (
            "email/good_review_reply.html" if is_positive else "email/bad_review_reply.html"
        )
        context = {
            "customer_name": review.user.get_full_name(),
            "restaurant_name": Restaurant.objects.get(id=review.restaurant_id).name,
        }

        if not is_positive:
            locationId =  Location.objects.filter(restaurant_id=review.restaurant_id).first().id
            reward_group, reward, user_reward = create_reward_group_and_user_reward(
                restaurant_id=review.restaurant_id,
                location_id=locationId, 
                user_id=review.user.id,
                voucher_amount = auto_reply_settings.voucher_amount,
                
            )
            context["coupon_code"] = user_reward.code
        send_email(
            subject=subject,
            html_path=html_path,
            context=context,
            to_emails=[review.user.email],
            restaurant=review.restaurant_id,
        )
        
        logger.debug(f"Email sent successfully for Review ID: {review.id}")

    except Exception as e:
        logger.error(f"Error sending email for Review ID {review.id}: {e}")








@receiver(post_save, sender=Review)  # Trigger when a review is added or updated
@receiver(post_delete, sender=Review)  # Trigger when a review is deleted
def update_restaurant_rating(sender, instance, **kwargs):
    """Update the restaurant's average rating when a review is added, updated, or deleted."""
    
    # Check if the review has a valid restaurant
    if not instance.restaurant:
        print("Warning: Review has no associated restaurant. Skipping rating update.")
        return  # Stop execution to avoid AttributeError

    instance.restaurant.update_average_rating()
