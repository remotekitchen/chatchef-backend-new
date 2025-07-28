import datetime
from celery import shared_task
from django.utils.timezone import now, make_aware
from marketing.models import AutoReplyToComments, Review

from marketing.email_sender import send_email
from core.utils import get_logger

logger = get_logger()

@shared_task(name="marketing.auto_reply_to_reviews")
def auto_reply_to_reviews():
    try:
        # Get reviews from the last 24 hours
        cutoff_time = now() - datetime.timedelta(minutes=1)
        new_reviews = Review.objects.filter(created_at__gte=cutoff_time)

        for review in new_reviews:
            try:
                # Get related AutoReplyToComments settings
                auto_reply_settings = AutoReplyToComments.objects.filter(
                    restaurant_id=review.restaurant_id
                ).first()

                if not auto_reply_settings:
                    continue

                # Determine if the review is "good" or "bad"
                is_good_review = review.rating >= 4  # Adjust threshold as needed
                is_bad_review = review.rating <= 2  # Adjust threshold as needed

                # Send emails based on settings
                if is_good_review and auto_reply_settings.auto_reply_to_good_comments:
                    send_auto_reply_email(review, is_positive=True)
                elif is_bad_review and auto_reply_settings.auto_reply_to_bad_comments:
                    send_auto_reply_email(review, is_positive=False)

            except Exception as e:
                logger.error(f"Error processing review {review.id}: {e}")

    except Exception as e:
        logger.error(f"Error in auto_reply_to_reviews task: {e}")


def send_auto_reply_email(review, is_positive):
    subject = "Thank You for Your Feedback!" if is_positive else "We Appreciate Your Feedback"
    template = (
        "email/good_review_reply.html" if is_positive else "email/bad_review_reply.html"
    )
    context = {
        "user": review.user,
        "review": review.review,
        "rating": review.rating,
    }

    send_email(
        subject=subject,
        template=template,
        context=context,
        recipient_list=[review.user.email],
        restaurant_id=review.restaurant_id,
    )
