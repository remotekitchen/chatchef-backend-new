from core.utils import get_logger
from Webhook.otter.request_handler import send_post_request_with_auth_retry

# Get a logger object
logger = get_logger()


def update_order_status_on_otter(order, order_status="FULFILLED"):
    # Log a separator for readability
    logger.error('--------------------------------')
    # Log a message indicating the start of the update process
    logger.error('Updating order status on Otter')
    # Log the order ID
    logger.error('Order ID:', order.id)

    # Define a mapping of order statuses
    status_map = {
        "PREPARED": "PREPARED",
        "CANCELED": "CANCELED",
        "FULFILLED": "FULFILLED"
    }

    # Construct the API endpoint for updating order status
    endpoint = f"v1/orders/{order.order_id}/status"

    # Prepare the initial headers for the API request
    initial_headers = {
        'Authorization': f'Bearer {order.restaurant.otter_auth_token}',
        'Content-Type': 'application/json',
        'X-Store-Id': f'{order.restaurant.otter_x_store_id}',
        'scope': 'menus.publish'
    }

    # Create a data payload containing the new order status
    data = {
        "orderStatus": status_map[order_status]
    }

    # Log the data payload for reference
    logger.error('--------------------------------')
    logger.error(data)

    # Get the restaurant information from the order
    restaurant = order.restaurant

    # Send a POST request to the Otter API with authentication retry
    response = send_post_request_with_auth_retry(
        endpoint, initial_headers, data, restaurant)

    # log the response
    logger.error('--------------------------------')
    if response.status_code != 202:
        logger.error('Something went wrong!')

    logger.error('Order status updated.')

    # Return the response data as a dictionary
    return
