from threading import Thread

from core.utils import get_logger
from food.models import Location, Restaurant
from Webhook.otter.delivery_status_update import delivery_status_update
from Webhook.otter.store_availability import (menu_availability,
                                              pause_unpause_store,
                                              store_availability)
from Webhook.otter.update_oder_status import (canceled_order_on_otter,
                                              updated_order_status_otter)

from .menu_importer import menu_publish, otter_remover

logger = get_logger()


def otter_webhook_handler(data):
    # Extract relevant data from the webhook payload
    store = data['metadata']['storeId']
    event_id = data['eventId']

    # Check if a restaurant with the given Otter store ID exists in the database
    if Location.objects.filter(otter_x_store_id=store).exists():
        location = Location.objects.get(otter_x_store_id=store)
        location.otter_x_event_id = event_id
        location.is_menu_importing = True
        location.save()
        restaurant = location.restaurant

        eventType = data['eventType']

        # Handle 'menus.menu_publish' events
        if eventType == 'menus.menu_publish':
            logger.error('----------------------')
            logger.error('menu_publish webhook')

            menu_publish_thread = Thread(
                target=menu_publish, args=(data, restaurant, location))
            menu_publish_thread.start()
            '''
                Working well with new requirement
            '''
            return True

        # Handle menus availabilities
        if eventType == "menus.update_menu_entities_availabilities":
            logger.error('----------------------')
            logger.error('menu_publish webhook')
            menu_availability_thread = Thread(
                target=menu_availability, args=(data, restaurant, location))
            menu_availability_thread.start()
            '''
                Working well with new requirement
            '''
            return True

        # Handle 'orders.order_status_update' events
        if eventType == "orders.order_status_update":
            logger.error('-----------------------------')
            logger.error('order status updating ----->')

            # Process the order status update and check if it was successful
            status = updated_order_status_otter(data, restaurant, location)
            if status:
                '''
                Working well with new requirement
                '''
                return True

        # Handle 'orders.cancel_order' events
        if eventType == "orders.cancel_order":
            logger.error('-----------------------------')
            logger.error('order canceled')

            # Process the order cancellation and check if it was successful
            status = canceled_order_on_otter(data, restaurant)
            if status:
                return True

        if eventType == "storefront.get_store_availability":
            logger.error('-----------------------------')
            logger.error('get store availability')

            # run store availability functions
            # store_availability(data, restaurant, location)
            thread = Thread(
                target=store_availability, args=(data, restaurant, location))
            thread.start()
            return True

        if eventType == "storefront.pause_store":
            logger.error('-----------------------------')
            logger.error('pausing store')

            # run pause store functions
            # pause_unpause_store(data, restaurant, location, 'pause')
            thread = Thread(
                target=pause_unpause_store, args=(data, restaurant, location, 'pause'))
            thread.start()
            return True

        if eventType == "storefront.unpause_store":
            logger.error('-----------------------------')
            logger.error('unpausing store')

            # run unpause store functions
            # pause_unpause_store(data, restaurant, location, 'unpause')
            thread = Thread(
                target=pause_unpause_store, args=(data, restaurant, location, 'unpause'))
            thread.start()
            return True

        if eventType == "otter.remove":
            otter_remover(data, restaurant, location)
            return True
    # If the restaurant doesn't exist or the event type is not recognized, return False
    return False


'''
How Otters Work:

Under an account, you can create multiple brands or stores. Under each brand or store, you can create multiple menus, but only one menu can be active at a time.

How ChatChefs Works:

Under a company, you can create multiple restaurants. Within each restaurant, you can create multiple menus, and all of these menus are active.

------------------
after pause a restaurant otter should not send any request because it's temp pause so we need to run a cronjob and make restaurant status open again when pause time is over.

'''
