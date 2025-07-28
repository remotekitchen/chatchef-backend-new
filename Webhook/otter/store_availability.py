import datetime

from food.models import Menu, MenuItem, TimeTable
from Webhook.models import OtterAuth
from Webhook.otter.auth import refresh_auth_token
from Webhook.otter.create_order import logger
from Webhook.otter.request_handler import send_post_request_with_auth_retry

days_list = {
    "mon": "MONDAY",
    "tue": "TUESDAY",
    "wed": "WEDNESDAY",
    "thu": "THURSDAY",
    "fri": "FRIDAY",
    "sat": "SATURDAY",
    "sun": "SUNDAY",
    "all": "all"
}


'''
storeStates: "OPEN" "OFF_HOUR" "SERVICE_PROVIDER_PAUSED" "OPERATOR_PAUSED" "SERVICE_PROVIDER_PAUSED_COURIERS_UNAVAILABLE" "STORE_UNAVAILABLE" "HOLIDAY_HOUR" "MENU_UNAVAILABLE" "SERVICE_PROVIDER_PAUSED_MISCONFIGURED" "OPEN_FOR_PICKUP_ONLY" "OPEN_FOR_DELIVERY_ONLY" "CLOSED_FOR_UNDETERMINED_REASON"

'''


def store_availability(data, restaurant, location):
    current_utc_time = datetime.datetime.utcnow()
    print(current_utc_time)
    formatted_time = current_utc_time.strftime(
        '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    print(formatted_time)
    token = ''
    if OtterAuth.objects.filter().exists():
        token = OtterAuth.objects.filter().first().token
    else:
        token = refresh_auth_token()

    endpoint = 'v1/storefront/hours'
    initial_headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-Store-Id': f'{location.otter_x_store_id}',
        'scope': 'storefront.store_hours_configuration'
    }

    hours = []
    count = 0

    for data in Menu.objects.filter(
            restaurant=restaurant, locations__in=[location.id]):

        query_set_hours = data.opening_hours.all()
        if count < len(query_set_hours):
            hours = query_set_hours

    otter_hours = [
        {
            "dayOfWeek": days_list[hour.day_index],
            "timeRanges": [
                {
                    "startTime": timetable.start_time.strftime("%H:%M"),
                    "endTime": timetable.end_time.strftime("%H:%M")
                } for timetable in TimeTable.objects.filter(opening_hour=hour)
            ]
        } for hour in hours
    ]

    print('final data', otter_hours)

    data = {
        "storeHoursConfiguration": {
            "deliveryHours": {
                "regularHours": otter_hours,
                "specialHours": []
            },
            "pickupHours": {
                "regularHours": otter_hours,
                "specialHours": []
            },
            "timezone": "America/Vancouver"
        },
        "statusChangedAt": f"{formatted_time}",
        "eventResultMetadata": {
            "operationStatus": "SUCCEEDED",
            "additionalInformation": "Completed without problems.",
            "operationFinishedAt": f"{formatted_time}"
        }
    }

    # callback 1
    response = send_post_request_with_auth_retry(
        endpoint, initial_headers, data, restaurant)

    print(response)

    # callback 2
    endpoint_2 = 'v1/storefront/availability'
    initial_headers_2 = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-Store-Id': f'{location.otter_x_store_id}',
        'scope': 'storefront.store_availability',
        'X-Event-Id': location.otter_x_event_id,
    }

    store_state = "STORE_UNAVAILABLE" if location.is_location_closed else "OPEN"

    logger.error('--------------------------------')
    logger.error(f'location availability --> {location.id}')
    logger.error(f'store state availability --> {store_state}')
    logger.error(
        f'store state db availability --> {location.is_location_closed}')

    data_2 = {
        "storeState": store_state,
        # "storeState": "STORE_UNAVAILABLE",
        "statusChangedAt": f"{formatted_time}",
        "eventResultMetadata": {
            "operationStatus": "SUCCEEDED",
            "additionalInformation": "Completed without problems.",
            "operationFinishedAt": f"{formatted_time}"
        }
    }

    response_2 = send_post_request_with_auth_retry(
        endpoint_2, initial_headers_2, data_2, restaurant)

    print(response_2)
    return


def pause_unpause_store(data, restaurant, location, event):
    '''
    steps -->
    update restaurant with event type
    save details to our database
    send callback to otter based on event type

    '''
    current_utc_time = datetime.datetime.utcnow()
    print(current_utc_time)
    formatted_time = current_utc_time.strftime(
        '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    print(formatted_time)

    status = True if event == "pause" else False

    location.is_location_closed = status
    location.save()

    logger.error('--------------------------------')
    logger.error(f'location --> {location.id}')
    logger.error(f'store state --> {status}')
    logger.error(f'store state db --> {location.is_location_closed}')

    token = ''
    if OtterAuth.objects.filter().exists():
        token = OtterAuth.objects.filter().first().token
    else:
        token = refresh_auth_token()

    # sending requests to otter
    endpoint = 'v1/storefront/unpause' if event == "unpause" else 'v1/storefront/pause'
    initial_headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-Store-Id': f'{location.otter_x_store_id}',
        'scope': 'storefront.store_availability',
        'X-Event-Id': location.otter_x_event_id,
    }

    # data for pause store
    data = {
        "eventResultMetadata": {
            "operationStatus": "SUCCEEDED",
            "additionalInformation": "Completed without problems.",
            "operationFinishedAt": f"{formatted_time}"
        }
    }

    # data for unpause store
    data_ = {
        "closureId": "",
        "eventResultMetadata": {
            "operationStatus": "SUCCEEDED",
            "additionalInformation": "Completed without problems.",
            "operationFinishedAt": f"{formatted_time}"
        }
    }

    rq_data = data if event == "pause" else data_
    response = send_post_request_with_auth_retry(
        endpoint, initial_headers, rq_data, restaurant)

    print(response.text)
    return True


def menu_availability(data, restaurant, location):
    print('print from menu availability')
    items = data['metadata']['payload']['updates']
    for item in items:
        otter_item_id = item['selector']['id']
        status = item['status']['saleStatus']

        if MenuItem.objects.filter(otter_item_id=otter_item_id, restaurant=restaurant.id, locations=location).exists():
            menu_item = MenuItem.objects.get(otter_item_id=otter_item_id)
            print(menu_item.name)
            if status == 'TEMPORARILY_NOT_FOR_SALE':
                menu_item.is_available = False
            elif status == 'FOR_SALE':
                menu_item.is_available = True
            menu_item.save()

    token = ''
    if OtterAuth.objects.filter().exists():
        token = OtterAuth.objects.filter().first().token
    else:
        token = refresh_auth_token()

    endpoint = 'v1/menus/entity/availability/bulk'
    initial_headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'scope': 'menus.entity_suspension',
        'X-Store-Id': f'{location.otter_x_store_id}',
        'X-Event-Id': location.otter_x_event_id,
    }

    # data for pause store
    data = {}
    response = send_post_request_with_auth_retry(
        endpoint, initial_headers, data, restaurant)

    return True
