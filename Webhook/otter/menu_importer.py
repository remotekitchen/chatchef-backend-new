from django.db.models import Q

from food.models import (Category, Image, Menu, MenuItem, ModifierGroup,
                         OpeningHour, TimeTable)
from marketing.models import Rating
from pos.code.clover.core.views import log_saver
from pos.models import POS_logs
from Webhook.models import OtterAuth
from Webhook.otter.auth import refresh_auth_token
from Webhook.otter.request_handler import send_post_request_with_auth_retry


def update_time_tables(opening_hour, time_ranges, log):
    for time_range in time_ranges:
        start_time = time_range['start']
        end_time = time_range['end']

        # Check if a TimeTable with the same opening_hour, start_time, and end_time exists
        if not TimeTable.objects.filter(opening_hour=opening_hour, start_time=start_time, end_time=end_time).exists():
            TimeTable.objects.create(
                opening_hour=opening_hour, start_time=start_time, end_time=end_time)
        else:
            log_saver(log, f'timetable data exists')

    log_saver(log, f'finished time range data processing')
    return


def menu_handler(menus, restaurant, location, log):
    is_success = True
    try:
        for menu in menus:
            query_set = Menu.objects.filter(
                Q(otter_menu_id=menus[menu]['id']), restaurant=restaurant, locations=location)

            if len(query_set) > 1:
                print('multi menu found')
                query_set.delete()

            if Menu.objects.filter(Q(otter_menu_id=menus[menu]['id']), restaurant=restaurant, locations=location).exists():
                print('menu found')
                updateMenu = Menu.objects.get(
                    Q(otter_menu_id=menus[menu]['id']), restaurant=restaurant, locations=location)
                if updateMenu.title != menus[menu]['name']:
                    updateMenu.name = menus[menu]['name']
                    updateMenu.save()
                else:
                    log_saver(log, f'menu already up to date')
                menu_instance = updateMenu

            else:
                print('menu not found')
                company = restaurant.company
                menu_instance = Menu.objects.create(
                    title=menus[menu]['name'],
                    restaurant=restaurant,
                    company=company,
                    otter_menu_id=menus[menu]['id']
                )
                menu_instance.locations.add(location)

            # adding hours to the menu
            log_saver(log, f'adding timing details to the menu.')
            menu_instance_hours = menu_instance.opening_hours.all()
            reg_hours = menus[menu]['hoursData']['regularHours']
            days_list = {
                "MONDAY": "mon",
                "TUESDAY": "tue",
                "WEDNESDAY": "wed",
                "THURSDAY": "thu",
                "FRIDAY": "fri",
                "SATURDAY": "sat",
                "SUNDAY": "sun",
                "all": "all"
            }

            for i in menu_instance_hours:
                i.delete()

            for reg_hour in reg_hours:
                days = reg_hour['days']
                time_ranges = reg_hour['timeRanges']
                for day in days:
                    # if menu_instance_hours.filter(day_index=days_list[day]).exists():
                    #     opening_hour = menu_instance_hours.get(
                    #         day_index=days_list[day])
                    #     update_time_tables(opening_hour, time_ranges, log)
                    # else:
                    opening_hour = OpeningHour.objects.create(
                        day_index=days_list[day])

                    update_time_tables(opening_hour, time_ranges, log)
                    menu_instance.opening_hours.add(opening_hour)
        return is_success

    except Exception as error:
        log_saver(log, f'line 84 --> {error}')
        is_success = False
        return is_success


def category_handler(categories, restaurant, location, log):
    is_success = True
    try:
        for category in categories:

            query_set = Category.objects.filter(
                Q(otter_category_id=categories[category]['id']), restaurant=restaurant, locations=location)

            if len(query_set) > 1:
                print('multi category found')
                query_set.delete()

            if Category.objects.filter(Q(otter_category_id=categories[category]['id']), restaurant=restaurant, locations=location).exists():
                print('category found')
                update_category = Category.objects.get(
                    Q(otter_category_id=categories[category]['id']), restaurant=restaurant, locations=location)
                if update_category.name != categories[category]['name']:
                    update_category.name = categories[category]['name']
                else:
                    log_saver(log, f'category name up to date')

                if update_category.description != categories[category]['description']:
                    update_category.description = categories[category]['description']
                else:
                    log_saver(log, f'category description up to date')
                update_category.save()
            else:
                print('category not found')
                category = Category.objects.create(
                    name=categories[category]['name'],
                    description=categories[category]['description'],
                    restaurant=restaurant,
                    otter_category_id=categories[category]['id']
                )
                category.locations.add(location)
        return is_success
    except Exception as error:
        log_saver(log, f'line 113 --> {error}')
        is_success = False
        return is_success


def photo_handler(photos, restaurant, log):
    is_success = True
    try:
        for photo in photos:
            query_set = Image.objects.filter(
                Q(remote_url=photos[photo]['url']) | Q(otter_image_id=photos[photo]['id']))

            if len(query_set) > 1:
                print('multi category found')
                query_set.delete()

            if Image.objects.filter(Q(remote_url=photos[photo]['url']) | Q(otter_image_id=photos[photo]['id'])).exists():
                print('image found')
                update_photo = Image.objects.get(
                    Q(remote_url=photos[photo]['url']) | Q(otter_image_id=photos[photo]['id']))
                if update_photo.remote_url != photos[photo]['url']:
                    update_photo.remote_url = photos[photo]['url']
                    update_photo.save()
                else:
                    log_saver(log, f'Already Photo up to date')
            else:
                print('image not found')
                Image.objects.create(
                    remote_url=photos[photo]['url'],
                    otter_image_id=photos[photo]['id']
                )
        return is_success
    except Exception as error:
        log_saver(log, f'line 137 --> {error}')
        is_success = False
        return is_success


def items_handler(items, restaurant, location, log):
    is_success = True
    try:
        for item in items:
            query_set = MenuItem.objects.filter(
                Q(otter_item_id=items[item]['id']), restaurant=restaurant, locations=location)

            if len(query_set) > 1:
                print('multi category found')
                # query_set.delete()
                for i in query_set:
                    print('more than one item found with same otter id ', i.id)
                    if Rating.objects.filter(menuItem=i.id).exists():
                        print('rating exits --> do not remove the menu')
                        continue
                    i.delete()

            if MenuItem.objects.filter(Q(otter_item_id=items[item]['id']), restaurant=restaurant, locations=location).exists():
                print('menu item found')
                update_item = MenuItem.objects.get(
                    Q(otter_item_id=items[item]['id']), restaurant=restaurant, locations=location)
                if update_item.name != items[item]['name']:
                    update_item.name = items[item]['name']
                else:
                    log_saver(log, f'item name up to date')

                if update_item.description != items[item]['description']:
                    update_item.description = items[item]['description']
                else:
                    log_saver(log, f'item description up to date')

                if update_item.base_price != items[item]['price']['amount']:
                    update_item.base_price = items[item]['price']['amount']
                else:
                    log_saver(log, f'item base_price up to date')

                if update_item.currency != items[item]['price']['currencyCode']:
                    update_item.currency = items[item]['price']['currencyCode']
                else:
                    log_saver(log, f'item currency up to date')

                update_item.disabled = False
                update_item.save()

                photoIds = items[item]['photoIds']

                for photo in photoIds:
                    if Image.objects.filter(otter_image_id=photo).exists():
                        image = Image.objects.get(otter_image_id=photo)
                        update_item.images.add(image)
            else:
                print('menu item not found')
                itemID = MenuItem.objects.create(
                    name=items[item]['name'],
                    description=items[item]['description'],
                    base_price=items[item]['price']['amount'],
                    currency=items[item]['price']['currencyCode'],
                    restaurant=restaurant,
                    otter_item_id=items[item]['id']
                )
                itemID.locations.add(location)

                photoIds = items[item]['photoIds']

                for photo in photoIds:
                    if Image.objects.filter(otter_image_id=photo).exists():
                        image = Image.objects.get(otter_image_id=photo)
                        itemID.images.add(image)
        return is_success
    except Exception as error:
        log_saver(log, f'line 187 --> {error}')
        is_success = False
        return is_success


def add_item_category(categories, restaurant, location, log):
    is_success = True
    try:
        for category in categories:

            items = categories[category]['itemIds']
            for itemID in items:
                if MenuItem.objects.filter(otter_item_id=itemID, restaurant=restaurant, locations=location).exists():
                    print('menu found')

                    item = MenuItem.objects.get(
                        otter_item_id=itemID, restaurant=restaurant, locations=location)
                    if Category.objects.filter(otter_category_id=category, restaurant=restaurant, locations=location).exists():
                        categoryID = Category.objects.get(
                            otter_category_id=category, restaurant=restaurant, locations=location)
                        item.category.add(categoryID)
                        print('category added')
                        log_saver(log, f'category added to the item')
                    else:
                        log_saver(log, f'Category not exists')
                else:
                    print('menu not found --> 223')
                    log_saver(log, f'Menu item does not exists')
        return is_success
    except Exception as error:
        log_saver(log, f'line 215 --> {error}')
        is_success = False
        return is_success


def add_item_menu(menus, restaurant, location, log):
    is_success = True
    try:
        print('menus --> ', menus)
        for menu in menus:
            print('adding menu to category and items --> ', menu)
            categories = menus[menu]['categoryIds']
            print('categories --> ', categories)
            for category in categories:
                if Menu.objects.filter(otter_menu_id=menus[menu]['id'], restaurant=restaurant, locations=location).exists():
                    menuID = Menu.objects.get(
                        otter_menu_id=menus[menu]['id'], restaurant=restaurant, locations=location)
                    if Category.objects.filter(otter_category_id=category).exists():
                        categoryID = Category.objects.get(
                            otter_category_id=category)

                        categoryID.menu = menuID

                        print('menu added to the category')

                        if menuID.disabled:
                            categoryID.shared_with.remove(menuID)
                        else:
                            categoryID.shared_with.add(menuID)

                        print('shared category added to the category')
                        categoryID.save()
                        items = MenuItem.objects.filter(
                            category__otter_category_id=category)
                        for item in items:
                            item.menu = menuID
                            item.save()
                    else:
                        log_saver(log, f'line 239 --> Category not exists')
                else:
                    log_saver(log, f'line 241 --> Menu not exists')

        print('completed')
        return is_success
    except Exception as error:
        log_saver(log, f'line 244 --> {error}')
        is_success = False
        return is_success


def handle_modifier_group(modifierGroups, restaurant, location, menus, items, log):
    print(list(menus.keys()))
    first_key = list(menus.keys())[0]
    menu = Menu.objects.get(otter_menu_id=first_key)
    for modifierGroup_id in modifierGroups:
        log_saver(log, f'269 --> {modifierGroup_id}')

        query_set = ModifierGroup.objects.filter(
            otter_id=modifierGroups[modifierGroup_id]['id'], restaurant=restaurant, locations=location)

        if len(query_set) > 1:
            print('multi category found')
            query_set.delete()

        if ModifierGroup.objects.filter(otter_id=modifierGroups[modifierGroup_id]['id'], restaurant=restaurant, locations=location).exists():
            log_saver(log, f'modifier found')
            modifier = ModifierGroup.objects.get(
                otter_id=modifierGroups[modifierGroup_id]['id'])
            modifier.name = modifierGroups[modifierGroup_id]['name']
            modifier.modifier_limit = modifierGroups[modifierGroup_id]['maxPerModifierSelectionQuantity']
            modifier.item_limit = modifierGroups[modifierGroup_id]['maxPerModifierSelectionQuantity']
            modifier.restaurant = restaurant
            modifier.menu = menu
            modifier.save()
            log_saver(log, f'modifier items adding')
            for item in modifierGroups[modifierGroup_id]['itemIds']:
                item_obj = MenuItem.objects.get(
                    otter_item_id=item, restaurant=restaurant, locations=location)
                modifier.modifier_items.add(item_obj)

        else:
            log_saver(log, f'modifier not found')
            minimumSelections = modifierGroups[modifierGroup_id]['minimumSelections']
            requirement_status = 'required'
            if minimumSelections == 0:
                requirement_status = 'optional'

            modifier = ModifierGroup.objects.create(
                otter_id=modifierGroups[modifierGroup_id]['id'],
                name=modifierGroups[modifierGroup_id]['name'],
                requirement_status=requirement_status,
                modifier_limit=minimumSelections,
                item_limit=modifierGroups[modifierGroup_id]['maxPerModifierSelectionQuantity'],
                restaurant=restaurant,
                menu=menu
            )
            modifier.locations.add(location)
            log_saver(log, f'modifier items adding')
            for item in modifierGroups[modifierGroup_id]['itemIds']:
                item_obj = MenuItem.objects.get(
                    otter_item_id=item, restaurant=restaurant, locations=location)
                modifier.modifier_items.add(item_obj)

    log_saver(log, f'adding modifiers to items')

    for item in items:
        if MenuItem.objects.filter(Q(otter_item_id=items[item]['id']), restaurant=restaurant, locations=location).exists():
            menu_item = MenuItem.objects.get(
                Q(otter_item_id=items[item]['id']), restaurant=restaurant, locations=location)

            for modifier in items[item]['modifierGroupIds']:
                if ModifierGroup.objects.filter(otter_id=modifier, restaurant=restaurant, locations=location).exists():
                    modifier_obj = ModifierGroup.objects.get(
                        otter_id=modifier, restaurant=restaurant, locations=location)
                    modifier_obj.used_by.add(menu_item)
                    log_saver(
                        log, f'added {modifier_obj.name} modifier to {menu_item} item')
                else:
                    log_saver(log, 'ModifierGroup not exits')
        else:
            item_id = items[item]['id']
            log_saver(log, f'menu item not exists {item_id}')

    return


'''
Compare data between server and otter

'''


def extract_to_ids(json_data):
    ids = [items["id"] for items in json_data.values()]
    return ids


def compareMenu(restaurant, location, menus):
    try:
        menuIDs = list(Menu.objects.filter(restaurant=restaurant.id, locations=location).values_list(
            'otter_menu_id', flat=True))
        menusFromOtter = extract_to_ids(menus)
        notInOtter = list(set(menuIDs) - set(menusFromOtter))

        for i in notInOtter:
            items = Menu.objects.filter(otter_menu_id=i)
            for item in items:
                item.disabled = True
                item.save()
        return
    except Exception as error:
        pass


def compareCategory(restaurant, location, categories):
    try:
        categoryIDs = list(Category.objects.filter(restaurant=restaurant.id, locations=location).values_list(
            'otter_category_id', flat=True))
        categoriesFromOtter = extract_to_ids(categories)
        notInOtter = list(set(categoryIDs) - set(categoriesFromOtter))

        for i in notInOtter:
            items = Category.objects.filter(otter_category_id=i)
            for item in items:
                item.disabled = True
                item.save()
        return
    except Exception as error:
        pass


def compareItems(restaurant, location, items):
    try:
        itemsIDs = list(MenuItem.objects.filter(restaurant=restaurant.id, locations=location).values_list(
            'otter_item_id', flat=True))
        itemsFromOtter = extract_to_ids(items)
        notInOtter = list(set(itemsIDs) - set(itemsFromOtter))

        for i in notInOtter:
            items = MenuItem.objects.filter(otter_item_id=i)
            for item in items:
                item.disabled = True
                item.save()

        return
    except Exception as error:
        pass


def compareModifiers(restaurant, location, modifierGroups):
    try:
        modifierGroupIDs = list(ModifierGroup.objects.filter(restaurant=restaurant.id, locations=location).values_list(
            'otter_id', flat=True))
        modifierGroupsFromOtter = extract_to_ids(modifierGroups)
        notInOtter = list(set(modifierGroupIDs) - set(modifierGroupsFromOtter))
        for i in notInOtter:
            items = ModifierGroup.objects.filter(otter_id=i)
            for item in items:
                item.disabled = True
                item.save()
        return
    except Exception as error:
        pass


def menu_publish(data, restaurant, location):
    is_success = True
    store = data['metadata']['storeId']
    menus = data['metadata']['payload']['menuData']['menus']
    items = data['metadata']['payload']['menuData']["items"]
    categories = data['metadata']['payload']['menuData']["categories"]
    modifierGroups = data['metadata']['payload']['menuData']["modifierGroups"]
    photos = data['metadata']['payload']['menuData']["photos"]

    log = POS_logs.objects.create(
        logs=f"otter logs --> restaurant id {restaurant}")

    log_saver(log, f'store id --> {store}')

    log_saver(log, f'start menus handling')
    menu_status = menu_handler(menus, restaurant, location, log)
    log_saver(log, f'menus handled')

    log_saver(log, f'start categories handling')
    category_status = category_handler(categories, restaurant, location, log)
    log_saver(log, f'categories handled')

    log_saver(log, f'start photos handling')
    photo_status = photo_handler(photos, restaurant, log)
    log_saver(log, f'photos handled')

    log_saver(log, f'start items handling')
    items_status = items_handler(items, restaurant, location, log)
    log_saver(log, f'start items handled')

    log_saver(log, f'adding categories to items')
    add_item_category_status = add_item_category(
        categories, restaurant, location, log)
    log_saver(log, f'adding categories to items successfully')

    log_saver(log, f'adding items to the menu')
    add_item_menu_status = add_item_menu(menus, restaurant, location, log)
    log_saver(log, f'adding items to the menu successfully')

    log_saver(log, f'start modifiers handling')
    handle_modifier_group(modifierGroups, restaurant,
                          location, menus, items, log)
    log_saver(log, f'modifiers handled')

    log_saver(log, f'menu comparing')
    compareMenu(restaurant, location, menus)
    log_saver(log, f'menu comparing completed')

    log_saver(log, f'category comparing')
    compareCategory(restaurant, location, categories)
    log_saver(log, f'category comparing completed')

    log_saver(log, f'items comparing')
    compareItems(restaurant, location, items)
    log_saver(log, f'items comparing completed')

    log_saver(log, f'modifierGroups comparing')
    compareModifiers(restaurant, location, modifierGroups)
    log_saver(log, f'modifierGroups comparing completed')

    log_saver(log, f'menu imported')
    log_saver(log, f'submitting a request to Otter for menu import completion')
    endpoint = 'v1/menus/publish'

    token = ''
    if OtterAuth.objects.filter().exists():
        token = OtterAuth.objects.filter().first().token
    else:
        token = refresh_auth_token()
    initial_headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-Store-Id': location.otter_x_store_id,
        'X-Event-Id': location.otter_x_event_id,
        'scope': 'menus.publish'
    }
    data = {}
    status_list = [menu_status, category_status, photo_status,
                   items_status, add_item_category_status, add_item_menu_status]

    for i in status_list:
        log_saver(log, f'status --> {i}')

    if any(status is False for status in status_list):
        is_success = False

    log_saver(log, f'status --> {is_success}')
    if is_success:
        data = {
            "requestedToCreatedEntityIds": {
                "empty": True,
            }
        }
        response = send_post_request_with_auth_retry(
            endpoint, initial_headers, data)
        log_saver(log, f'response --> {response.text}')

        if response.status_code != 204:
            log_saver(log, f'failed to confirm importing')
            return
        log_saver(log, f'request importing successful')
    return


def otter_remover(data, restaurant, location):
    store = data['metadata']['storeId']
    menus = data['metadata']['payload']['menuData']['menus']
    items = data['metadata']['payload']['menuData']["items"]
    categories = data['metadata']['payload']['menuData']["categories"]
    modifierGroups = data['metadata']['payload']['menuData']["modifierGroups"]
    photos = data['metadata']['payload']['menuData']["photos"]

    menus_list = extract_to_ids(menus)
    item_list = extract_to_ids(items)
    categories_list = extract_to_ids(categories)
    modifierGroups_list = extract_to_ids(modifierGroups)
    photos_list = extract_to_ids(photos)

    menu_query = Menu.objects.filter(otter_menu_id__in=menus_list)
    menu_item_query = MenuItem.objects.filter(otter_item_id__in=item_list)
    category_query = Category.objects.filter(
        otter_category_id__in=categories_list)
    modifiers_query = ModifierGroup.objects.filter(
        otter_id__in=modifierGroups_list)
    photo_query = Image.objects.filter(otter_image_id__in=photos_list)

    for i in menu_query:
        i.delete()

    for i in menu_item_query:
        i.delete()

    for i in category_query:
        i.delete()

    for i in modifiers_query:
        i.delete()

    for i in photo_query:
        i.delete()
