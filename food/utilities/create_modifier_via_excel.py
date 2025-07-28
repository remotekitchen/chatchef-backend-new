import ast

from food.models import MenuItem, ModifierGroup, ModifiersItemsOrder


def create_modifiers_via_excel(row, restaurant, location):
    try:

        modifiers_name = row[0]
        modifiers_description = row[1]
        modifiers_requirement_status = row[2]
        items_limit = row[3]
        items_data = row[4]

        # modifier_obj = None

        # if ModifierGroup.objects.filter(name=modifiers_name, restaurant_id=restaurant.id, locations=location).exists():
        #     modifier_obj = ModifierGroup.objects.get(
        #         name=modifiers_name, restaurant_id=restaurant.id, locations=location)
        # else:
        modifier_obj = ModifierGroup.objects.create(
            name=modifiers_name,
            restaurant_id=restaurant.id,
            item_limit=items_limit,
            description=modifiers_description,
            requirement_status=modifiers_requirement_status

        )
        modifier_obj.locations.add(location)

        items = ast.literal_eval(items_data)

        count = 0
        for i in items:
            _item_name = i[0]
            _item_price = i[1]

            # modifier_item = None

            # if MenuItem.objects.filter(name=_item_name, base_price=_item_price, restaurant_id=restaurant.id, locations=location).exists():

            #     modifier_item = MenuItem.objects.get(
            #         name=_item_name, base_price=_item_price, restaurant_id=restaurant.id, locations=location)
            # else:
            modifier_item = MenuItem.objects.create(
                name=_item_name,
                base_price=_item_price,
                restaurant_id=restaurant.id
            )
            modifier_item.locations.add(location)

            modifier_obj.modifier_items.add(modifier_item)

            ModifiersItemsOrder.objects.create(
                menu_item=modifier_item,
                modifier_item=modifier_obj,
                order=count
            )
            count += 1

        return

    except Exception as error:
        print(error)


def create_modifiers_via_excel_wrapper(row_list, restaurant, location):
    for row in row_list[1:]:
        create_modifiers_via_excel(row, restaurant, location)


def create_menu_via_excel(row_list, menu):
    for row in row_list[1:]:
        items = []
        try:
            item = MenuItem.objects.create_from_excel(row, menu)
        except Exception as e:
            pass
