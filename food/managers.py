import ast

from django.db import transaction
from django.db.models import Manager
from django.db.transaction import atomic

from core.models import ExtraData
from food.models import Category, Image, Menu


class MenuItemManager(Manager):

    def create_from_excel(self, row, menu):
        # print(row)
        name = row[1]
        description = row[2]
        category_name = row[0]
        price = row[3]
        currency = row[4]
        image = row[5]
        modifier_data = row[6]
        is_alcoholic = row[7]

        menu_item_kwargs = {
            'name': name,
            'description': description,
            'menu': menu,
            'base_price': price,
            'restaurant': menu.restaurant,
            'currency': currency
        }
        menu_item = self.create(
            **{k: v for k, v in menu_item_kwargs.items() if v is not None and v != ''})

        if is_alcoholic:
            menu_item.is_alcoholic = True
            menu_item.save()

        if image is not None and image != '':
            try:
                img_obj = Image.objects.create(remote_url=image)
                menu_item.original_image = img_obj
                menu_item.images.add(img_obj)
                menu_item.save(update_fields=['original_image'])
            except Exception as e:
                print(f'Image error: {e}')

        if category_name is not None and category_name != '':
            categories = category_name.split(',')
            categories = self.get_created_categories(
                categories=categories, menu=menu)
            menu_item.category.add(*categories)

        if modifier_data != '':
            self.modifier_handler(menu, menu_item, modifier_data)

        return menu_item

    def modifier_handler(self, menu, menu_item, modifiers_data):
        from food.models import ModifierGroup, ModifierGroupOrder

        modifiers = ast.literal_eval(modifiers_data)
        order = 0

        for modifier in modifiers:
            modifier_data = modifier[0]

            if not ModifierGroup.objects.filter(name=modifier_data, restaurant_id=menu.restaurant.id).exists():
                continue

            try:
                modifier_obj = ModifierGroup.objects.filter(
                    name=modifier_data, restaurant_id=menu.restaurant.id).first()

                modifier_obj.used_by.add(menu_item)
                modifier_obj.menu = menu
                modifier_obj.save()

                ModifierGroupOrder.objects.create(
                    menu_item=menu_item,
                    modifier_group=modifier_obj,
                    order=order
                )

                order += 1

            except Exception as error:
                print(error)

    def modifier_handler_old(self, menu, menu_item, modifier_data):
        from food.models import ModifierGroup
        modifier_data_tuple = ast.literal_eval(modifier_data)
        for i in modifier_data_tuple:
            modifier_list = list(i)
            __name = modifier_list[0]
            __description = modifier_list[1]
            __rq_status = modifier_list[2]
            __modifiers_items = modifier_list[3]
            __modifier_limit = modifier_list[4]
            __items_limit = modifier_list[5]

            modifier_obj, created = ModifierGroup.objects.get_or_create(
                name=__name,
                restaurant=menu.restaurant,
                menu=menu,
            )

            modifier_obj.used_by.add(menu_item)

            if __description and __description != '':
                modifier_obj.description = __description

            if __rq_status and __rq_status != '':
                modifier_obj.requirement_status = __rq_status

            if __modifiers_items and __modifiers_items != '':
                for modifiers_item in __modifiers_items:
                    modifiers_item = list(modifiers_item)
                    modifiers_item_name = modifiers_item[0]
                    modifiers_item_price = modifiers_item[1]

                    modifier_menu_item, created = self.get_or_create(
                        name=modifiers_item_name,
                        base_price=modifiers_item_price,
                        menu=menu,
                        restaurant=menu.restaurant

                    )
                    modifier_obj.modifier_items.add(modifier_menu_item)

            if __modifier_limit and __modifier_limit != '':
                modifier_obj.modifier_limit = __modifier_limit

            if __items_limit and __items_limit != '':
                modifier_obj.item_limit = __items_limit

            modifier_obj.save()

        return modifier_obj

    def create_from_excel_old(self, row, menu):
        original_name = row[0]
        v1_name = row[1]
        v2_name = row[2]
        v3_name = row[3]
        original_category = row[4]
        original_addon = row[5]
        v1_category = row[6]
        v2_category = row[7]
        v3_category = row[8]
        base_price = row[9]
        product_price = row[10]
        virtual_price = row[11]
        original_description = row[12]
        v1_description = row[13]
        v2_description = row[14]
        v3_description = row[15]
        product_image = row[17]
        v1_image = row[19]
        v2_image = row[20]
        v3_image = row[21]

        menu_item_kwargs = {
            'name': original_name,
            'description': original_description,
            'menu': menu,
            'base_price': base_price,
            'virtual_price': virtual_price,
            'restaurant': menu.restaurant
        }

        menu_item = self.create(
            **{k: v for k, v in menu_item_kwargs.items() if v is not None})

        image_links = [product_image, v1_image, v2_image, v3_image]
        images = self.get_created_images(image_links)

        category_list = [original_category,
                         v1_category, v2_category, v3_category]
        categories = self.get_created_categories(category_list, menu)

        extra_name_map = {
            'v1_name': v1_name,
            'v2_name': v2_name,
            'v3_name': v3_name
        }
        extra_names = self.get_name_desc(extra_name_map)

        extra_desc_map = {
            'v1_description': v1_description,
            'v2_description': v2_description,
            'v3_description': v3_description
        }
        extra_descriptions = self.get_name_desc(extra_desc_map)
        try:
            menu_item.original_image = images[0]
            menu_item.images.add(*images)
            menu_item.category.add(*categories)
            menu_item.extra_names.add(*extra_names)
            menu_item.extra_descriptions.add(*extra_descriptions)
            menu_item.save(update_fields=['original_image'])
        except:
            pass
        return menu_item

    def get_created_images(self, image_links):
        images = []
        for image_link in image_links:
            if image_link is not None and image_link != '':
                images.append(Image(remote_url=image_link))
        return Image.objects.bulk_create(images)

    @atomic
    def get_created_categories(self, categories, menu: Menu):
        category_list = []
        for category in categories:
            if category is None or category == '':
                continue
            obj = Category.objects.get_or_create(
                name=category.strip(), menu=menu, restaurant=menu.restaurant)[0]

            obj.shared_with.add(menu)
            category_list.append(obj)

        return category_list

    def get_name_desc(self, data):
        objs = []
        for label, value in data.items():
            if value is not None and value != '':
                objs.append(ExtraData(
                    label=label,
                    value=value
                ))
        return ExtraData.objects.bulk_create(objs)
