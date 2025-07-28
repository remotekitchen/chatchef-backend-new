import ast

from core.utils import get_logger
from food.models import (MenuItem, ModifierGroup, ModifierGroupOrder,
                         ModifiersItemsOrder)

logger = get_logger()


def save_item_modifiers(self, serializer):
    modifiers = ast.literal_eval(self.request.data.get('modifiers', '[]'))

    for modifier in modifiers:
        if not ModifierGroup.objects.filter(id=modifier).exists():
            print('modifiers objects not exists')
            continue

        ModifierGroupOrder.objects.create(
            menu_item=serializer.instance,
            modifier_group=ModifierGroup.objects.get(id=modifier))


def save_modifiers_items(obj: ModifierGroup):
    for item in obj.used_by.all():
        ModifierGroupOrder.objects.create(
            menu_item=item,
            modifier_group=obj)

    return


def save_modifier_items(obj: ModifierGroup):
    for item in obj.modifier_items.all():
        ModifiersItemsOrder.objects.create(
            menu_item=item, modifier_item=obj)

    return
