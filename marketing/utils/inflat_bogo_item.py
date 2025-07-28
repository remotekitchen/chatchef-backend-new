from marketing.models import Bogo, BxGy


def inflate_items_after_set_bogo(instance: Bogo):
    inflate_percent = instance.inflate_percent
    for item in instance.items.all():
        inflated = item.base_price * (1 + inflate_percent / 100)
        item.base_price = "{:.2f}".format(inflated)
        print(item.base_price)
        item.save()
    return

# def inflate_items_after_set_bxgy(instance: BxGy):
#     inflate_percent = instance.inflate_percent
#     for item in instance.items.all():
#         inflated = item.base_price * (1 + inflate_percent / 100)
#         item.base_price = "{:.2f}".format(inflated)
#         print(item.base_price)
#         item.save()
#     return