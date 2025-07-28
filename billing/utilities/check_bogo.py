from rest_framework.exceptions import ParseError
from rest_framework.generics import get_object_or_404

from marketing.models import Bogo


def check_bogo(order_list, bogo_id, location_id):
    # bogo = get_object_or_404(Bogo, id=bogo_id.id)
    # if bogo.location.id != location_id.id:
    #     raise ParseError('bogo does not belong to the location!')
    return
  
def check_bxgy(order_list, bxgy_id, location_id):
    return
