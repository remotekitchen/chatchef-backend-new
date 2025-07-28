import json

import requests

from chatchef.settings import env
from Webhook.models import OtterAuth


def refresh_auth_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": f"{env.str('OTTER_CLIENT_ID', default='005f0e66-c524-4a10-8f4a-9f8fcc953d29')}",
        "client_secret": f"{env.str('OTTER_CLIENT_SECRET', default='CC57DM5TDN6VD5ZPNRDQ')}",
        "scope": "menus.publish orders.create menus.upsert_hours orders.update ping callback.error.write menus.entity_suspension storefront.store_hours_configuration storefront.store_pause_unpause storefront.store_availability"
    }

    url = "https://partners.cloudkitchens.com/v1/auth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, headers=headers, data=data)
    response_dict = json.loads(response.text)
    if OtterAuth.objects.filter().exists():
        otter_token_obj = OtterAuth.objects.filter().first()
        otter_token_obj.token = response_dict["access_token"]
        otter_token_obj.save()

    else:
        OtterAuth.objects.create(token=response_dict["access_token"])

    return response_dict["access_token"]
