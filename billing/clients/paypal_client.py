import base64

import requests

from billing.models import PaymentDetails
from chatchef.settings import env
from core.utils import get_logger

logger = get_logger()


class PaypalClient:
    def __init__(self):
        self.client_id = env.str('PAYPAL_CLIENT_ID')
        self.secret_key = env.str('PAYPAL_SECRET_KEY')

        self.is_sandbox = True

    def get_paypal_header(self):
        token = f'{self.client_id}:{self.secret_key}'
        encoded_token = base64.b64encode(token.encode("utf-8")).decode("utf-8")
        print(encoded_token)
        return {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {encoded_token}',
        }

    def create_order(self, items, total_amount, payment_details: PaymentDetails):
        data = {
            'purchase_units': [
                {
                    'items': [
                        {
                            'name': item.get('name'),
                            'quantity': item.get('quantity'),
                            'unit_amount': {
                                'currency_code': item.get('currency'),
                                'value': item.get('price')
                            }
                        } for item in items
                    ],
                    'amount': {
                        'currency_code': items[0].get('currency'),
                        'value': total_amount,
                        'breakdown': {
                            'item_total': {
                                'currency_code': items[0].get('currency'),
                                'value': total_amount,
                            }
                        }
                    },
                }
            ],
            'intent': 'CAPTURE'
        }

        if payment_details is not None:
            data['purchase_units'][0]['payee'] = {}
            if payment_details.paypal_email is not None:
                data['purchase_units'][0]['payee']['email_address'] = payment_details.paypal_email
            if payment_details.paypal_merchant_id is not None:
                data['purchase_units'][0]['payee']['merchant_id'] = payment_details.paypal_merchant_id

        response = requests.post(
            'https://api-m.sandbox.paypal.com/v2/checkout/orders',
            headers=self.get_paypal_header(),
            json=data
        )
        return response

    def capture_order(self, order_id):
        response = requests.post(
            f'https://api-m.sandbox.paypal.com/v2/checkout/orders/{order_id}/capture',
            headers=self.get_paypal_header()
        )
        return response
