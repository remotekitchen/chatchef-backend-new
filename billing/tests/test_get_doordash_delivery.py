import json
from rest_framework.test import APITestCase

from billing.api.v1.serializers import OrderSerializer


class TestDoordashDeliveryData(APITestCase):
    # def test_get_doordash_delivery_data(self, doordash_delivery_data):
    def setUp(self):
        # order_data = {}
        with open("billing/tests/fixtures/order_data.json") as f:
            order_data = json.load(f)
            f.close()
        serializer = OrderSerializer(data=order_data)
        serializer.is_valid(raise_exception=True)
        order = serializer.save()

    def test_doordash_delivery_status(self):
        endpoint = "/api/doordash/v1/status/"
        response = self.client.get(endpoint)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 1)
