from billing.models import Order

{
  "eventId": "d6703cc8-9e79-415d-ac03-a4dc7f6ab43c",
  "eventTime": "2007-12-03T10:15:30+01:00",
  "eventType": "delivery.delivery_status_update",
  "metadata": {
    "storeId": "partner-store-unique-identifier",
    "applicationId": "ad4ff59d-04c0-4c7d-8ca3-e3a673f8443d",
    "resourceId": "resource-id-if-needed",
    "payload": {
      "provider": "doordash",
      "courier": {
        "name": "Jane Doe",
        "phone": "+1-555-555-5555",
        "phoneCode": "111 11 111",
        "email": "email@email.com",
        "personalIdentifiers": {
          "taxIdentificationNumber": 1234567890
        }
      },
      "estimatedDeliveryTime": "2007-12-03T10:15:30+01:00",
      "estimatedPickupTime": "2007-12-03T10:15:30+01:00",
      "status": "REQUESTED",
      "deliveryStatus": "REQUESTED",
      "currencyCode": "EUR",
      "baseFee": 0,
      "extraFee": 0,
      "totalFee": 0,
      "distance": {
        "unit": "KILOMETERS",
        "value": 0
      },
      "updatedTime": "2007-12-03T10:15:30+01:00"
    },
    "resourceHref": "resource-href-id-if-needed"
  }
}

def delivery_status_update(data, restaurant):
    order_id = data['metadata']['resourceId']

    order_data = Order.objects.get(order_id=order_id)
    print(order_data.status)
    print(order_id)
    return True