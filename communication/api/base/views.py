import json
import re
from time import sleep

import requests
from rest_framework import viewsets
from rest_framework.generics import GenericAPIView, ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from twilio.rest import Client

from chatchef.settings import env
from communication.api.base.serializer import (BaseTwiloSerializer,
                                               BaseWhatsAppOfferSerializer,
                                               GroupInvitationORSerializer)
from communication.models import (CustomerInfo, GroupInvitationOR,
                                  whatsAppCampaignHistory)
from communication.utils import Twilo, WhatsApp_header
from core.api.paginations import StandardResultsSetPagination
from core.api.permissions import HasRestaurantAccess
from core.utils import get_logger

logger = get_logger()


class BaseTwiloSendMsgView(GenericAPIView):
    serializer_class = BaseTwiloSerializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data

        account_sid = Twilo['account_sid']
        auth_token = Twilo['account_token']
        sender = "MGa7184251895903890087a794bc56e855"  # Twilo['msg_from']
        client = Client(account_sid, auth_token)

        if data.get('msg_to') == 'all':
            customers = CustomerInfo.objects.filter(
                restaurant=data.get("restaurant")).values('contact_no')
            for customer in customers:
                contact_no = customer['contact_no']
                try:
                    sleep(1/8)
                    logger.error('line 46 ', contact_no)
                    # print('line 47 ', contact_no)

                    message = client.messages.create(
                        body=data.get('body'),
                        messaging_service_sid=sender,
                        to=f'{contact_no}'
                    )
                except Exception as error:
                    pass
            return Response("msg send all")

        else:
            message = client.messages.create(
                body=data.get('body'),
                from_=sender,
                to=data.get('msg_to')
            )

            return Response(message.status)

    # def send_twilio_message(self, body, msg_to):
    #     account_sid = Twilo['account_sid']
    #     auth_token = Twilo['account_token']
    #     sender = Twilo['msg_from']
    #     client = Client(account_sid, auth_token)
    #     message = client.messages.create(
    #         body=body,
    #         from_=sender,
    #         to=msg_to
    #     )
    #     logger.info(
    #         f"Twilio message sent to {msg_to} with status {message.status}")
    #     return message.status


class BaseGroupInvitationORModelView(viewsets.ModelViewSet):
    serializer_class = GroupInvitationORSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [IsAuthenticated, HasRestaurantAccess]

    def get_queryset(self):
        restaurant = self.request.query_params.get('restaurant')
        queryset = GroupInvitationOR.objects.all()
        if restaurant:
            queryset = GroupInvitationOR.objects.filter(restaurant=restaurant)
        return queryset


class BaseGroupInvitationORreadOnlyModelView(viewsets.ReadOnlyModelViewSet):
    serializer_class = GroupInvitationORSerializer

    def get_queryset(self):
        restaurant = self.request.query_params.get('restaurant')
        queryset = GroupInvitationOR.objects.all()
        if restaurant:
            queryset = GroupInvitationOR.objects.filter(restaurant=restaurant)
        return queryset

 # WhatsApp template##


version = env.str("WhatsApp_Vesion")
Phone_Number_ID = env.str("WhatsApp_Phone_Number_ID")


class BaseWhatsAppTemplateSent(GenericAPIView):
    serializer_class = BaseWhatsAppOfferSerializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data

        Audience = data.get('audience')
        msg_type = data.get('msg_type')
        url = f'https://graph.facebook.com/{version}/{Phone_Number_ID}/messages'
        headers = WhatsApp_header
        Recipient_Phone_Number = data.get("msg_to")
        body = data.get("body")
        msg_header = data.get("msg_header")
        msg_url = data.get("url")
        img_link = data.get("img_link")
        # Use "Restaurant" key to access the list of restaurant IDs
        restaurant_ids = data.get("restaurant", [])

        # Create and save a new instance of your model
        whatsapp_campaign_history = whatsAppCampaignHistory.objects.create(
            audience=Audience,
            msg_header=msg_header,
            msg_type=msg_type,
            img_link=img_link,
            msg_to=Recipient_Phone_Number,
            body=body,
            url=msg_url,
            time=data.get('time'),
        )

        # Assign the first restaurant ID from the list, if available
        if restaurant_ids:
            whatsapp_campaign_history.restaurant.add(restaurant_ids[0])

        # Save the WhatsApp campaign history record
        whatsapp_campaign_history.save()
        if (Audience == "all"):
            result = CustomerInfo.objects.filter(
                restaurant=data.get("restaurant")).values('contact_no')
            for entry in result:
                contact_no = entry['contact_no']
                if (msg_type == 'template'):

                    data = {
                        "messaging_product": "whatsapp",
                        "recipient_type": "individual",
                        "to": f"{contact_no}",
                        "type": "template",
                        "template": {
                            "name": "offers",
                            "language": {
                                "code": "en"
                            },
                            "components": [
                                {
                                    "type": "header",
                                    "parameters": [
                                        {
                                            "type": "text",
                                            "text": f"{msg_header}"
                                        }
                                    ]
                                },
                                {
                                    "type": "body",
                                    "parameters": [
                                        {"type": "text",
                                         "text": f"{body}"
                                         }


                                    ]
                                },
                                {
                                    "type": "button",
                                    "sub_type": "URL",
                                    "index": "0",
                                    "parameters": [
                                        {
                                            "type": "payload",
                                            "payload": f"{msg_url}"
                                        }
                                    ]
                                }
                            ]
                        }
                    }

                if (msg_type == "text"):

                    data = {"messaging_product": "whatsapp",
                            "to": f"{contact_no}",
                            "text": {
                                "preview_url": 'true',
                                "body": f"{body}. --our page link: {msg_url}"
                            }
                            }
                if (msg_type == "image" and img_link != ""):

                    url = f'https://graph.facebook.com/{version}/{Phone_Number_ID}/messages'
                    headers = WhatsApp_header
                    data = {
                        "messaging_product": "whatsapp",
                        "recipient_type": "individual",
                        "to": f"{contact_no}",
                        "type": "image",
                        "image": {
                            "link": f"{img_link}"
                        }
                    }
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                print(response.text)
                return Response(response.json())
        if (Audience == "member"):
            result = CustomerInfo.objects.filter(restaurant=data.get(
                "restaurant"), is_member=True).values('contact_no')
            for entry in result:
                contact_no = entry['contact_no']
                if (msg_type == 'template'):

                    data = {
                        "messaging_product": "whatsapp",
                        "recipient_type": "individual",
                        "to": f"{contact_no}",
                        "type": "template",
                        "template": {
                            "name": "offers",
                            "language": {
                                "code": "en"
                            },
                            "components": [
                                {
                                    "type": "header",
                                    "parameters": [
                                        {
                                            "type": "text",
                                            "text": f"{msg_header}"
                                        }
                                    ]
                                },
                                {
                                    "type": "body",
                                    "parameters": [
                                        {"type": "text",
                                         "text": f"{body}"
                                         }


                                    ]
                                },
                                {
                                    "type": "button",
                                    "sub_type": "URL",
                                    "index": "0",
                                    "parameters": [
                                        {
                                            "type": "payload",
                                            "payload": f"{msg_url}"
                                        }
                                    ]
                                }
                            ]
                        }
                    }

                if (msg_type == "text"):

                    data = {"messaging_product": "whatsapp",
                            "to": f"{contact_no}",
                            "text": {
                                "preview_url": 'true',
                                "body": f"{body}. --our page link: {msg_url}"
                            }
                            }
                if (msg_type == "image" and img_link != ""):

                    url = f'https://graph.facebook.com/{version}/{Phone_Number_ID}/messages'
                    headers = WhatsApp_header
                    data = {
                        "messaging_product": "whatsapp",
                        "recipient_type": "individual",
                        "to": f"{contact_no}",
                        "type": "image",
                        "image": {
                            "link": f"{img_link}"
                        }
                    }
                response = requests.post(
                    url, headers=headers, data=json.dumps(data))
                print(response.text)
                return Response(response.json())
        if (Audience == "custom"):
            if (msg_type == 'template'):

                data = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": f"{Recipient_Phone_Number}",
                    "type": "template",
                    "template": {
                        "name": "offers",
                        "language": {
                            "code": "en"
                        },
                        "components": [
                            {
                                "type": "header",
                                "parameters": [
                                    {
                                        "type": "text",
                                        "text": f"{msg_header}"
                                    }
                                ]
                            },
                            {
                                "type": "body",
                                "parameters": [
                                    {"type": "text",
                                     "text": f"{body}"
                                     }


                                ]
                            },
                            {
                                "type": "button",
                                "sub_type": "URL",
                                "index": "0",
                                "parameters": [
                                    {
                                        "type": "payload",
                                        "payload": f"{msg_url}"
                                    }
                                ]
                            }
                        ]
                    }
                }

            if (msg_type == "text"):

                data = {"messaging_product": "whatsapp",
                        "to": f"{Recipient_Phone_Number}",
                        "text": {
                            "preview_url": 'true',
                            "body": f"{body}. --our page link: {msg_url}"
                        }
                        }
            if (msg_type == "image" and img_link != ""):

                url = f'https://graph.facebook.com/{version}/{Phone_Number_ID}/messages'
                headers = WhatsApp_header
                data = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": f"{Recipient_Phone_Number}",
                    "type": "image",
                    "image": {
                        "link": f"{img_link}"
                    }
                }
            response = requests.post(
                url, headers=headers, data=json.dumps(data))
            print(response.text)
            return Response(response.json())
        else:
            return


# Whats app template history

class BaseWhatsAppCampaignHistory(ListAPIView):
    serializer_class = BaseWhatsAppOfferSerializer  # Replace with your serializer

    def get_queryset(self):
        restaurant = self.request.query_params.get('restaurant')
        queryset = whatsAppCampaignHistory.objects.all()
        if restaurant:
            queryset = whatsAppCampaignHistory.objects.filter(
                restaurant=restaurant)
        return queryset

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.serializer_class(queryset, many=True)
        return Response(serializer.data)

    # Email communication
