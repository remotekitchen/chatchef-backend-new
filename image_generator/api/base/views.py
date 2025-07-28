import asyncio
import datetime
import os
import re

import openai
import requests
from django.contrib.sites.shortcuts import get_current_site
from django.db.models import Q
from rest_framework.exceptions import ParseError
from rest_framework.generics import ListAPIView, get_object_or_404
from rest_framework.response import Response
from rest_framework.views import APIView

from chatchef import settings
from chatchef.settings import OPEN_AI_API_KEY, GOOGLE_SEARCH_API_KEY, GOOGLE_SEARCH_ENGINE_ID, REMOVE_BG_API_KEY
from food.models import Menu, Image
from image_generator.api.base.serializers import BaseImageUrlSerializer, SheetDetailsSerializer, ExcelURLSerializer, \
    CellDataSerializer
from image_generator.clients.google_custom_search_client import GoogleCustomSearchClient
from image_generator.models import ImageUrl
from image_generator.services.image_recognition import ImageRecognition
from image_generator.utils import convert_to_png

from image_generator.services.google_sheet_service import GoogleSheetsService


class BaseAiImagesAPIView(APIView):
    def get(self, request):
        req = request.GET.get('req', None)
        if req is None:
            raise ParseError('req is required in the query params!')

        openai.api_key = OPEN_AI_API_KEY

        response = openai.Image.create(
            prompt=req,
            n=3,
            size="1024x1024",
            response_format="url",
        )

        if response and "data" in response:
            data = [{
                'dish': req,
                'url': image["url"],
                'dish_Des': None,
                'match': None
            } for image in response["data"]]

            return Response(data)

        return Response({})


class BaseAiImagesVariationAPIView(APIView):
    def post(self, request):
        req = request.data.get('req')
        openai.api_key = OPEN_AI_API_KEY

        image_bytes = asyncio.run(convert_to_png(req))
        response = openai.Image.create_variation(
            image=image_bytes,
            # image_name="Food Image",
            n=3,
            size="1024x1024",
            # response_format="b64_json",
            user="TestUser"
        )
        if response and "data" in response:
            data = [{
                'dish': req,
                'url': image["url"],
                'dish_Des': None,
                'match': None
            } for image in response["data"]]

            return Response(data)

        return Response({})


class BaseGetImagesListAPIView(ListAPIView):
    serializer_class = BaseImageUrlSerializer

    def get_queryset(self):
        text = self.kwargs['text']
        q_exp = Q(dish_name__icontains=text) | Q(dish_description__icontains=text)
        queryset = ImageUrl.objects.filter(q_exp)
        return queryset


class BaseGetFilteredImageAPIView(APIView):
    def get(self, request):
        category = request.GET.get('category', None)
        search_query = request.GET.get('Searchquery', None)
        category = None if category == "" else category
        search_query = None if search_query == "" else search_query

        query = ""
        if search_query:
            query = search_query.lower()

        search_words = query.split(' ')
        prepositions = [
            "about", "above", "across", "after", "against", "along", "among", "around", "at", "before", "behind",
            "below", "beneath", "beside", "between", "beyond", "by", "concerning", "considering", "despite", "down",
            "during", "except", "for", "from", "in", "inside", "into", "like", "near", "of", "off", "on", "onto",
            "out", "outside", "over", "past", "regarding", "round", "since", "through", "throughout", "till", "to",
            "toward", "under", "underneath", "until", "unto", "up", "upon", "with", "within", "without"
        ]

        filtered_words = [word for word in search_words if word.lower() not in prepositions]
        filtered_query_string = ' '.join(filtered_words)

        total_word_count = len(filtered_query_string)

        matched_result = []
        final_result = []

        if not category and search_query:
            # Combine the Q objects using the OR operator
            query = Q()
            for word in filtered_words:
                query |= Q(dish_name__icontains=word) | Q(dish_description__icontains=word)
            # for word in filtered_words:
            #     result_set = Imageurls.objects.filter(
            #         Q(dish_name__icontains=word) | Q(dish_description__icontains=word)
            #     )[:20]
            #
            #     matched_result.extend(result_set)
            matched_result = ImageUrl.objects.filter(query).values('dish_description', 'dish_name', 'weblink')[:20]

        elif category and not search_query:
            result_set = ImageUrl.objects.filter(
                cusine_category__iexact=category
            )

            return Response(
                {
                    'dish': item.dish_name,
                    'dish_Des': item.dish_description,
                    'url': item.weblink,
                    'match': 99
                }
                for item in result_set
            )

        elif category and search_query:
            q_exp = Q()
            for word in filtered_words:
                q_exp |= Q(dish_name__icontains=word) | Q(dish_description__icontains=word)
            q_exp &= Q(cusine_category__iexact=category)
            # for word in filtered_words:
            #     q_exp = Q(cusine_category__iexact=category) & (
            #             Q(dish_name__icontains=word) | Q(dish_description__icontains=word))
            #     result_set = Imageurls.objects.filter(
            #         q_exp
            #     )

            #    matched_result.extend(result_set)
            matched_result = ImageUrl.objects.filter(q_exp).values('dish_description', 'dish_name', 'weblink')[:20]

        else:
            return Response("not found")

        return self.get_final_results(matched_result=matched_result, search_words=search_words)

    def get_final_results(self, matched_result, search_words):
        matching_word_count = 0
        final_result = []
        for item in matched_result:
            dish_name = item.get('dish_name', None)
            dish_description = item.get('dish_description', None)
            weblink = item.get('weblink', None)
            dish_words = dish_name.lower().split(' ')
            matching_words = set(dish_words).intersection(search_words)

            if dish_description:
                description_words = dish_description.lower().split(' ')
                matching_words = matching_words.union(set(description_words).intersection(search_words))

            if matching_words:
                matching_word_count += len(matching_words)
                match = len(matching_words)
                total = len(search_words)

                mi = {
                    'dish': dish_name,
                    'dish_Des': dish_description,
                    'url': weblink,
                    'match': (match / total) * 100
                }

                final_result.append(mi)

        final_result.sort(key=lambda x: x['match'], reverse=True)
        return Response(final_result)


class BaseGetWebImageUrlAPIView(APIView):
    def get(self, request):
        url = request.query_params.get('img_url')
        api_key = GOOGLE_SEARCH_API_KEY
        search_engine_id = GOOGLE_SEARCH_ENGINE_ID
        client = GoogleCustomSearchClient(api_key, search_engine_id)
        result = client.search_by_image_url(url)
        return Response(result)


class BaseChangeBgAPIView(APIView):
    def post(self, request):
        bg_img_urls = [
            'https://encrypted-tbn1.gstatic.com/images?q=tbn:ANd9GcQ7Kc4Mxjc2zTbFIP8wrZFDUwsfFSSTJXp4bLFni99EIWnvZHMW',
            'https://encrypted-tbn2.gstatic.com/images?q=tbn:ANd9GcSayN9GeVvMwetsanuIMl7sWNDjCk5-C5OBeRxL2Bc7nsaLNyaR'
        ]

        bg_img_prompt = request.data.get('bgImageUrl', '')
        bg_img_prompt = None if bg_img_prompt == '' else bg_img_prompt
        if bg_img_prompt:
            openai.api_key = OPEN_AI_API_KEY

            response = openai.Image.create(
                prompt=bg_img_prompt,
                n=2,
                size="1024x1024",
                response_format="url",
            )
            if "data" in response:
                bg_img_urls = [x.get("url") for x in response.get("data")]

        output_img_urls = []

        for bg_url in bg_img_urls:
            data = {
                "image_url": request.data.get("imageUrl"),
                "bg_image_url": bg_url
            }
            headers = {
                "X-Api-Key": REMOVE_BG_API_KEY
            }

            response = requests.post("https://api.remove.bg/v1.0/removebg", data=data, headers=headers)

            if response.status_code == 200:
                image_data = response.content
                file_name = f"{int(datetime.datetime.now().timestamp())}.png"
                path = f"modified_bg/{file_name}"
                # Define the file path where you want to save the image
                file_path = os.path.join(settings.MEDIA_ROOT, path)
                image_url = f"{request.scheme}://{get_current_site(request).domain}/media/{path}"
                os.makedirs(os.path.join(settings.MEDIA_ROOT, "modified_bg"), exist_ok=True)

                with open(file_path, "wb") as file:
                    file.write(image_data)
                output_img_urls.append(image_url)

            else:
                print('Bg change error:', response.text)

        return Response(output_img_urls)


class BaseReadGoogleSheetAPIView(APIView):
    def post(self, request):
        serializer = SheetDetailsSerializer(data=request.data)
        if serializer.is_valid():
            url = serializer.validated_data['url']
            sheet_name = serializer.validated_data['sheetName']

            uri = url
            sheet_id_regex = r'\/d\/(?P<sheetId>[^\/]+)\/'
            match = re.search(sheet_id_regex, uri)
            sheet_id = match.group('sheetId')

            GSS = GoogleSheetsService("Image Url", sheet_id, sheet_name)
            sheet_data = GSS.get_sheet_data()
            return Response(sheet_data)
        else:
            return Response(serializer.errors, status=400)


class BaseWriteGoogleSheetAPIView(APIView):
    def post(self, request, colFrom, colTo):
        serializer = ExcelURLSerializer(data=request.data)
        if serializer.is_valid():
            urls = serializer.validated_data
            uri = urls['SD']['url']
            sheet_id_regex = r'\/d\/(?P<sheetId>[^\/]+)\/'
            match = re.search(sheet_id_regex, uri)
            sheet_id = match.group('sheetId')

            gss = GoogleSheetsService("Image Url", sheet_id, urls['SD']['sheetName'])
            gss.add_sheet_data(colFrom, colTo, urls)

            return Response("Data set to Excel")
        else:
            return Response(serializer.errors, status=400)


class BaseUpdateGoogleSheetAPIView(APIView):
    def post(self, request):
        serializer = CellDataSerializer(data=request.data, many=True)
        if serializer.is_valid():
            cell_data = serializer.validated_data

            sd = cell_data[0]['SD']
            url = sd['url']
            sheet_name = sd['sheetName']

            uri = url
            sheet_id_regex = r'\/d\/(?P<sheetId>[^\/]+)\/'
            match = re.search(sheet_id_regex, uri)
            sheet_id = match.group('sheetId')

            gss = GoogleSheetsService("Image Url", sheet_id, sheet_name)
            gss.update_sheet_data(cell_data)

            return Response("Data set to Excel")
        else:
            return Response(serializer.errors, status=400)


class BaseImageRecognitionAPIView(APIView):
    def post(self, request):
        dataset_links = request.data.get('dataset_links')
        image_link = request.data.get('image_link')
        image_recognition = ImageRecognition()
        similar_images = image_recognition.get_similar_images(dataset_links=dataset_links,
                                                              specific_image_path=image_link)
        return Response(similar_images)


class BaseMenuImageRecognitionAPIView(APIView):
    def get(self, request):
        menu_id = request.query_params.get('menu', None)
        if menu_id is None:
            raise ParseError('menu is not provided!')
        menu_images = list(Image.objects.filter(menuitem__menu_id=menu_id).values_list('remote_url', flat=True))
        if len(menu_images) == 0:
            return Response([])
        print(menu_images)
        image_recognition = ImageRecognition()
        similar_pairs = image_recognition.get_similar_pairs(menu_images)
        return Response(similar_pairs)
