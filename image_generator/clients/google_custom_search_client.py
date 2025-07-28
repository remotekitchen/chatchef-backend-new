import requests
from rest_framework.views import APIView
from rest_framework.response import Response


class GoogleCustomSearchResult:
    def __init__(self, items):
        self.items = items


class GoogleCustomSearchItem:
    def __init__(self, link):
        self.link = link


class GoogleCustomSearchClient:
    def __init__(self, api_key, search_engine_id):
        self.api_key = api_key
        self.search_engine_id = search_engine_id
        self.base_url = 'https://www.googleapis.com/customsearch/v1'

    def search_by_image_url(self, image_url):
        params = {
            'key': self.api_key,
            'cx': self.search_engine_id,
            'searchType': 'image',
            'q': f'related:{image_url}'
        }
        response = requests.get(self.base_url, params=params)
        result = response.json()
        items = [item['link'] for item in result.get('items', [])]
        return items
