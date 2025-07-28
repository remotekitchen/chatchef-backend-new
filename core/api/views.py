from rest_framework.response import Response
from rest_framework.views import APIView

from core.api.serializers import AppStoreSerializer
from core.models import AppStore


class GetLatestAPP(APIView):
    def get(self, request):
        type = request.query_params.get('type') or 'oms'
        latest = AppStore.objects.filter(type=type).first()
        sr = AppStoreSerializer(latest)
        return Response(sr.data)
