import requests
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.views import View


# Create your views here.
class UbereatsLoginRedirectView(View):
    # Login url
    # https://login.uber.com/oauth/v2/authorize?client_id=x8aKRR2qQBjCsUBI1hnaHSmYWaRTifHU&response_type=code&redirect_uri=http://127.0.0.1:8000/ubereats/login/redirect/&scope=eats.pos_provisioning
    def get(self, request):
        print(request.GET)
        code = request.GET.get('code', None)
        if code is not None:
            client_secret = 'V0aWHFOGSKhAvhPoHSeF-TVN_MayQVmvJqy5dNmE'
            client_id = 'x8aKRR2qQBjCsUBI1hnaHSmYWaRTifHU'
            token_endpoint = 'https://login.uber.com/oauth/v2/token/'
            scopes = 'eats.pos_provisioning'
            body = {
                'client_secret': client_secret,
                'client_id': client_id,
                'grant_type': 'authorization_code',
                'redirect_uri': 'http://127.0.0.1:8000/ubereats/login/redirect/',
                'code': code
            }
            response = requests.post(token_endpoint, data=body)
            print(response.json())
            return HttpResponse(response.text)
            # print(response)

        return HttpResponse(f'Access token: {request.GET.get("access_token")}')
