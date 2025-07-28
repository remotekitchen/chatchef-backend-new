import json

import requests

from .auth import refresh_auth_token


def send_post_request(endpoint, headers=None, data=None):
    try:
        url = f"https://partners.cloudkitchens.com/{endpoint}"
        response = requests.post(url, headers=headers, json=data)
        return response
    except Exception as e:
        return f"An error occurred: {str(e)}"


def send_post_request_with_auth_retry(endpoint, initial_headers, data=None, restaurant_instance=None, max_retries=3):
    print('running')
    headers = initial_headers.copy()
    retries = 0

    while retries < max_retries:
        response = send_post_request(endpoint, headers=headers, data=data)
        print(response)
        print(response.text)
        if response.status_code in [401, 403]:
            new_token = refresh_auth_token()
            headers['Authorization'] = f'Bearer {new_token}'
            print(new_token)
            retries += 1
        else:
            return response
    return "Max retry limit reached, unable to refresh token."
