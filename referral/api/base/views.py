import hashlib
import random
import string

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from referral.api.base.serializers import (BaseGetReferralSerializer,
                                           BaseInviteCodeSerializer,
                                           BaseReferralSerializer)
from referral.models import InviteCodes, Referral


class BaseReferralAPIView(APIView):
    def get(self, request):
        user_id = request.query_params.get("user")
        restaurant = request.query_params.get("restaurant")
        location = request.query_params.get("location")

        if not Referral.objects.filter(
                user=user_id, restaurant=restaurant, location=location).exists():

            referral_data = {
                "user": user_id,
                "restaurant": restaurant,
                "location": location,
            }

            _sr = BaseReferralSerializer(data=referral_data)
            _sr.is_valid(raise_exception=True)
            _sr.save()

        data = Referral.objects.get(
            user=user_id, restaurant=restaurant, location=location)
        sr = BaseGetReferralSerializer(data)
        return Response(sr.data)

    def patch(self, request, pk=None):
        if not Referral.objects.filter(user=pk).exists():
            return Response("invalid user id!", status=status.HTTP_400_BAD_REQUEST)

        obj = Referral.objects.get(id=pk)
        sr = BaseReferralSerializer(obj, data=request.data, partial=True)
        sr.is_valid(raise_exception=True)
        sr.save()
        return Response(BaseGetReferralSerializer(sr.instance).data)


class BaseInviteCodeAPIView(APIView):
    def get(self, request):
        user_id = request.query_params.get("user")
        restaurant = request.query_params.get("restaurant")
        location = request.query_params.get("location")
        if not Referral.objects.filter(
                user=user_id, restaurant=restaurant, location=location).exists():

            referral_data = {
                "user": user_id,
                "restaurant": restaurant,
                "location": location,
            }

            _sr = BaseReferralSerializer(data=referral_data)
            _sr.is_valid(raise_exception=True)
            _sr.save()

        refer_obj = Referral.objects.get(
            user=user_id, restaurant=restaurant, location=location)

        invite_code = self.get_invite_code(user_id, restaurant, location)
        sr = BaseInviteCodeSerializer(
            data={"refer": refer_obj.id, "code": invite_code})
        sr.is_valid(raise_exception=True)
        sr.save()
        return Response(sr.data)

    def get_invite_code(self, user, restaurant, location):
        if not all([user, restaurant, location]):
            raise ValueError(
                "User, restaurant, and location must all be provided")

        def create_code():
            base_string = f"{user}{restaurant}{location}"
            salt = ''.join(random.choices(
                string.ascii_letters + string.digits, k=8))
            combined_string = base_string + salt
            hash_object = hashlib.sha256(combined_string.encode())
            code = hash_object.hexdigest()[:5]
            return code

        code = create_code()

        while InviteCodes.objects.filter(code=code).exists():
            code = create_code()
        return code
