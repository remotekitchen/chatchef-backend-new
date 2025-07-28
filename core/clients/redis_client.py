# import json

# from django.conf import settings
# from redis import Redis

# from core.constants import get_constant
# from core.utils import get_logger

# logger = get_logger()


# class RedisClient:
#     def __init__(self):
#         self.client = None
#         self.url = settings.REDIS_CLIENT_URL
#         self.prefix_key = settings.REDIS_CLIENT_PREFIX_KEY
#         self.constants = dict()

#     @staticmethod
#     def load_or_decode(value: bytes):
#         try:
#             return json.loads(value)
#         except (json.decoder.JSONDecodeError, Exception):
#             return value.decode("utf-8")

#     @staticmethod
#     def load_or_return(value: bytes):
#         try:
#             return json.loads(value)
#         except (json.decoder.JSONDecodeError, Exception):
#             return value

#     def get_client(self):
#         if self.client:
#             return self.client

#         self.client = Redis.from_url(self.url)
#         return self.client

#     def get(self, key: str, default=None):
#         if not settings.CACHE_ENABLED and not key.startswith("b_token_of"):
#             return None

#         value = self.get_client().get(f"{self.prefix_key}_{key}")
#         if not value:
#             return default

#         return self.load_or_decode(value)

#     def set(self, key: str, value, ex=None):
#         if not settings.CACHE_ENABLED and not key.startswith("b_token_of"):
#             return

#         if ex is None:  
#             ex = settings.REDIS_CLIENT_DEFAULT_TTL
#         if not isinstance(value, int) and not isinstance(value, str):
#             value = json.dumps(value)

#         return self.get_client().set(f"{self.prefix_key}_{key}", value, ex=ex)

#     # Constants
#     def get_constants(self):
#         if self.constants:
#             return self.constants

#         self.constants = self.get("constants", {})
#         return self.constants

#     def get_constant(self, key: str, data_type="str"):
#         if not settings.CACHE_ENABLED:
#             return get_constant(key, data_type)

#         self.get_constants()
#         if not self.constants.get(key):
#             self.constants[key] = get_constant(key, data_type)
#             self.set("constants", self.constants)

#         return self.constants.get(key)

#     def set_constant(self, key: str, value, ex=None):
#         if not settings.CACHE_ENABLED:
#             return

#         self.get_constants()
#         if key in self.constants.keys():
#             value = self.load_or_return(value)
#             self.constants[key] = value
#             self.set("constants", self.constants, ex=ex)

#     # User Profile
#     # def get_user_profile(self, user_id: int):
#     #     if not settings.CACHE_ENABLED:
#     #         return get_billing_profile(user_id=user_id)

#     #     profile = self.get(f"profile_{user_id}")
#     #     if not profile:
#     #         profile = get_billing_profile(user_id=user_id)
#     #         self.set_user_profile(user_id, profile)

#     #     return profile

#     # def set_user_profile(self, user_id, profile_info: dict):
#     #     if not settings.CACHE_ENABLED:
#     #         return

#     #     timeout = 60 * 60 * 24  # 24 hours
#     #     self.set(f"profile_{user_id}", profile_info, ex=timeout)

#     # # Campaign Count
#     # def get_campaign_count(self, campaign_type: str) -> int:
#     #     if not settings.CACHE_ENABLED:
#     #         return get_active_campaign_count(campaign_type)

#     #     count = self.get(f"{campaign_type}_campaign_count")
#     #     if count is None:
#     #         count = get_active_campaign_count(campaign_type)
#     #         self.set_campaign_count(campaign_type, count)

#     #     return int(count) if count else 0

#     # def set_campaign_count(self, campaign_type: str, count: int) -> None:
#     #     if not settings.CACHE_ENABLED:
#     #         return

#     #     key = f"{campaign_type}_campaign_count"
#     #     self.set(key, count)

#     # def increment_campaign_count(self, campaign_type: str) -> None:
#     #     if not settings.CACHE_ENABLED:
#     #         return

#     #     count = self.get_campaign_count(campaign_type) + 1
#     #     self.set_campaign_count(campaign_type, count)

#     # def decrement_campaign_count(self, campaign_type: str) -> None:
#     #     if not settings.CACHE_ENABLED:
#     #         return

#     #     count = max(0, self.get_campaign_count(campaign_type) - 1)
#     #     self.set_campaign_count(campaign_type, count)
