from pos.code.clover.core.views import BaseCloverWebhook, BaseGetCloverAuthCode


class GetCloverAuthCode(BaseGetCloverAuthCode):
    '''
        Added Clover Credentials to the .env file

        sample:
            CLOVER_END_POINT="https://sandbox.dev.clover.com"
            CLOVER_APP_ID='E64G20Y5'
            CLOVER_APP_SECRET='e016787e-997f-663a'

    '''
    pass


class CloverWebhook(BaseCloverWebhook):
    pass
