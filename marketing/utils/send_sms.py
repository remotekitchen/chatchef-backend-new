import requests
from twilio.rest import Client
from chatchef.settings import env

# Twilio credentials
TWILIO_ACCOUNT_SID = env.str("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = env.str("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER = env.str("TWILIO_FROM_NUMBER")

SMS_API_KEY = env.str("SMS_API_KEY")
URL = "https://api.sms.net.bd/sendsms"


def send_sms_bd(number, text):
    print("Sending SMS to", number)
    payload = {'api_key': SMS_API_KEY, 'msg': f'{text}', 'to': f'{number}'}
    response = requests.request("POST", URL, data=payload)
    return response
  
def send_sms_twilio(number, text):
    try:
        # Create a Twilio client
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        
        # Send the SMS
        message = client.messages.create(
            body=text,
            from_=TWILIO_FROM_NUMBER,
            to=number
        )
        
        return {"status": "success", "message_sid": message.sid}
    except Exception as e:
        # Return error if something goes wrong
        return {"status": "error", "error": str(e)}
