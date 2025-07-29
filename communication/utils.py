from hungrytiger.settings import env

# waht's app Api credentials
Twilo = {

    'account_sid': 'ACe7b9e5a41d474a5826c19fbf35130ce1',
    'account_token': '2b1843a8955f624ad3ef0c60c8c15aae',
    "msg_from": "+18149046396",
}

WhatsApp_header = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + env.str('WhatsApp_User_Access_Token')
}
