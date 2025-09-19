import requests

LOGIN_URL = "https://www.tradingview.com/accounts/signin/"

payload = {
    "username": "axitdacloang@gmail.com",
    "password": "hanhdam1",
    "remember": "on"
}

headers = {
    "Referer": "https://www.tradingview.com/"
}

session = requests.session()
resp = session.post(LOGIN_URL, data=payload, headers=headers)

# TV_COOKIE
tv_cookie = session.cookies.get_dict()
print("TV_COOKIE:", tv_cookie)

# Nếu TradingView trả về auth_token trong JSON
try:
    auth_token = resp.json().get("user", {}).get("auth_token")
    print("TV_AUTH_TOKEN:", auth_token)
except:
    print("Response:", resp.text)
