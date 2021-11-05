import requests
from datetime import date


def pull_daily_change_and_volume_csv(ticker):
    url = f"https://fmpcloud.io/api/v3/historical-price-full/{ticker}?datatype=csv&apikey=52c809fe67a824679980abd73e0229d6"
    r = requests.get(url)
    with open(f'./data/{ticker}_daily_{date.today().strftime("%d_%m_%y")}.csv', 'w', newline='') as file:
        file.write(r.text)
