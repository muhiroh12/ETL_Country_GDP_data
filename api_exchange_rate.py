import requests
import pandas as pd


api_key = 'your_api_key'
url = f'https://v6.exchangerate-api.com/v6/{api_key}/latest/USD'

response = requests.get(url)

data = response.json()

get_conversion_rates = data['conversion_rates']

for currency, rate in get_conversion_rates.items():
    #print(f'{currency}: {rate}')
    df = pd.DataFrame(get_conversion_rates.items(), columns=['currency', 'rate'])
    exchange_rate = df.to_csv('./exchange_rate.csv', index=False)