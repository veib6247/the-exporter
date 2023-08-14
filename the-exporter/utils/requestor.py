import requests
import os
from dotenv import load_dotenv


def test_env():
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    print(access_token)


def generate_checkout():
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v1/checkouts'

    headers = {
        'Authorization': f'Bearer {access_token}'}

    payload = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'amount': '92.00',
        'currency': 'EUR',
        'paymentType': 'DB'
    }

    r = requests.post(url=url, data=payload, headers=headers)

    print(f'HTTP Status Code: {r.status_code}')
    print(f'Response Data: {r.text}')


# thingy
def fetch_transactions(page: int):
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v3/query'

    headers = {
        'Authorization': f'Bearer {access_token}'}

    params = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'date.from': '2023-08-14 00:00:00',
        'date.to': '2023-08-14 23:59:59',
        'pageNo': str(page)
    }

    r = requests.get(url=url, params=params, headers=headers)

    print(f'HTTP Status Code: {r.status_code}')
    print(f'Response Data: {r.text}')


if __name__ == '__main__':
    test_env()
