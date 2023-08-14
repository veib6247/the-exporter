import requests


def generate_checkout():
    url = 'https://eu-test.oppwa.com/v1/checkouts'

    headers = {
        'Authorization': 'Bearer OGE4Mjk0MTc0YjdlY2IyODAxNGI5Njk5MjIwMDE1Y2N8c3k2S0pzVDg='}

    payload = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'amount': '92.00',
        'currency': 'EUR',
        'paymentType': 'DB'
    }

    r = requests.post(url=url, data=payload, headers=headers)

    print(f'HTTP Status Code: {r.status_code}')
    print(f'Response Data: {r.text}')
