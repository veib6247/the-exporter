import requests
import os
import time
import logging
from dotenv import load_dotenv

# init logging
logging.basicConfig(level=logging.INFO)


def test_env():
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    print(access_token)


# for COPYANDPAY
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


# fetch a list of transaction from a starting page
def fetch_transactions(page: int):
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v3/query'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    params = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'date.from': '2023-08-14 00:00:00',
        'date.to': '2023-08-14 23:59:59',
        'pageNo': page
    }

    logging.info(f'Fetching data from page {page}...')
    r = requests.get(url=url, params=params, headers=headers)
    parsed_data = dict(r.json())
    next_page = page + 1

    if r.status_code == 200:
        page_count = int(parsed_data.get('pages'))
        logging.info(f'Parsing page {page} of {page_count} pages')

        records = list(parsed_data['records'])

        logging.info('Dumping items to file')

        # todo: dump data to csv! but for now, embrace the jank!
        for record in records:
            # uuid = record['id']
            with open('records_per_page.txt', 'a', encoding='utf-8') as file:
                file.write('%s\n' % str(record))
                # file.write('%s\n' % str(uuid))

        # recursive call until all pages are fetched
        if page <= int(page_count):
            fetch_transactions(next_page)
        else:
            logging.info('das ol folks!')

    else:
        logging.warning(
            f"Http code: {r.status_code} - {parsed_data['result']['description']}")

        logging.info('Sleeping for 30 secs before trying again')

        # sleep for 30 seconds because ACI throttles 2 requests per minute
        time.sleep(30)
        fetch_transactions(page)


# just one beeg list, limited to only 500 results, lol
def fetch_transactions_as_list():
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v3/query'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    params = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'date.from': '2023-08-14 00:00:00',
        'date.to': '2023-08-14 23:59:59',
        'limit': 500
    }

    print(f'Fetching list...')
    r = requests.get(url=url, params=params, headers=headers)
    parsed_data = dict(r.json())

    if r.status_code == 200:
        print(f'http code: {r.status_code}')
        list_of_transactions = list(parsed_data.get('records'))

        # print(list_of_transactions)
        print(f'Found {len(list_of_transactions)} items in the list')

    else:
        print(parsed_data)


if __name__ == '__main__':
    test_env()
    # fetch_transactions(1)
    # fetch_transactions_as_list()
    # pass
