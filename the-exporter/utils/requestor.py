import logging
import os
import requests
import time

from dotenv import load_dotenv

# init logging
logging.basicConfig(level=logging.INFO)


# don't forget to clear the terminal after testing this
def test_env():
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    logging.warning(access_token)
    logging.warning('Clear your terminal after testing this!')


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

    print(f'http: {r.status_code}')
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
        'date.from': '2023-08-15 00:00:00',
        'date.to': '2023-08-15 23:59:59',
        'pageNo': page
    }

    try:
        logging.info(f'Fetching records from page {page}')
        r = requests.get(url=url, params=params, headers=headers)
        parsed_data = dict(r.json())
        next_page = page + 1

        if r.status_code == 200:
            page_count = int(parsed_data.get('pages'))
            logging.info(
                f'Fetching successful! Parsing page {page} of {page_count}')

            records = list(parsed_data['records'])

            # todo: dump data to csv! but for now, embrace the jank!
            for record in records:
                # uuid = record['id']
                with open('export.txt', 'a', encoding='utf-8') as file:
                    file.write('%s\n' % str(record))
                    # file.write('%s\n' % str(uuid))

            # recursive call until all pages are fetched
            if next_page <= int(page_count):
                fetch_transactions(next_page)
            else:
                logging.info('Writing data to text file completed')

        else:
            logging.warning(
                f"http: {r.status_code} - {parsed_data['result']['description']}")

            # sleep for 30 seconds because ACI throttles 2 requests per minute
            sleep_in_seconds = 30
            logging.info(
                f'Sleeping for {sleep_in_seconds} seconds before trying again')

            time.sleep(sleep_in_seconds)

            fetch_transactions(page)

    except requests.exceptions.ConnectTimeout:
        logging.error('The connection timed out')

    except KeyboardInterrupt:
        logging.info('Program terminated manually')

    except Exception as e:
        logging.error('Program terminated unexpectedly')
        logging.error(e)


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

    logging.info(f'Fetching list...')
    r = requests.get(url=url, params=params, headers=headers)
    parsed_data = dict(r.json())

    if r.status_code == 200:
        logging.info(f'http: {r.status_code}')
        list_of_transactions = list(parsed_data.get('records'))

        # print(list_of_transactions)
        logging.info(f'Found {len(list_of_transactions)} items in the list')

    else:
        print(parsed_data)


if __name__ == '__main__':
    test_env()
    # fetch_transactions(1)
    # fetch_transactions_as_list()
    # pass
