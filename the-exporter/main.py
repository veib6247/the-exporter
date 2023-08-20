import logging
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

# init logging
logging.basicConfig(
    format='%(asctime)s-%(levelname)s: %(message)s',
    level=logging.INFO
)

load_dotenv()

columns = [
    'id',
    'paymentType',
    'paymentBrand',
    'amount',
    'currency',
    'descriptor',
    'result_code',
    'result_description',
    'resultDetails',
    'card',
    'risk',
    'timestamp'
]

df = pd.DataFrame(columns=columns)


# fetch a list of transaction from a starting page
def fetch_transactions(page: int, include_headers: bool):
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v3/query'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    params = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'date.from': '2023-08-01 00:00:00',
        'date.to': '2023-08-19 23:59:59',
        'paymentTypes': 'DB,RF,PA,CP,RV',
        'pageNo': page
    }

    try:
        logging.info(f'Fetching records from page {page}')
        r = requests.get(url=url, params=params, headers=headers)
        parsed_data = dict(r.json())

        next_page = page + 1

        match r.status_code:
            case 200:
                page_count = int(parsed_data['pages'])

                logging.info(
                    f'Fetching successful! Parsing page {page} of {page_count}'
                )

                records = list(parsed_data['records'])

                for record in records:

                    row = []
                    for column_name in columns:

                        if column_name in record:
                            row.append(record[column_name]['score']) if column_name == 'risk' else row.append(
                                record[column_name])

                        else:
                            match column_name:
                                case 'result_code':
                                    row.append(record['result']['code'])
                                case 'result_description':
                                    row.append(record['result']['description'])
                                case _:
                                    row.append('')

                    df.loc[len(df)] = row

                df.to_csv(
                    'export.csv',
                    mode='a',
                    index=False,
                    header=include_headers
                )

                # recursive call until all pages are fetched
                fetch_transactions(
                    next_page, False) if next_page <= page_count else logging.info('Task completed')

            case 429:
                logging.warning(
                    f"http: {r.status_code} - {parsed_data['result']['description']}"
                )

                # sleep for 30 seconds because ACI throttles 2 requests per minute
                sleep_in_seconds = 30

                logging.info(
                    f'Sleeping for {sleep_in_seconds} seconds before trying again'
                )

                time.sleep(sleep_in_seconds)

                fetch_transactions(page, False)

            case _:
                logging.error(
                    f"http: {r.status_code} - {parsed_data['result']['description']}"
                )

    except requests.exceptions.ConnectTimeout:
        logging.error('The connection timed out')
    except KeyboardInterrupt:
        logging.info('Program terminated manually')
    except Exception as e:
        logging.exception(e)


# recursive too, electric bugaloo!
def get_user_input():
    try:
        page_start = int(input('Enter a start page no: '))
        fetch_transactions(page_start, True)

    except ValueError:
        logging.info('Please input valid number!')
        get_user_input()

    except Exception as e:
        logging.exception(e)


def main():
    get_user_input()


if __name__ == '__main__':
    main()
