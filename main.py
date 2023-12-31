import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# init logging
logging.basicConfig(
    format='%(asctime)s-%(levelname)s: %(message)s',
    level=logging.INFO
)

# init date & time to append to the file name on 1st run
now = datetime.now()
dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")

load_dotenv()

columns = [
    'id',
    'registrationId',
    'paymentType',
    'paymentBrand',
    'amount',
    'currency',
    'descriptor',
    'result',  # contains code and description
    'resultDetails',  # POSSIBLY contains clearingInstituteName, ...
    'card',  # POSSIBLY contains bin, last4Digits, holder, ...
    'customer',  # POSSIBLY contains ip, ...
    'threeDSecure',  # POSSIBLY contains eci
    'customParameters',  # POSSIBLY contains CTPE_DESCRIPTOR_TEMPLATE...
    'risk',  # POSSIBLY contains score
    'timestamp',
    'referencedId'
]

columns_with_json_inside = [
    'result',
    'resultDetails',
    'card',
    'customer',
    'threeDSecure',
    'customParameters',
    'risk'
]

df = pd.DataFrame(columns=columns)


# fetch a list of transaction from a starting page
def fetch_transactions(page: int, include_headers: bool):
    access_token = os.getenv('ACCESS_TOKEN')

    url = 'https://eu-test.oppwa.com/v3/query'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # All possible for paymentTypes are:
    # CB,CD,CR,DB,CP,IV,PA,RB,RC,RF,RL,RV,AD,IN,FI,CL,CF,DR,RG,RR,TE,CT,DS,
    # RS,SD,AC,MD,EA,RI,3D,SA,EN,ID,IC,DP,CG,TG,SF,IS,VD,DV,AF,GK,RD,TM,FT,
    # KT,AR,AL,PL,FN,FZ,RE,RX,FO,CS,EX,SE,AU,TK,TF,ER
    # separete each with a comma when using multiple types

    params = {
        'entityId': '8a8294174b7ecb28014b9699220015ca',
        'date.from': '2023-11-01 00:00:00',
        'date.to': '2023-11-16 23:59:59',
        'paymentTypes': 'PA',
        'pageNo': page  # do not modify
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

                for record in tqdm(records):

                    row = []
                    for column_name in columns:

                        if column_name in record:
                            row.append(json.dumps(record[column_name])) if column_name in columns_with_json_inside else row.append(
                                record[column_name])

                        else:
                            row.append('')

                    df.loc[len(df)] = row

                file_name = f'downloads/export-{dt_string}.csv'
                df.to_csv(
                    file_name,
                    mode='a',
                    index=False,
                    encoding='utf-8',
                    header=include_headers
                )

                # recursive call until all pages are fetched
                fetch_transactions(
                    next_page, False) if next_page <= page_count else logging.info('Task completed')

            case 429:
                logging.warning(
                    f"http {
                        r.status_code} - {parsed_data['result']['description']}"
                )

                # sleep for 30 seconds because ACI throttles 2 requests per minute
                sleep_in_seconds = 30

                logging.info(
                    f'Sleeping for {
                        sleep_in_seconds} seconds before trying again'
                )

                time.sleep(sleep_in_seconds)

                fetch_transactions(page, False)

            case _:
                logging.error(
                    f"http {
                        r.status_code} - {parsed_data['result']['description']}"
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
        logging.warning('Please input valid number')
        get_user_input()

    except KeyboardInterrupt:
        logging.info('Program terminated manually')

    except Exception as e:
        logging.exception(e)


def main():
    get_user_input()


if __name__ == '__main__':
    main()
