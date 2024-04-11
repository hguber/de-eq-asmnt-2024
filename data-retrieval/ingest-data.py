import os
import argparse

from time import time
from datetime import datetime

import pandas as pd
import json
from libcomcat.search import count, get_event_by_id, search

#lon min, lon max, lat min, lat max
#sw lon, ne lon, sw lat, ne lat
    
def download_csv(output_file, dt_start, dt_end, mag_start, mag_end, country, co_bb):
    os.system(f"getcsv data/{output_file} \
        -b {co_bb[country]['sw']['lon']} \
        {co_bb[country]['ne']['lon']} \
        {co_bb[country]['sw']['lat']} \
        {co_bb[country]['ne']['lat']} \
        -s {dt_start} \
        -e {dt_end} \
        -m {mag_start} \
        {mag_end} \
        -f csv")

#with open('keys.json', encoding='utf-8') as fh:
    #data = json.load(fh)

def main(params):
    country = params.country
    dt_start = params.dt_start
    dt_end = params.dt_end
    mag_start = params.mag_start
    mag_end = params.mag_end
    with open('countries_bbox.json', encoding = 'utf-8') as j:
        co_bb = json.load(j)
    if country == 'global':
        co_list = []
        for co_obj in co_bb:
            co_list.append(co_obj)
        for co in co_list:
            output_file = f'{co.lower()}_eq.csv'
            download_csv(output_file, dt_start, dt_end, mag_start, mag_end, co, co_bb)
    else:
        for co in country:
            print(country)
            output_file = f'{co.lower()}_eq.csv'
            download_csv(output_file, dt_start, dt_end, mag_start, mag_end, co.upper(), co_bb)

    print(f'file successfully downloaded. final row count of {output_file}: {len(pd.read_csv(output_file))}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to GCP bucket')

    parser.add_argument('--country', nargs='+', help='three-letter country code(s). not specifying country arg will download all countries', default ='global')
    parser.add_argument('--dt_start', required=True, help='date for range start: YYYY-mm-dd, YYYY-mm-ddTHH:MM:SS, or YYYY-mm-ddTHH:MM:SS.s')
    parser.add_argument('--dt_end', required=True, help='date for range end: YYYY-mm-dd, YYYY-mm-ddTHH:MM:SS, or YYYY-mm-ddTHH:MM:SS.s.')
    parser.add_argument('--mag_start', help='mag for range start (0-10): whole number of decimal e.g. 4 or 4.5', default = 4)
    parser.add_argument('--mag_end', help='mag for range end (0-10): whole number of decimal e.g. 4 or 4.5', default = 10)

    args = parser.parse_args()

    main(args)