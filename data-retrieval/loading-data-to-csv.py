import io
import pandas as pd

from time import time
from datetime import datetime

import json
from libcomcat.search import count, get_event_by_id, search
from libcomcat.dataframes import get_detail_data_frame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def search_data(dt_start, dt_end, mag_start, mag_end, country, co_bb):

    mag_events = search(starttime=datetime(int(dt_start[0:4]), int(dt_start[5:7]), int(dt_start[8:11])), 
                        endtime=datetime(int(dt_end[0:4]), int(dt_end[5:7]), int(dt_end[8:11])),
                        minlatitude=co_bb[country]['sw']['lat'], 
                        maxlatitude=co_bb[country]['ne']['lat'], 
                        minlongitude=co_bb[country]['sw']['lon'], 
                        maxlongitude=co_bb[country]['ne']['lon'],
                        minmagnitude=mag_start, maxmagnitude=mag_end)
    return mag_events

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    dt_start = kwargs["DT_START"]
    dt_end = kwargs["DT_END"]
    mag_start = kwargs["MAG_START"]
    mag_end = kwargs["MAG_END"]
    headers = kwargs["HEADER"]

    event_list = []
    eq_events = []

    with open('/home/src/default_repo/json_folder/countries_bbox.json', encoding = 'utf-8') as j:
        co_bb = json.load(j)
    for country in list(co_bb.keys()):
        try:
            eq_events = search_data(dt_start, dt_end, mag_start, mag_end, country, co_bb)
        except Exception:
            pass
        for events in eq_events:
            events = events.toDict()
            event_list.append([events['id'], events['time'], events['location'], events['latitude'], events['longitude'],
                events['depth'], events['magnitude'], events['significance'], events['alert'],
                events['url'], events['eventtype'], country])

        print('successfully appended ' + country + ': ' + str(len(event_list)))            
                        
    df = pd.DataFrame(event_list,columns=headers)
    df.to_csv('/home/src/default_repo/data/eq_events.csv', encoding='utf-8')
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'