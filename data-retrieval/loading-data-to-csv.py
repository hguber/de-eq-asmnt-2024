import os
import argparse

from time import time
from datetime import datetime

import pandas as pd
import json
from libcomcat.search import count, get_event_by_id, search

#lon min, lon max, lat min, lat max
#sw lon, ne lon, sw lat, ne lat

dt_start = '1990-01-01'
dt_end = '2024-04-01'
mag_start = 4
mag_end = 10
    
def pull_from_api(dt_start, dt_end, mag_start, mag_end, country, co_bb):
    event_list = []
    eq_events = []
    with open('countries_bbox.json', encoding = 'utf-8') as j:
        co_bb = json.load(j)
    all_countries = [co_obj for co_obj in co_bb]
    for country in all_countries:
        try:
            eq_events = pull_from_api(dt_start, dt_end, mag_start, mag_end, country, co_bb)
        except Exception:
            pass
        print(country)
        for events in eq_events:
            events = events.toDict()
            event_list.append([events['id'], events['location'], events['latitude'], events['longitude'],
                        events['depth'], events['magnitude'], events['significance'], events['alert'],
                        events['url'], events['eventtype'], country])
        
    df=pd.DataFrame(event_list,columns=headers)
    df.to_csv('data', encoding='utf-8')
    return df
#with open('keys.json', encoding='utf-8') as fh:
    #data = json.load(fh)

def main(params):
    event_list = []
    eq_events = []
    with open('countries_bbox.json', encoding = 'utf-8') as j:
        co_bb = json.load(j)
    all_countries = [co_obj for co_obj in co_bb]
    for country in all_countries:
        eq_events = pull_from_api(dt_start, dt_end, mag_start, mag_end, country, co_bb)
        for events in eq_events:
            events = events.toDict()
            event_list.append([events['id'], events['location'], events['latitude'], events['longitude'],
                        events['depth'], events['magnitude'], events['significance'], events['alert'],
                        events['url'], events['eventtype'], country])
        
    df=pd.DataFrame(event_list,columns=headers)
    
