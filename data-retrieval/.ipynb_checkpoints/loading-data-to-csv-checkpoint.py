from time import time
from datetime import datetime

import pandas as pd
import json
from libcomcat.search import count, get_event_by_id, search

#lon min, lon max, lat min, lat max
#sw lon, ne lon, sw lat, ne lat

    
def search_data(dt_start, dt_end, mag_start, mag_end, country, co_bb):
    eq_events = []
    co_list = []
    mag_events = search(starttime=datetime(int(dt_start[0:4]), int(dt_start[5:7]), int(dt_start[8:11])), 
                        endtime=datetime(int(dt_end[0:4]), int(dt_end[5:7]), int(dt_end[8:11])),
                        minlatitude=co_bb[country]['sw']['lat'], 
                        maxlatitude=co_bb[country]['ne']['lat'], 
                        minlongitude=co_bb[country]['sw']['lon'], 
                        maxlongitude=co_bb[country]['ne']['lon'],
                        minmagnitude=mag_start, maxmagnitude=mag_end)
    return mag_events

def main(params):
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
        eq_events = search(dt_start, dt_end, mag_start, mag_end, country, co_bb)
        for events in eq_events:
            events = events.toDict()
            event_list.append([events['id'], events['location'], events['latitude'], events['longitude'],
                        events['depth'], events['magnitude'], events['significance'], events['alert'],
                        events['url'], events['eventtype'], country])
        
    df=pd.DataFrame(event_list,columns=headers)
    
