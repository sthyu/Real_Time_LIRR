
from google.transit import gtfs_realtime_pb2
import requests
import time 
from datetime import datetime, timezone, timedelta
import os 
#import pandas as pd
import numpy as np
from protobuf_to_dict import protobuf_to_dict
import sqlite3
import csv

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get(' https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr')#?loc=JAM
feed.ParseFromString(response.content)



lirr_feed = protobuf_to_dict(feed)

realtime_data = lirr_feed['entity'] 
#print(realtime_data[0])


def print_arrivals(data,stopID,feed_to_save):

    #feed_to_save = np.empty(0)
    current_time = datetime.now(timezone.utc)
    for train in data:
      
        if 'trip_update' in train:
            trip_update = train['trip_update']
            
        
            direction_id = trip_update['trip']['direction_id'] if 'direction_id' in trip_update['trip'] else None
            
           
            for stop_time in trip_update['stop_time_update']:
                
                trip_id = trip_update['trip']['trip_id']
               
                if stop_time['stop_id'] == stopID:    
                   
                    if 'arrival' in stop_time and 'delay' in stop_time['arrival'] and 'direction_id' in trip_update['trip']:
                        arrival_time_epoch = stop_time['arrival']['time']
                        delay_seconds = stop_time['arrival']['delay']
                        
                      
                        arrival_time_utc = datetime.fromtimestamp(arrival_time_epoch, tz=timezone.utc)
                        
                     
                        local_arrival_time = arrival_time_utc.astimezone()
                        
                        # Check if the arrival time is in the past
                        if local_arrival_time <= current_time:
                           
                            delay_minutes = delay_seconds // 60
                            delay_seconds_remainder = delay_seconds % 60
                            delay_formatted = f"{delay_minutes}m {delay_seconds_remainder}s"

                            direction = 'Long Island'
                            if(direction_id == 1):
                                direction = 'City Terminals'
                            
                            print(f"Train ID: {trip_id} arrived at stop ID 102 with direction of {direction} with a delay of {delay_formatted} at time {local_arrival_time.strftime('%H:%M:%S')}")
                    
                            trip_id = trip_update['trip']['trip_id']
                            start_date = trip_update['trip']['start_date']
                            schedule_relationship = trip_update['trip']['schedule_relationship']
                            schedule_relationship = trip_update['trip']['schedule_relationship']
                            route_id = trip_update['trip']['route_id']
                            data_to_save_trip = {
                                'trip_id': trip_id,
                                'direction_id': direction_id,
                                'delay_formatted': delay_formatted,
                                'local_arrival_time': local_arrival_time, #.strftime('%Y-%m-%d %H:%M:%S'),
                                'start_date': start_date,
                                'schedule_relationship': schedule_relationship,
                                'route_id': route_id,
                                
                            }
                         

                            duplicate = any(
                                entry['trip_id'] == data_to_save_trip['trip_id'] and
                                entry['route_id'] == data_to_save_trip['route_id'] and
                                entry['direction_id'] == data_to_save_trip['direction_id'] and
                                entry['start_date'] == data_to_save_trip['start_date']
                            for entry in feed_to_save)

                            if not duplicate:
                                #feed_to_save = np.append(feed_to_save, np.array([data_to_save_trip]))
                                feed_to_save.append(data_to_save_trip)


#print_arrivals(realtime_data,'102') #102 is JAM

#loops data gathering, saves to file
def loop_data_gathering(time_hours):
   

    end_time = time.time() + time_hours * 3600

    feed_to_save_1 = []
    
    while(time.time()<end_time):

        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(' https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr')#?loc=JAM
        feed.ParseFromString(response.content)

        lirr_feed = protobuf_to_dict(feed) 
        #print(lirr_feed)
        realtime_data = lirr_feed['entity']
        #print(realtime_data[0])
        print_arrivals(realtime_data,'102',feed_to_save_1)
        print(len(feed_to_save_1))

        time.sleep(60)#wait 1 minute

    with open('gtfs_data_collection.csv', 'w', newline='') as csvfile:
        fieldnames = ['trip_id', 'direction', 'delay_formatted', 'local_arrival_time', 'start_date', 'schedule_relationship', 'route_id', 'direction_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(feed_to_save_1)

    return feed_to_save_1


feed_save = loop_data_gathering(4)    
print(len(feed_save))
print(feed_save)


