from google.transit import gtfs_realtime_pb2
import requests
import time
from datetime import datetime, timezone
import numpy as np
from protobuf_to_dict import protobuf_to_dict
import csv

def fetch_feed():
    url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr'
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def parse_feed(content):
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(content)
        return feed
    except Exception as e:
        print(f"Failed to parse feed: {e}")
        return None

def print_arrivals(data, stopID, feed_to_save):
    current_time = datetime.now(timezone.utc)
    for train in data:
        if 'trip_update' in train:
            trip_update = train['trip_update']
            direction_id = trip_update['trip'].get('direction_id', None)
            for stop_time in trip_update['stop_time_update']:
                trip_id = trip_update['trip']['trip_id']
                if stop_time['stop_id'] == stopID:
                    if 'arrival' in stop_time and 'delay' in stop_time['arrival'] and 'direction_id' in trip_update['trip']:
                        arrival_time_epoch = stop_time['arrival']['time']
                        delay_seconds = stop_time['arrival']['delay']
                        arrival_time_utc = datetime.fromtimestamp(arrival_time_epoch, tz=timezone.utc)
                        local_arrival_time = arrival_time_utc.astimezone()
                        if local_arrival_time <= current_time:
                            delay_minutes = delay_seconds // 60
                            delay_seconds_remainder = delay_seconds % 60
                            delay_formatted = f"{delay_minutes}m {delay_seconds_remainder}s"
                            direction = 'Long Island' if direction_id == 1 else 'City Terminals'
                            print(f"Train ID: {trip_id} arrived at stop ID 102 with direction of {direction} with a delay of {delay_formatted} at time {local_arrival_time.strftime('%H:%M:%S')}")
                            data_to_save_trip = {
                                'trip_id': trip_id,
                                'direction_id': direction_id,
                                'delay_formatted': delay_formatted,
                                'local_arrival_time': local_arrival_time,
                                'start_date': trip_update['trip']['start_date'],
                                'schedule_relationship': trip_update['trip']['schedule_relationship'],
                                'route_id': trip_update['trip']['route_id']
                            }
                            duplicate = any(
                                entry['trip_id'] == data_to_save_trip['trip_id'] and
                                entry['route_id'] == data_to_save_trip['route_id'] and
                                entry['direction_id'] == data_to_save_trip['direction_id'] and
                                entry['start_date'] == data_to_save_trip['start_date']
                            for entry in feed_to_save)
                            if not duplicate:
                                feed_to_save.append(data_to_save_trip)

def loop_data_gathering(time_hours):
    end_time = time.time() + time_hours * 3600
    feed_to_save_1 = []
    while time.time() < end_time:
        content = fetch_feed()
        if content:
            feed = parse_feed(content)
            if feed:
                lirr_feed = protobuf_to_dict(feed)
                realtime_data = lirr_feed['entity']
                print_arrivals(realtime_data, '102', feed_to_save_1)
        print(len(feed_to_save_1))
        time.sleep(60)
    with open('gtfs_data_collection.csv', 'w', newline='') as csvfile:
        fieldnames = ['trip_id', 'direction', 'delay_formatted', 'local_arrival_time', 'start_date', 'schedule_relationship', 'route_id', 'direction_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(feed_to_save_1)
    return feed_to_save_1

feed_save = loop_data_gathering(5)
print(len(feed_save))
print(feed_save)

