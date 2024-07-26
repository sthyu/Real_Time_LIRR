
from google.transit import gtfs_realtime_pb2
import requests
import time 
from datetime import datetime, timezone, timedelta
import os 
import pandas as pd
import numpy as np
from protobuf_to_dict import protobuf_to_dict
import sqlite3
import csv



def db_setup():
    con = sqlite3.connect("lirr_gtfs.db")
    cur = con.cursor()

    # Drop tables 
    cur.execute('DROP TABLE IF EXISTS agency')
    cur.execute('DROP TABLE IF EXISTS calendar_dates')
    cur.execute('DROP TABLE IF EXISTS feed_info')
    cur.execute('DROP TABLE IF EXISTS routes')
    cur.execute('DROP TABLE IF EXISTS shapes')
    cur.execute('DROP TABLE IF EXISTS stop_times')
    cur.execute('DROP TABLE IF EXISTS stops')
    cur.execute('DROP TABLE IF EXISTS transfers')
    cur.execute('DROP TABLE IF EXISTS trips')
    #crete table
    cur.execute('''CREATE TABLE IF NOT EXISTS agency (
                        agency_id TEXT PRIMARY KEY,
                        agency_name TEXT,
                        agency_url TEXT,
                        agency_timezone TEXT,
                        agency_lang TEXT,
                        agency_phone TEXT)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS calendar_dates (
                        service_id TEXT,
                        date TEXT,
                        exception_type INTEGER,
                        PRIMARY KEY (service_id, date))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS feed_info (
                        feed_publisher_name TEXT,
                        feed_publisher_url TEXT,
                        feed_timezone TEXT,
                        feed_lang TEXT,
                        feed_version TEXT)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS routes (
                        route_id TEXT PRIMARY KEY,
                        route_long_name TEXT,
                        route_type INTEGER,
                        route_color TEXT,
                        route_text_color TEXT)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS shapes (
                        shape_id TEXT,
                        shape_pt_lat REAL,
                        shape_pt_lon REAL,
                        shape_pt_sequence INTEGER)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS stop_times (
                        trip_id TEXT,
                        arrival_time TEXT,
                        departure_time TEXT,
                        stop_id TEXT,
                        stop_sequence INTEGER,
                        pickup_type INTEGER,
                        drop_off_type INTEGER)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS stops (
                        stop_id TEXT PRIMARY KEY,
                        stop_code TEXT,
                        stop_name TEXT,
                        stop_lat REAL,
                        stop_lon REAL,
                        stop_url TEXT,
                        wheelchair_boarding INTEGER)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS transfers (
                        from_stop_id TEXT,
                        to_stop_id TEXT,
                        from_trip_id TEXT,
                        to_trip_id TEXT,
                        transfer_type INTEGER,
                        min_transfer_time INTEGER)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS trips (
                        route_id TEXT,
                        service_id TEXT,
                        trip_id TEXT PRIMARY KEY,
                        trip_headsign TEXT,
                        trip_short_name TEXT,
                        direction_id INTEGER,
                        shape_id TEXT,
                        peak_offpeak INTEGER)''')

    file_list = {
        'agency': 'google_transit/agency.txt',
        'calendar_dates': 'google_transit/calendar_dates.txt',
        'feed_info': 'google_transit/feed_info.txt',
        'routes': 'google_transit/routes.txt',
        'shapes': 'google_transit/shapes.txt',
        'stop_times': 'google_transit/stop_times.txt',
        'stops': 'google_transit/stops.txt',
        'transfers': 'google_transit/transfers.txt',
        'trips': 'google_transit/trips.txt'
    }


    for table_name, file_path in file_list.items():
        with open(file_path, 'r') as file:
            reader = csv.reader(file, delimiter=',')
            headers = next(reader)
            placeholders = ','.join(['?'] * len(headers))
            query = f'INSERT OR IGNORE INTO {table_name} ({",".join(headers)}) VALUES ({placeholders})'
            for row in reader:
                try:
                    cur.execute(query, row)
                except sqlite3.IntegrityError as e:
                    print(f"Error inserting row into {table_name}: {row}")
                    print(e)
    
    con.commit()
    con.close()
def import_txt_as_list(path):
    lists = {}
    for file_name in os.listdir(path):
        if file_name.endswith('.txt'):
            file_path = os.path.join(path, file_name)
            with open(file_path, 'r') as file:
                lists[file_name] = [line.strip() for line in file]
    return lists


def analyze():
    return 0    


def print_all_tables():
    con = sqlite3.connect("lirr_gtfs.db")
    cur = con.cursor()
    tables = ['agency', 'calendar_dates', 'feed_info', 'routes', 'shapes', 'stop_times', 'stops', 'transfers', 'trips']
    
    for table in tables:
        cur.execute(f'SELECT * FROM {table}')
        rows = cur.fetchall()
        print(f'Table: {table}')
        for row in rows:
            print(row)
        print('\n')
    
    con.close()

def get_info_trip(cur, train):
    trip_id = train['trip_id']
    trip = cur.execute(f'SELECT * FROM trips WHERE trip_id = ?', (trip_id,)).fetchone()
    route = trip[0]
    service_id = trip[1]
    trip_id = trip[2]
    trip_headsign = trip[3]
    trip_short_name = trip[4]
    direction_id = trip[5]
    shape_id = trip[6] #not used
    peak_offpeak = trip[7]
    terminal_info = [route, service_id, trip_headsign, trip_short_name, direction_id, peak_offpeak]
    return terminal_info

#Get all transfers of a particualr tripat station station_id
def get_transfers_trip(cur, station_id, trip_id):
    #transfers = cur.execute(f'SELECT ALL FROM transfers WHERE from_stop_id = ?',(station_id)).fechall()
    query = '''
    SELECT *
    FROM transfers
    WHERE from_stop_id = ?
    AND (from_trip_id = ? OR to_trip_id = ?)
    '''
    cur.execute(query, (station_id, trip_id, trip_id))
    transfers = cur.fetchall()
    return transfers
    #return 0

def convert_to_seconds (time_str):
    hours, minutes, seconds = map(int, time_str.split(':'))
    # Calculate the total number of seconds
    seconds = hours * 3600 + minutes * 60 + seconds

    return seconds
#Method to find how many transfers were "missed". This assumes a 3 min buffer period for all transfers.
#Trains with <3 min buffer period 
#Obtains all transfer times via analyzing stop_times.txt
#Returns number of transfers where transfer time is in the negatives
def analyze_transfers_trip(cur, transfers, train_arrival_info,station_id,expected_val,df):
    trip_id = train_arrival_info['trip_id']
    actual_arrival_time_str = train_arrival_info['local_arrival_time']
    other_id = transfers['to_trip_id']
    missed_transfers = 0
    unknown_transfers = 0
    for transfer in transfers  :
        if(transfer['to_trip_id'] == trip_id):
            other_id = transfer['from_trip_id']
        query = '''
        SELECT * 
        FROM stop_times
        WHERE trip_id = ?
        AND stop_id = ?
        '''
        cur.execute(query,(other_id,station_id))
        stop_time_info = cur.fetchall()

        '"trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type"'
        scheduled_arrival_time = stop_time_info['arrival_time']
        scheduled_departure_time = stop_time_info['departure_time']

        dt = datetime.fromisoformat(actual_arrival_time_str)
        actual_arrival_time = dt.time()

        scheduled_arrival_time_sec = convert_to_seconds(scheduled_arrival_time)
        scheduled_departure_time_sec = convert_to_seconds(scheduled_departure_time)

        actual_arrival_time_sec = convert_to_seconds(actual_arrival_time)

        diff_arrival_times = actual_arrival_time_sec -scheduled_arrival_time_sec

        diff_departure_times = actual_arrival_time_sec-scheduled_departure_time_sec

        if(expected_val == 1):
            
            #find local arrival time
            train_info = find_train_csv(df, other_id)
            if(train_info.empty):
                unknown_transfers+=1
            else:
                actual_transfer_arrival_time_unparsed =  find_arrival_time(train_info)   
                actual_transfer_arrival_time_parsed = actual_transfer_arrival_time_unparsed.time()
                actual_transfer_arrival_time_sec = convert_to_seconds(actual_transfer_arrival_time_parsed)
                diff_actual_arrival_times =  actual_arrival_time_sec - actual_transfer_arrival_time_sec
                if(diff_actual_arrival_times<0):
                    missed_transfers+=1    
        #use actual arrival times in the sheet        
        else:
            #Use scheduled departure times
            if(diff_departure_times<0):
                missed_transfers+=1
            
    if(expected_val == 1): 
        return missed_transfers, unknown_transfers
    return missed_transfers              
    #return 0    

#given a file path, combine every file inside the list into a single pandas dataframe
def combine_all_csv(file_path):
    
    #list all the files from the directory
    file_list = os.listdir(file_path)

    for file in file_list:
        df = pd.read_csv(file)
        df_combined = df_combined.append(df, ignore_index=True)
    #file_list
    return df_combined 

csv_path_const = 'gtfs_data_collection.csv'

def read_csv(csv_path):
    data = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def find_train_csv(panda_df, train_id):
    train_data = panda_df[panda_df['train_id'] == train_id]
    #if failed
    if(train_data.empty):
        return None
    return train_data

def find_arrival_time(train_data):
    return train_data['local_arrival_time']

def convert_to_seconds(delay_str):
    parts = delay_str.split()
    minutes = int(parts[0]) if parts[1] == 'm' else 0
    seconds = int(parts[2]) if parts[3] == 's' else 0
    total_seconds = minutes * 60 + seconds
    return total_seconds if delay_str[0] != '-' else -total_seconds

#Given a delay string, convert it to a number representing seconds
def parse_delay(delay):
    
    sign = 1
    if delay.startswith('-'):
        sign = -1
        delay = delay[1:]
    
    parts = delay.split(' ')
    minutes = int(parts[0].replace('m', ''))
    seconds = int(parts[1].replace('s', ''))
    total_seconds = minutes * 60 + seconds
    if(sign = -1):
        total_seconds = total_seconds * -1
    return total_seconds 

    
#Given OPTION = 1, remove delays <-15 min or >15 min (Excessive delays)
def clean_data_drop_outliers(data_frame,option):
    data_frame_new = data_frame #make a copy
    data_frame_new = data_frame_new.drop_duplicates()
    if(option = -1):
        for index, row in data_frame.iterrows():
            delay_converted_s = parse_delay(row['delay_formatted'])
            if delay_converted_s >(15*60) or delay_converted_s < (-15*60):
                data_frame_new.at[index, 'reviews_per_month'] = 0
    return data_frame_new

#return number of trains aka length of dataframe
def count_trains(pandas_df):
    return len(pandas_df)

#counts the number of trains on a particular route 
#Routes/Branches: Babylon, Hempstead, Oyster Bay, Ronkonkoma, Montauk, Long Beach, Far Rockaway, West Hempstead, Port Washington, Port Jefferson, Belmont Park, City Terminal
def count_routes(pandas_df):
    return 0
#db_setup()

#print_all_tables()

csv_data = read_csv(csv_path_const)
#print(csv_data)
first_data = csv_data[0]
#print(first_data)

con = sqlite3.connect("lirr_gtfs.db")
cur = con.cursor()
ti = get_info_trip(cur, first_data)
print(ti)

con.close()