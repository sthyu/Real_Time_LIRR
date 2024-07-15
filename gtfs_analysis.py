
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

def get_transfer(cur, station_id, trip_id):
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

csv_path_const = 'gtfs_data_collection.csv'

def read_csv(csv_path):
    data = []
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data
    


# Setup database and insert data
#db_setup()

# Print all tables to ensure they are set up correctly
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