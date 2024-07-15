import requests
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
#import protobuf

# URL of the GTFS-RT feed
url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr'

# Your MTA API key (if required, replace 'YOUR_API_KEY' with the actual key)
headers = {
    'x-api-key': 'YOUR_API_KEY'
}

# Fetch the GTFS-RT feed
response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Create a FeedMessage object
    feed = gtfs_realtime_pb2.FeedMessage()
    
    # Parse the response content into the FeedMessage object
    feed.ParseFromString(response.content)
    
    print(feed)