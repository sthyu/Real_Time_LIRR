const fs = require('fs');
const axios = require('axios');
const fastcsv = require('fast-csv');

//from google.transit import gtfs_realtime_pb2;

// Function to fetch data from API endpoint
async function fetchData(url) {
  try {
    const response = await axios.get(url, {
      headers: {
        'x-api-key': 'YOUR_API_KEY', // Replace with your actual API key if required
        'Accept': 'application/json'
      }
    });
    console.log(response.data)
    return response.data; // Return JSON data
  } catch (error) {
    console.error('Error fetching data:', error);
    throw error;
  }
}

// Function to compile train information from JSON response
function compileTrainInfo(data) {
  const departures = data.response.feed.departures;
  const trainInfo = departures.map((departure) => {
    const trip = departure.trip;
    const status = departure.status;

    const trainNumber = trip.shortName || '';
    const destination = departure.destination.name || '';
    const scheduledTime = departure.departure.time || '';
    const scheduledPlatform = trip.stops[0].stop.name || ''; // Assuming platform is the stop name
    const actualArrivalTime = status.status === 'Arrived' ? status.estimatedDeparture.time || '' : 'N/A';

    return {
      trainNumber,
      destination,
      scheduledTime,
      scheduledPlatform,
      actualArrivalTime
    };
  });

  return trainInfo;
}

// Function to write data to CSV file
function writeToCSV(data, filePath) {
  const csvStream = fastcsv.format({ headers: true });
  const writableStream = fs.createWriteStream(filePath, { flags: 'a' });

  csvStream.pipe(writableStream);
  data.forEach((train) => csvStream.write(train));
  csvStream.end();

  console.log('CSV file updated successfully.');
}

// URL to fetch GTFS feed data
//const apiUrl = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr';
const apiUrl = 'https://traintime.lirr.org/api/Departure?loc=JAM'
// Path to CSV file
const filePath = 'train_info.csv';

// Fetch data and process
fetchData(apiUrl)
  .then((data) => {
    const trainInfo = compileTrainInfo(data);
    writeToCSV(trainInfo, filePath);
  })
  .catch((error) => {
    console.error('Error processing data:', error);
  });

//run fetch data (For Jamaica Station Only)
  //loop for every 5 minutes, for a certain amount of time
  function loopDataGathering(hours, filePath) {
    const interval = 5 * 60 * 1000; // 5 minutes in milliseconds
    const duration = hours * 60 * 60 * 1000; // total duration in milliseconds
  
    const intervalId = setInterval(() => {
      fetchData(apiUrl)
        .then((data) => {
          const trainInfo = compileTrainInfo(data);
          writeToCSV(trainInfo, filePath);
        })
        .catch((error) => {
          console.error('Error processing data:', error);
        });
    }, interval);
  
    // Stop the interval after the specified duration
    setTimeout(() => {
      clearInterval(intervalId);
      console.log('Data gathering completed.');
    }, duration);
  }

  loopDataGathering(0.5, filePath);//30 min data gathering   
//let responseData = fetchData('https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr')
//let compiledTrains =  compileTrainInfo(fetchData)
//writeToCSV(compiledTrains, filePath)