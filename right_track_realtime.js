const core = require('right-track-core');
const RightTrackDB = require('right-track-db-sqlite3');
const LIRR = require('right-track-agency-lirr');

// Set up the Right Track DB for LIRR
let db = new RightTrackDB(LIRR);

// Get the Stop for Jamaica (id='15') by querying the RightTrackDB
core.query.stops.getStop(db, '15', function(err, stop) {

  // Load the StationFeed for Jamaica
  LIRR.loadFeed(db, stop, function(err, feed) {

    // Do something with the feed
    console.log(feed);

  });

});

//this is to compile the train information into a usable format
const compileTrainInfo = (response) => {
  const departures = response.response.feed.departures;
  const trainInfo = departures.map((departure) => {
    const trip = departure.trip;
    const status = departure.status;

    const trainNumber = trip.shortName;
    const destination = departure.destination.name;
    const scheduledTime = departure.departure.time;
    const scheduledPlatform = trip.stops[0].stop.name; // Assuming platform is the stop name
    const actualArrivalTime = status.status === "Arrived" ? status.estimatedDeparture.time : "N/A";

    return {
      trainNumber,
      destination,
      scheduledTime,
      scheduledPlatform,
      actualArrivalTime
    };
  });

  return trainInfo;
};

const trainInfo = compileTrainInfo(response);

console.log(trainInfo);

//this is to write to a .csv file
const fs = require('fs');
const fastcsv = require('fast-csv');

const ws = fs.createWriteStream('train_info.csv');
fastcsv
  .write(trainInfo, { headers: true })
  .pipe(ws);

console.log('CSV file written successfully.');