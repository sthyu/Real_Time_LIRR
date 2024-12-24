This project is intended to collect real time data using GTFS-RT General Transit Feed Specification - Real Time feeds on the Long Island Rail Road (LIRR) at Jamaica station.
Data was collected at a semi-random basis from July 2024 to August 2024. 

Jamaica station is the busiest interchange station on the Long Island Rail Road, with all but one line serving the station. Prior to Grand Central Madison opening, trains would be held for late arriving connections as part of "Timed connections". 
However, after Grand Central Madison, trains would no longer be held due to "Timed Connections". This project sets out to analyze the actual experience of riders at the station that are transferring to other trains when trains are operating normally.
Thus, this analysis excludes trains that are arrive at a time greater than 15 minutes earler or 15 minutes later than the scheduled time.

A "Late" train, by LIRR standards, is a train that arrives later than 5 minutes 59 seconds at its scheduled terminal station. For the purpose of this study, we will define a "Late" train as a train that arrives more than 1 minute (60 seconds) late at Jamaica station.
This is due to transfers that are timed as tight as 5 minutes, and transfers that may take several minutes to complete as passengers may have to go "up and over" the various overpasses at the station (This is guaranteed to happen with Atlantic Terminal shuttle trains).

The actual LIRR_GTFS_Analysis.ipynb contains the actual analysis files, while the mta_gtfs.py contains the file to record the data. 
