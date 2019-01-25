# WavetronixDataAggHDFS

Code for processing the data collected by Wavetronix Roadway Sensors with a brief data quality indicator.

Reference code for data processing for future needed from other reasearch projects.

## Scheme of raw WAVETRONIX data stored in INTRANS HDFS cluster

detectorID, date, startTime, endTime, status, numOfLanes, laneID_1, count, volume, occupancy, speed, smallCount, 
smallVolume, mediumCount, mediumVolume, largeCount, largeVolume, (laneID_2, count, volume, occupancy, ......)

- "NUll" - no value available
- Attributes split by "," 

Example:
I-74 NB from North Tower to Isle, 20150829, 232740, 232800, operational, 2, 1,null,null,0,0,null,null,
null,null,null,null,2,2,2,6,51,2,2,null,null,null,null

## Scheme of aggregated 5-mins data

detectorID, date, hour, 5minID, numberOfLanes, weightedOccupancy, weightedSpeed, sumVolume, qualityIndicator

- Records with volume > 3600 veh/h/ln are thrown out
- qualityIndicator = 0, accceptable records
- qualityIndicator = 1, duplicated records > 20%
- qualityIndicator = 2, missing records > 20%

Example:
9th Ave to I-29 SB, 20180302, 14, 6, 1, 3.423076923, 44.96342688, 26, 0

sensor '9th Ave to I-29 SB', 03/02/2018, 14:30:00, weighted_speed = 44.96 mph, weighted_occupancy = 3.42, volume = 26, indicator = 0 (the record is acceptable).
