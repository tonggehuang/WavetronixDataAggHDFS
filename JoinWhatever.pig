--Load Data

data_list = LOAD 'Tongge/SWZDI/Sensor_SWZDI.csv' Using PigStorage(',');

data_345 = LOAD 'Tongge/wavetronix_5min_lanes/2018/{201803,201804,201805}' using PigStorage(',');
data_678 = LOAD 'Tongge/wavetronix_5min_lanes/2018/{201806,201807,201808}' using PigStorage(',');
data_91011 = LOAD 'Tongge/wavetronix_5min_lanes/2018/{201809,201810,201811}' using PigStorage(',');

data3 = JOIN data_345 BY $0, data_list BY $0 USING 'REPLICATED';
data6 = JOIN data_678 BY $0, data_list BY $0 USING 'REPLICATED';
data9 = JOIN data_91011 BY $0, data_list BY $0 USING 'REPLICATED';

--Store
store data3 into 'Tongge/SWZDI/aggDataLanes3' using PigStorage(',');
fs -getmerge Tongge/SWZDI/aggDataLanes3 5min_SWZDI1.csv;

store data6 into 'Tongge/SWZDI/aggDataLanes6' using PigStorage(',');
fs -getmerge Tongge/SWZDI/aggDataLanes6 5min_SWZDI2.csv;

store data9 into 'Tongge/SWZDI/aggDataLanes9' using PigStorage(',');
fs -getmerge Tongge/SWZDI/aggDataLanes9 5min_SWZDI3.csv;
