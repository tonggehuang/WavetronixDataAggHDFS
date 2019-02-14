package fiveMinAggLanes;

import java.io.*;
import java.lang.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FiveMinAggLanes extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new FiveMinAggLanes(), args);
		System.exit(res); 
		
	} // end main
	
	public int run ( String[] args ) throws Exception {
		
		int startDate = 201803;
		
		int endDate = 201803;
		
		for(int i=startDate;i<=endDate;i++) 
        
		{	
        	String MM = Integer.toString(i);
        	
        	String YY = MM.substring(0, 4);

        	String input1 = "WAVETRONIX/IOWA/" + YY + "/" + MM;  //input location of the wavetronix raw data
        	
    		String output1 = "Tongge/wavetronix_5min_lanes/" + YY + "/" + MM; //output location of the agg data
		
			//int reduce_tasks = 10;  
			Configuration conf1 = this.getConf(); //new Configuration();
			conf1.set("mapred.textoutputformat.separator", ",");	
			
			// Create the job
			Job job_one = new Job(conf1, "wavetronix_5min_lanes"); 
			
			// Attach the job
			job_one.setJarByClass(FiveMinAggLanes.class);
			
			//job_one.setNumReduceTasks(reduce_tasks);

			job_one.setMapOutputKeyClass(Text.class); 
			job_one.setMapOutputValueClass(Text.class); 
			job_one.setOutputKeyClass(Text.class);   
			job_one.setOutputValueClass(Text.class);  
			
			// The class that provides the map method
			job_one.setMapperClass(Map_One.class); 
			
			// The class that provides the reduce method
			job_one.setReducerClass(Reduce_One.class);
			
			// This means each map method receives one line as an input
			job_one.setInputFormatClass(TextInputFormat.class);  
			
			// Decides the Output Format
			job_one.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job_one, new Path(input1)); 
			FileOutputFormat.setOutputPath(job_one, new Path(output1));
	
			// Run the job
			job_one.waitForCompletion(true);
			
        }
	
		return 0;
	
	} // End run
	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		
		{
			String line = value.toString();
			String [] nodes = line.split(",");
			int length = nodes.length;
			
			// agg sensor data with operational status and make sure the data with correct number of lane split
			if (length>6 & ((length-6)%11==0))
			{
				// count number of lanes of the sensors
				int stationLanes = (int)Double.parseDouble(nodes[5]);
				
				// sensor basic info
				String name=nodes[0].trim();
				String date=nodes[1];
				String time=nodes[2];
				
				String hour=time.substring(0, 2);
				String minute=time.substring(2, 4);
				
				// 5-mins block
				int minsFive = (int)Double.parseDouble(minute) / 5;

				// lane seperate
				if (stationLanes == 1) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")){
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1));
					
				} else if (stationLanes == 2) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")){
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					// lane2
					String laneVehCount2 = nodes[1*11+7];
					if(laneVehCount2.equals("null")) {
						laneVehCount2="0";
					}
					String laneOcc2 = nodes[1*11+9];
					String laneSpeed2 = nodes[1*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1+","+
									  laneVehCount2+","+laneOcc2+","+laneSpeed2));
					
				} else if (stationLanes == 3) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")) {
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					// lane2
					String laneVehCount2 = nodes[1*11+7];
					if(laneVehCount2.equals("null")) {
						laneVehCount2="0";
					}
					String laneOcc2 = nodes[1*11+9];
					String laneSpeed2 = nodes[1*11+10];
					
					// lane3
					String laneVehCount3 = nodes[2*11+7];
					if(laneVehCount3.equals("null")) {
						laneVehCount3="0";
					}
					String laneOcc3 = nodes[2*11+9];
					String laneSpeed3 = nodes[2*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1+","+
									  laneVehCount2+","+laneOcc2+","+laneSpeed2+","+
									  laneVehCount3+","+laneOcc3+","+laneSpeed3));
					
				} else if (stationLanes == 4) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")) {
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					// lane2
					String laneVehCount2 = nodes[1*11+7];
					if(laneVehCount2.equals("null")) {
						laneVehCount2="0";
					}
					String laneOcc2 = nodes[1*11+9];
					String laneSpeed2 = nodes[1*11+10];
					
					// lane3
					String laneVehCount3 = nodes[2*11+7];
					if(laneVehCount3.equals("null")) {
						laneVehCount3="0";
					}
					String laneOcc3 = nodes[2*11+9];
					String laneSpeed3 = nodes[2*11+10];
					
					// lane4
					String laneVehCount4 = nodes[3*11+7];
					if(laneVehCount4.equals("null")) {
						laneVehCount4="0";
					}
					String laneOcc4 = nodes[3*11+9];
					String laneSpeed4 = nodes[3*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1+","+
									  laneVehCount2+","+laneOcc2+","+laneSpeed2+","+
									  laneVehCount3+","+laneOcc3+","+laneSpeed3+","+
									  laneVehCount4+","+laneOcc4+","+laneSpeed4));
					
				} else if (stationLanes == 5) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")) {
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					// lane2
					String laneVehCount2 = nodes[1*11+7];
					if(laneVehCount2.equals("null")) {
						laneVehCount2="0";
					}
					String laneOcc2 = nodes[1*11+9];
					String laneSpeed2 = nodes[1*11+10];
					
					// lane3
					String laneVehCount3 = nodes[2*11+7];
					if(laneVehCount3.equals("null")) {
						laneVehCount3="0";
					}
					String laneOcc3 = nodes[2*11+9];
					String laneSpeed3 = nodes[2*11+10];

					// lane4
					String laneVehCount4 = nodes[3*11+7];
					if(laneVehCount4.equals("null")) {
						laneVehCount4="0";
					}
					String laneOcc4 = nodes[3*11+9];
					String laneSpeed4 = nodes[3*11+10];

					// lane5
					String laneVehCount5 = nodes[4*11+7];
					if(laneVehCount5.equals("null")) {
						laneVehCount5="0";
					}
					String laneOcc5 = nodes[4*11+9];
					String laneSpeed5 = nodes[4*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1+","+
									  laneVehCount2+","+laneOcc2+","+laneSpeed2+","+
									  laneVehCount3+","+laneOcc3+","+laneSpeed3+","+
									  laneVehCount4+","+laneOcc4+","+laneSpeed4+","+
									  laneVehCount5+","+laneOcc5+","+laneSpeed5));
					
				} else if (stationLanes == 6) {
					
					// lane1
					String laneVehCount1 = nodes[0*11+7];
					if(laneVehCount1.equals("null")) {
						laneVehCount1="0";
					}
					String laneOcc1 = nodes[0*11+9];
					String laneSpeed1 = nodes[0*11+10];
					
					// lane2
					String laneVehCount2 = nodes[1*11+7];
					if(laneVehCount2.equals("null")) {
						laneVehCount2="0";
					}
					String laneOcc2 = nodes[1*11+9];
					String laneSpeed2 = nodes[1*11+10];
					
					// lane3
					String laneVehCount3 = nodes[2*11+7];
					if(laneVehCount3.equals("null")) {
						laneVehCount3="0";
					}
					String laneOcc3 = nodes[2*11+9];
					String laneSpeed3 = nodes[2*11+10];

					// lane4
					String laneVehCount4 = nodes[3*11+7];
					if(laneVehCount4.equals("null")) {
						laneVehCount4="0";
					}
					String laneOcc4 = nodes[3*11+9];
					String laneSpeed4 = nodes[3*11+10];

					// lane5
					String laneVehCount5 = nodes[4*11+7];
					if(laneVehCount5.equals("null")) {
						laneVehCount5="0";
					}
					String laneOcc5 = nodes[4*11+9];
					String laneSpeed5 = nodes[4*11+10];
					
					// lane6
					String laneVehCount6 = nodes[5*11+7];
					if(laneVehCount6.equals("null")) {
						laneVehCount6="0";
					}
					String laneOcc6 = nodes[5*11+9];
					String laneSpeed6 = nodes[5*11+10];
					
					context.write(new Text(name+","+date+","+hour+","+minsFive+","+stationLanes),
							new Text (laneVehCount1+","+laneOcc1+","+laneSpeed1+","+
									  laneVehCount2+","+laneOcc2+","+laneSpeed2+","+
									  laneVehCount3+","+laneOcc3+","+laneSpeed3+","+
									  laneVehCount4+","+laneOcc4+","+laneSpeed4+","+
									  laneVehCount5+","+laneOcc5+","+laneSpeed5+","+
									  laneVehCount6+","+laneOcc6+","+laneSpeed6));
					
				} // 6 lanes maximum, need to be modified if new sensor which cover the lanes > 6 is added to the feed.
				
			} // end if
			
		} // map
		
	} // mapper
	
	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  
		
		{
			String skey = key.toString();
			
			String nodes[] = skey.split(",");
			
			// get name, date, hour, 5mins from key
			
			String name = nodes[0];
			String date = nodes[1];
			String hour = nodes[2];
			String minsFive = nodes[3];
			String stationLanes = nodes[4];
			
			String timeStamp = null;
        	
        	if ((int)Double.parseDouble(minsFive)*5 < 10) {
        		timeStamp = date + "-" + hour + "0" + (int)Double.parseDouble(minsFive)*5 + "00";
        	} else {
        		timeStamp = date + "-" + hour + Integer.toString((int)Double.parseDouble(minsFive)*5) + "00";
        	}

        	// time format
			DateFormat parseror = new SimpleDateFormat("yyyyMMdd-hhmmss");
			Date dt = null;
			try {
				dt = parseror.parse(timeStamp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
			String formattedDate = formatter.format(dt);
			
			int laneID = (int)Double.parseDouble(stationLanes);
			
			if (laneID == 1) {
				
				int aggVolume1=0;
				double aggSpeed1=0.0;
				double aggOcc1=0.0;
				
				
				int i=0;
				
				for (Text val : values) {
					
					String line = val.toString();
								
					String node[] = line.split(",");
					
					aggVolume1 = aggVolume1 + (int)Double.parseDouble(node[0*3+0]);
					aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0*3+2]) * (int)Double.parseDouble(node[0*3+0]);
					aggOcc1 = aggOcc1 + Double.parseDouble(node[0*3+1]);
					
					i=i+1;
					
				}
				
				double weightedSpeed1=0.0;
				double weightedOcc1=0.0;

				
				if (aggVolume1==0) {
					weightedSpeed1=weightedSpeed1+0.0; // no vehicle present lane1
					weightedOcc1=weightedOcc1+0.0; // no vehicle present lane1
				} else {
					weightedSpeed1 = (aggSpeed1/aggVolume1)/1.609; // mph convertor for weighted speed
					weightedOcc1 = aggOcc1/i; // weighted occ
				}
				
				// data completeness indicator for 5 mins records (20%)
				int completenessIndicator=0;
				
				if (i>18) {
					completenessIndicator = 1; // for duplicated records
				} else if(i<12) {
					completenessIndicator = 2; // for missing records
				} else {
					completenessIndicator = 0; // acceptable records number
				}
				
				context.write(new Text(skey+","+formattedDate),
						new Text (Double.toString(aggVolume1)+","+Double.toString(weightedOcc1)+","+Double.toString(weightedSpeed1)+","+
								Integer.toString(completenessIndicator)));
			} 
			
			else if (laneID == 2) {
				
				int aggVolume1=0;
				int aggVolume2=0;
				double aggSpeed1=0.0;
				double aggSpeed2=0.0;
				double aggOcc1=0.0;
				double aggOcc2=0.0;
				
				// loop the value in the key bag
				int i = 0;
	
				for (Text val : values) {
	
					String line = val.toString();
	
					String node[] = line.split(",");
	
					aggVolume1 = aggVolume1 + (int) Double.parseDouble(node[0 * 3 + 0]);
					aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0 * 3 + 2]) * (int) Double.parseDouble(node[0 * 3 + 0]);
					aggOcc1 = aggOcc1 + Double.parseDouble(node[0 * 3 + 1]);
	
					aggVolume2 = aggVolume2 + (int) Double.parseDouble(node[1 * 3 + 0]);
					aggSpeed2 = aggSpeed2 + Double.parseDouble(node[1 * 3 + 2]) * (int) Double.parseDouble(node[1 * 3 + 0]);
					aggOcc2 = aggOcc2 + Double.parseDouble(node[1 * 3 + 1]);
	
					i = i + 1;
	
					}
	
				double weightedSpeed1 = 0.0;
				double weightedSpeed2 = 0.0;
	
				double weightedOcc1 = 0.0;
				double weightedOcc2 = 0.0;

	
				if (aggVolume1 == 0) {
					weightedSpeed1 = weightedSpeed1 + 0.0; // no vehicle present lane1
					weightedOcc1 = weightedOcc1 + 0.0; // no vehicle present lane1
				} else if (aggVolume2 == 0) {
					weightedSpeed2 = weightedSpeed2 + 0.0; // no vehicle present lane2
					weightedOcc2 = weightedOcc2 + 0.0; // no vehicle present lane2
				} else {
					weightedSpeed1 = (aggSpeed1 / aggVolume1) / 1.609; // mph convertor for weighted speed
					weightedOcc1 = aggOcc1 / i; // weighted occ
					weightedSpeed2 = (aggSpeed2 / aggVolume2) / 1.609; // mph convertor for weighted speed
					weightedOcc2 = aggOcc2 / i; // weighted occ
				}
	
				// data completeness indicator for 5 mins records (20%)
				int completenessIndicator = 0;
	
				if (i > 18) {
					completenessIndicator = 1; // for duplicated records
				} else if (i < 12) {
					completenessIndicator = 2; // for missing records
				} else {
					completenessIndicator = 0; // acceptable records number
				}
	
				context.write(new Text(skey + "," + formattedDate),
						new Text(Double.toString(aggVolume1) + "," + Double.toString(weightedOcc1) + ","
								+ Double.toString(weightedSpeed1) + "," + Double.toString(aggVolume2) + ","
								+ Double.toString(weightedOcc2) + "," + Double.toString(weightedSpeed2) + ","
								+ Integer.toString(completenessIndicator)));
			}
			
			else if (laneID == 3){
				
				int aggVolume1=0;
				int aggVolume2=0;
				int aggVolume3=0;
				
				double aggSpeed1=0.0;
				double aggSpeed2=0.0;
				double aggSpeed3=0.0;
				
				double aggOcc1=0.0;
				double aggOcc2=0.0;
				double aggOcc3=0.0;

				int i=0;
				
				for (Text val : values) {
					
					String line = val.toString();
								
					String node[] = line.split(",");
					
					aggVolume1 = aggVolume1 + (int)Double.parseDouble(node[0*3+0]);
					aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0*3+2]) * (int)Double.parseDouble(node[0*3+0]);
					aggOcc1 = aggOcc1 + Double.parseDouble(node[0*3+1]);
					
					aggVolume2 = aggVolume2 + (int)Double.parseDouble(node[1*3+0]);
					aggSpeed2 = aggSpeed2 + Double.parseDouble(node[1*3+2]) * (int)Double.parseDouble(node[1*3+0]);
					aggOcc2 = aggOcc2 + Double.parseDouble(node[1*3+1]);
					
					aggVolume3 = aggVolume3 + (int)Double.parseDouble(node[2*3+0]);
					aggSpeed3 = aggSpeed3 + Double.parseDouble(node[2*3+2]) * (int)Double.parseDouble(node[2*3+0]);
					aggOcc3 = aggOcc3 + Double.parseDouble(node[2*3+1]);
					
					i=i+1;
				
				}
				
				double weightedSpeed1=0.0;
				double weightedSpeed2=0.0;
				double weightedSpeed3=0.0;
				
				double weightedOcc1=0.0;
				double weightedOcc2=0.0;
				double weightedOcc3=0.0;

				
				if (aggVolume1==0) {
					weightedSpeed1=weightedSpeed1+0.0; // no vehicle present lane1
					weightedOcc1=weightedOcc1+0.0; // no vehicle present lane1
				} else if (aggVolume2==0) {
					weightedSpeed2=weightedSpeed2+0.0; // no vehicle present lane2
					weightedOcc2=weightedOcc2+0.0; // no vehicle present lane2
				} else if (aggVolume3==0) {
					weightedSpeed3=weightedSpeed3+0.0; // no vehicle present lane2
					weightedOcc3=weightedOcc3+0.0; // no vehicle present lane2
				} else {
					weightedSpeed1 = (aggSpeed1/aggVolume1)/1.609; // mph convertor for weighted speed
					weightedOcc1 = aggOcc1/i; // weighted occ
					weightedSpeed2 = (aggSpeed2/aggVolume2)/1.609; // mph convertor for weighted speed
					weightedOcc2 = aggOcc2/i; // weighted occ
					weightedSpeed3 = (aggSpeed3/aggVolume3)/1.609; // mph convertor for weighted speed
					weightedOcc3 = aggOcc3/i; // weighted occ
				}
				
				// data completeness indicator for 5 mins records (20%)
				int completenessIndicator=0;
				
				if (i>18) {
					completenessIndicator = 1; // for duplicated records
				} else if(i<12) {
					completenessIndicator = 2; // for missing records
				} else {
					completenessIndicator = 0; // acceptable records number
				}
				
				context.write(new Text(skey+","+formattedDate),
						new Text (Double.toString(aggVolume1)+","+Double.toString(weightedOcc1)+","+Double.toString(weightedSpeed1)+","+
								Double.toString(aggVolume2)+","+Double.toString(weightedOcc2)+","+Double.toString(weightedSpeed2)+","+
								Double.toString(aggVolume3)+","+Double.toString(weightedOcc3)+","+Double.toString(weightedSpeed3)+","+
								Integer.toString(completenessIndicator)));
			}
			
			else if (laneID == 4){
				
				int aggVolume1=0;
				int aggVolume2=0;
				int aggVolume3=0;
				int aggVolume4=0;
				
				double aggSpeed1=0.0;
				double aggSpeed2=0.0;
				double aggSpeed3=0.0;
				double aggSpeed4=0.0;
				
				double aggOcc1=0.0;
				double aggOcc2=0.0;
				double aggOcc3=0.0;
				double aggOcc4=0.0;

				int i=0;
				
				for (Text val : values) {
					
					String line = val.toString();
								
					String node[] = line.split(",");
					
					aggVolume1 = aggVolume1 + (int)Double.parseDouble(node[0*3+0]);
					aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0*3+2]) * (int)Double.parseDouble(node[0*3+0]);
					aggOcc1 = aggOcc1 + Double.parseDouble(node[0*3+1]);
					
					aggVolume2 = aggVolume2 + (int)Double.parseDouble(node[1*3+0]);
					aggSpeed2 = aggSpeed2 + Double.parseDouble(node[1*3+2]) * (int)Double.parseDouble(node[1*3+0]);
					aggOcc2 = aggOcc2 + Double.parseDouble(node[1*3+1]);
					
					aggVolume3 = aggVolume3 + (int)Double.parseDouble(node[2*3+0]);
					aggSpeed3 = aggSpeed3 + Double.parseDouble(node[2*3+2]) * (int)Double.parseDouble(node[2*3+0]);
					aggOcc3 = aggOcc3 + Double.parseDouble(node[2*3+1]);
					
					aggVolume4 = aggVolume4 + (int)Double.parseDouble(node[3*3+0]);
					aggSpeed4 = aggSpeed4 + Double.parseDouble(node[3*3+2]) * (int)Double.parseDouble(node[3*3+0]);
					aggOcc4 = aggOcc4 + Double.parseDouble(node[3*3+1]);
					
					i=i+1;
				
				}
				
				double weightedSpeed1=0.0;
				double weightedSpeed2=0.0;
				double weightedSpeed3=0.0;
				double weightedSpeed4=0.0;
				
				double weightedOcc1=0.0;
				double weightedOcc2=0.0;
				double weightedOcc3=0.0;
				double weightedOcc4=0.0;

				
				if (aggVolume1==0) {
					weightedSpeed1=weightedSpeed1+0.0; // no vehicle present lane1
					weightedOcc1=weightedOcc1+0.0; // no vehicle present lane1
				} else if (aggVolume2==0) {
					weightedSpeed2=weightedSpeed2+0.0; // no vehicle present lane2
					weightedOcc2=weightedOcc2+0.0; // no vehicle present lane2
				} else if (aggVolume3==0) {
					weightedSpeed3=weightedSpeed3+0.0; // no vehicle present lane2
					weightedOcc3=weightedOcc3+0.0; // no vehicle present lane2
				} else if (aggVolume4==0) {
					weightedSpeed4=weightedSpeed4+0.0; // no vehicle present lane2
					weightedOcc4=weightedOcc4+0.0; // no vehicle present lane2
				} else {
					weightedSpeed1 = (aggSpeed1/aggVolume1)/1.609; // mph convertor for weighted speed
					weightedOcc1 = aggOcc1/i; // weighted occ
					weightedSpeed2 = (aggSpeed2/aggVolume2)/1.609; // mph convertor for weighted speed
					weightedOcc2 = aggOcc2/i; // weighted occ
					weightedSpeed3 = (aggSpeed3/aggVolume3)/1.609; // mph convertor for weighted speed
					weightedOcc3 = aggOcc3/i; // weighted occ
					weightedSpeed4 = (aggSpeed4/aggVolume4)/1.609; // mph convertor for weighted speed
					weightedOcc4 = aggOcc4/i; // weighted occ
				}
				
				// data completeness indicator for 5 mins records (20%)
				int completenessIndicator=0;
				
				if (i>18) {
					completenessIndicator = 1; // for duplicated records
				} else if(i<12) {
					completenessIndicator = 2; // for missing records
				} else {
					completenessIndicator = 0; // acceptable records number
				}
				
				context.write(new Text(skey+","+formattedDate),
						new Text (Double.toString(aggVolume1)+","+Double.toString(weightedOcc1)+","+Double.toString(weightedSpeed1)+","+
								Double.toString(aggVolume2)+","+Double.toString(weightedOcc2)+","+Double.toString(weightedSpeed2)+","+
								Double.toString(aggVolume3)+","+Double.toString(weightedOcc3)+","+Double.toString(weightedSpeed3)+","+
								Double.toString(aggVolume4)+","+Double.toString(weightedOcc4)+","+Double.toString(weightedSpeed4)+","+
								Integer.toString(completenessIndicator)));
			}
			
			else if (laneID == 5) {
				
				int aggVolume1=0;
				int aggVolume2=0;
				int aggVolume3=0;
				int aggVolume4=0;
				int aggVolume5=0;
				
				double aggSpeed1=0.0;
				double aggSpeed2=0.0;
				double aggSpeed3=0.0;
				double aggSpeed4=0.0;
				double aggSpeed5=0.0;

				double aggOcc1=0.0;
				double aggOcc2=0.0;
				double aggOcc3=0.0;
				double aggOcc4=0.0;
				double aggOcc5=0.0;
				
				// loop the value in the key bag
					
				int i = 0;

				for (Text val : values) {

					String line = val.toString();

					String node[] = line.split(",");

					aggVolume1 = aggVolume1 + (int) Double.parseDouble(node[0 * 3 + 0]);
					aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0 * 3 + 2]) * (int) Double.parseDouble(node[0 * 3 + 0]);
					aggOcc1 = aggOcc1 + Double.parseDouble(node[0 * 3 + 1]);

					aggVolume2 = aggVolume2 + (int) Double.parseDouble(node[1 * 3 + 0]);
					aggSpeed2 = aggSpeed2
							+ Double.parseDouble(node[1 * 3 + 2]) * (int) Double.parseDouble(node[1 * 3 + 0]);
					aggOcc2 = aggOcc2 + Double.parseDouble(node[1 * 3 + 1]);

					aggVolume3 = aggVolume3 + (int) Double.parseDouble(node[2 * 3 + 0]);
					aggSpeed3 = aggSpeed3
							+ Double.parseDouble(node[2 * 3 + 2]) * (int) Double.parseDouble(node[2 * 3 + 0]);
					aggOcc3 = aggOcc3 + Double.parseDouble(node[2 * 3 + 1]);

					aggVolume4 = aggVolume4 + (int) Double.parseDouble(node[3 * 3 + 0]);
					aggSpeed4 = aggSpeed4
							+ Double.parseDouble(node[3 * 3 + 2]) * (int) Double.parseDouble(node[3 * 3 + 0]);
					aggOcc4 = aggOcc4 + Double.parseDouble(node[3 * 3 + 1]);

					aggVolume5 = aggVolume5 + (int) Double.parseDouble(node[4 * 3 + 0]);
					aggSpeed5 = aggSpeed5
							+ Double.parseDouble(node[4 * 3 + 2]) * (int) Double.parseDouble(node[4 * 3 + 0]);
					aggOcc5 = aggOcc5 + Double.parseDouble(node[4 * 3 + 1]);

					i = i + 1;

				}

				double weightedSpeed1 = 0.0;
				double weightedSpeed2 = 0.0;
				double weightedSpeed3 = 0.0;
				double weightedSpeed4 = 0.0;
				double weightedSpeed5 = 0.0;

				double weightedOcc1 = 0.0;
				double weightedOcc2 = 0.0;
				double weightedOcc3 = 0.0;
				double weightedOcc4 = 0.0;
				double weightedOcc5 = 0.0;

				if (aggVolume1 == 0) {
					weightedSpeed1 = weightedSpeed1 + 0.0; // no vehicle present lane1
					weightedOcc1 = weightedOcc1 + 0.0; // no vehicle present lane1
				} else if (aggVolume2 == 0) {
					weightedSpeed2 = weightedSpeed2 + 0.0; // no vehicle present lane2
					weightedOcc2 = weightedOcc2 + 0.0; // no vehicle present lane2
				} else if (aggVolume3 == 0) {
					weightedSpeed3 = weightedSpeed3 + 0.0; // no vehicle present lane2
					weightedOcc3 = weightedOcc3 + 0.0; // no vehicle present lane2
				} else if (aggVolume4 == 0) {
					weightedSpeed4 = weightedSpeed4 + 0.0; // no vehicle present lane2
					weightedOcc4 = weightedOcc4 + 0.0; // no vehicle present lane2
				} else if (aggVolume5 == 0) {
					weightedSpeed5 = weightedSpeed5 + 0.0; // no vehicle present lane2
					weightedOcc5 = weightedOcc5 + 0.0; // no vehicle present lane2
				} else {
					weightedSpeed1 = (aggSpeed1 / aggVolume1) / 1.609; // mph convertor for weighted speed
					weightedOcc1 = aggOcc1 / i; // weighted occ
					weightedSpeed2 = (aggSpeed2 / aggVolume2) / 1.609; // mph convertor for weighted speed
					weightedOcc2 = aggOcc2 / i; // weighted occ
					weightedSpeed3 = (aggSpeed3 / aggVolume3) / 1.609; // mph convertor for weighted speed
					weightedOcc3 = aggOcc3 / i; // weighted occ
					weightedSpeed4 = (aggSpeed4 / aggVolume4) / 1.609; // mph convertor for weighted speed
					weightedOcc4 = aggOcc4 / i; // weighted occ
					weightedSpeed5 = (aggSpeed5 / aggVolume5) / 1.609; // mph convertor for weighted speed
					weightedOcc5 = aggOcc5 / i; // weighted occ
				}

				// data completeness indicator for 5 mins records (20%)
				int completenessIndicator = 0;

				if (i > 18) {
					completenessIndicator = 1; // for duplicated records
				} else if (i < 12) {
					completenessIndicator = 2; // for missing records
				} else {
					completenessIndicator = 0; // acceptable records number
				}

				context.write(new Text(skey + "," + formattedDate),
						new Text(Double.toString(aggVolume1) + "," + Double.toString(weightedOcc1) + ","
								+ Double.toString(weightedSpeed1) + "," + Double.toString(aggVolume2) + ","
								+ Double.toString(weightedOcc2) + "," + Double.toString(weightedSpeed2) + ","
								+ Double.toString(aggVolume3) + "," + Double.toString(weightedOcc3) + ","
								+ Double.toString(weightedSpeed3) + "," + Double.toString(aggVolume4) + ","
								+ Double.toString(weightedOcc4) + "," + Double.toString(weightedSpeed4) + ","
								+ Double.toString(aggVolume5) + "," + Double.toString(weightedOcc5) + ","
								+ Double.toString(weightedSpeed5) + "," + Integer.toString(completenessIndicator)));
			}
			
			else if (laneID == 6) {
				
				int aggVolume1=0;
				int aggVolume2=0;
				int aggVolume3=0;
				int aggVolume4=0;
				int aggVolume5=0;
				int aggVolume6=0;
				
				double aggSpeed1=0.0;
				double aggSpeed2=0.0;
				double aggSpeed3=0.0;
				double aggSpeed4=0.0;
				double aggSpeed5=0.0;
				double aggSpeed6=0.0;

				double aggOcc1=0.0;
				double aggOcc2=0.0;
				double aggOcc3=0.0;
				double aggOcc4=0.0;
				double aggOcc5=0.0;
				double aggOcc6=0.0;
				
				// loop the value in the key bag
					int i=0;
					
					for (Text val : values) {
						
					
						String line = val.toString();
									
						String node[] = line.split(",");
						
						aggVolume1 = aggVolume1 + (int)Double.parseDouble(node[0*3+0]);
						aggSpeed1 = aggSpeed1 + Double.parseDouble(node[0*3+2]) * (int)Double.parseDouble(node[0*3+0]);
						aggOcc1 = aggOcc1 + Double.parseDouble(node[0*3+1]);
						
						aggVolume2 = aggVolume2 + (int)Double.parseDouble(node[1*3+0]);
						aggSpeed2 = aggSpeed2 + Double.parseDouble(node[1*3+2]) * (int)Double.parseDouble(node[1*3+0]);
						aggOcc2 = aggOcc2 + Double.parseDouble(node[1*3+1]);
						
						aggVolume3 = aggVolume3 + (int)Double.parseDouble(node[2*3+0]);
						aggSpeed3 = aggSpeed3 + Double.parseDouble(node[2*3+2]) * (int)Double.parseDouble(node[2*3+0]);
						aggOcc3 = aggOcc3 + Double.parseDouble(node[2*3+1]);
						
						aggVolume4 = aggVolume4 + (int)Double.parseDouble(node[3*3+0]);
						aggSpeed4 = aggSpeed4 + Double.parseDouble(node[3*3+2]) * (int)Double.parseDouble(node[3*3+0]);
						aggOcc4 = aggOcc4 + Double.parseDouble(node[3*3+1]);
						
						aggVolume5 = aggVolume5 + (int)Double.parseDouble(node[4*3+0]);
						aggSpeed5 = aggSpeed5 + Double.parseDouble(node[4*3+2]) * (int)Double.parseDouble(node[4*3+0]);
						aggOcc5 = aggOcc5 + Double.parseDouble(node[4*3+1]);
						
						aggVolume6 = aggVolume6 + (int)Double.parseDouble(node[5*3+0]);
						aggSpeed6 = aggSpeed6 + Double.parseDouble(node[5*3+2]) * (int)Double.parseDouble(node[5*3+0]);
						aggOcc6 = aggOcc6 + Double.parseDouble(node[5*3+1]);
						
						i=i+1;
						
					}
					
					double weightedSpeed1=0.0;
					double weightedSpeed2=0.0;
					double weightedSpeed3=0.0;
					double weightedSpeed4=0.0;
					double weightedSpeed5=0.0;
					double weightedSpeed6=0.0;
					
					double weightedOcc1=0.0;
					double weightedOcc2=0.0;
					double weightedOcc3=0.0;
					double weightedOcc4=0.0;
					double weightedOcc5=0.0;
					double weightedOcc6=0.0;
					
					if (aggVolume1==0) {
						weightedSpeed1=weightedSpeed1+0.0; // no vehicle present lane1
						weightedOcc1=weightedOcc1+0.0; // no vehicle present lane1
					} else if (aggVolume2==0) {
						weightedSpeed2=weightedSpeed2+0.0; // no vehicle present lane2
						weightedOcc2=weightedOcc2+0.0; // no vehicle present lane2
					} else if (aggVolume3==0) {
						weightedSpeed3=weightedSpeed3+0.0; // no vehicle present lane2
						weightedOcc3=weightedOcc3+0.0; // no vehicle present lane2
					} else if (aggVolume4==0) {
						weightedSpeed4=weightedSpeed4+0.0; // no vehicle present lane2
						weightedOcc4=weightedOcc4+0.0; // no vehicle present lane2
					} else if (aggVolume5==0) {
						weightedSpeed5=weightedSpeed5+0.0; // no vehicle present lane2
						weightedOcc5=weightedOcc5+0.0; // no vehicle present lane2
					} else if (aggVolume6==0) {
						weightedSpeed6=weightedSpeed6+0.0; // no vehicle present lane2
						weightedOcc6=weightedOcc6+0.0; // no vehicle present lane2
					} else {
						weightedSpeed1 = (aggSpeed1/aggVolume1)/1.609; // mph convertor for weighted speed
						weightedOcc1 = aggOcc1/i; // weighted occ
						weightedSpeed2 = (aggSpeed2/aggVolume2)/1.609; // mph convertor for weighted speed
						weightedOcc2 = aggOcc2/i; // weighted occ
						weightedSpeed3 = (aggSpeed3/aggVolume3)/1.609; // mph convertor for weighted speed
						weightedOcc3 = aggOcc3/i; // weighted occ
						weightedSpeed4 = (aggSpeed4/aggVolume4)/1.609; // mph convertor for weighted speed
						weightedOcc4 = aggOcc4/i; // weighted occ
						weightedSpeed5 = (aggSpeed5/aggVolume5)/1.609; // mph convertor for weighted speed
						weightedOcc5 = aggOcc5/i; // weighted occ
						weightedSpeed6 = (aggSpeed6/aggVolume6)/1.609; // mph convertor for weighted speed
						weightedOcc6 = aggOcc6/i; // weighted occ
					}
					
					// data completeness indicator for 5 mins records (20%)
					int completenessIndicator=0;
					
					if (i>18) {
						completenessIndicator = 1; // for duplicated records
					} else if(i<12) {
						completenessIndicator = 2; // for missing records
					} else {
						completenessIndicator = 0; // acceptable records number
					}
					
					context.write(new Text(skey+","+formattedDate),
							new Text (Double.toString(aggVolume1)+","+Double.toString(weightedOcc1)+","+Double.toString(weightedSpeed1)+","+
									Double.toString(aggVolume2)+","+Double.toString(weightedOcc2)+","+Double.toString(weightedSpeed2)+","+
									Double.toString(aggVolume3)+","+Double.toString(weightedOcc3)+","+Double.toString(weightedSpeed3)+","+
									Double.toString(aggVolume4)+","+Double.toString(weightedOcc4)+","+Double.toString(weightedSpeed4)+","+
									Double.toString(aggVolume5)+","+Double.toString(weightedOcc5)+","+Double.toString(weightedSpeed5)+","+
									Double.toString(aggVolume6)+","+Double.toString(weightedOcc6)+","+Double.toString(weightedSpeed6)+","+
									Integer.toString(completenessIndicator)));
				
				} // end lanes

			} // end reduce
		
		} // reducer
	
	}// public class

