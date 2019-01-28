package fiveMinAgg;

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

public class FiveMinAgg extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new FiveMinAgg(), args);
		System.exit(res); 
		
	} // end main
	
	public int run ( String[] args ) throws Exception {
		
		int startDate = 201803;
		
		int endDate = 201812;
		
		for(int i=startDate;i<=endDate;i++) 
        
		{	
        	String MM = Integer.toString(i);
        	
        	String YY = MM.substring(0, 4);

        	String input1 = "WAVETRONIX/IOWA/" + YY + "/" + MM;  //input location of the wavetronix raw data
        	
    		String output1 = "Tongge/wavetronix_5min/" + YY + "/" + MM; //output location of the agg data
		
			//int reduce_tasks = 10;  
			Configuration conf1 = new Configuration();
			conf1.set("mapred.textoutputformat.separator", ",");	
			
			// Create the job
			Job job_one = new Job(conf1, "wavetronix_5min"); 
			
			// Attach the job to this Driver
			job_one.setJarByClass(FiveMinAgg.class);
			
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
				
				// station volume		
				
				int sumStationVolume=0;
				
				// weighted speed and occupancy for station
				
				double sumStationSpeed=0.0;
				double sumStationOcc=0.0;
				
				// Loop over the split lanes data
				
				for (int i=0; i<stationLanes; i++)
				{
					String laneVehCount = nodes[i*11+7];
					
					// no vehicle present during the night time
					
					if(laneVehCount.equals("null"))
					{
						laneVehCount="0";
					}
					
					String laneOcc = nodes[i*11+9];
					
					String laneSpeed = nodes[i*11+10];
					
					int laneVehCountInt = (int)Double.parseDouble(laneVehCount);
					double laneOccDouble = Double.parseDouble(laneOcc);
					double laneSpeedDouble = Double.parseDouble(laneSpeed);
					
					// brief data quality check, get rid of unreasonable data records (vol>20 in 20s interval per lane)
					
					if (laneVehCountInt>20 | laneVehCountInt<0)
					{
						laneVehCountInt = 0;
					}
					
					//  brief data quality check, OCC records from Wavetronix sensor does not contain decimal in our cluster (if needed)
					
					/*
					if (laneOccDouble==0 & laneVehCountInt!=0)
					{
						laneVehCountInt = 0;
					}
					*/

					sumStationVolume = sumStationVolume + laneVehCountInt;
					sumStationSpeed = sumStationSpeed + laneSpeedDouble * laneVehCountInt;
					sumStationOcc = sumStationOcc + laneOccDouble * laneVehCountInt;
					
				}
				
				// Map writer
				if (Integer.parseInt(date)>=20151001)
				{
					context.write(new Text(name+","+date+","+hour+","+minsFive),
							new Text (Integer.toString(stationLanes)+","+Double.toString(sumStationSpeed)+","+Double.toString(sumStationOcc)+","+Integer.toString(sumStationVolume)));	//station
				}
				
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

			int aggVolume=0;
			double aggSpeed=0.0;
			double aggOcc=0.0;
			
			int numlanes=0;
			
			// loop the value in the key bag
			
			int i=0;
			
			for (Text val : values) {
				
				String line = val.toString();
							
				String node[] = line.split(",");
				
				aggSpeed = aggSpeed + Double.parseDouble(node[1]);
				
				aggOcc = aggOcc + Double.parseDouble(node[2]);
				
				aggVolume = aggVolume + (int)Double.parseDouble(node[3]);
				
				// get maximum of numlanes
				if (Double.parseDouble(node[0])>numlanes)
				{
					numlanes = (int) Double.parseDouble(node[0]);
				}
				
				i=i+1;
				
			} // end loop values
			
			
			
			// get weighted speed and occ for 5 min
			
			double weightedSpeed=0.0;
			double weightedOcc=0.0;
		
			if (aggVolume==0) 
			{
				weightedSpeed=weightedSpeed+0.0; // no vehicle present
				weightedOcc=weightedOcc+0.0; // no vehicle present
			}
			
			else 
			{
				weightedSpeed = (aggSpeed/aggVolume)/1.609; // mph convertor for weighted speed
				weightedOcc = aggOcc/aggVolume; // weighted occ
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
			
			context.write(new Text(skey), 
					new Text(numlanes+","+Double.toString(weightedOcc)+","+Double.toString(weightedSpeed)+","+Integer.toString(aggVolume)+","+Integer.toString(completenessIndicator)));
			
			} // end reduce
		
		} // reducer
	}
























