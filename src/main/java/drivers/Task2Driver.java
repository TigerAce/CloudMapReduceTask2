package drivers;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;

import mappers.*;
import reducers.*;



public class Task2Driver extends Configured implements Tool{

	private static final String INTERMEDIATE_OUTPUT1 = "./task2_intermediate1";
	
	private static final int NUMBER_OF_NODES = 14;
	private static final int REDUCE_TASKS_MAXIMUM = 2;

	public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new Task2Driver(), args);
			     System.exit(res);
	}

	public int run(String[] args) throws Exception {


		 FileSystem fs; 
		 
		/**
		 * Job1 driver
		 */

		Configuration conf1 = new Configuration();

		conf1.set("place", args[args.length - 2]);
		
	 	Job job1 = Job.getInstance(conf1, "job1");
	    job1.setJarByClass(Task2Driver.class);

	    //set mapper
	   job1.setMapperClass(Job1Mapper1.class);

	    //set combiner
	   job1.setCombinerClass(Job1Reducer.class);

	    //set reducer
	    
	    /**
	     * set reducer number    a partitioner?
	     */
	    job1.setNumReduceTasks((int) (1.75 * NUMBER_OF_NODES * REDUCE_TASKS_MAXIMUM));
	    job1.setReducerClass(Job1Reducer.class);

	    //set output format
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    
	    for(int i = 0; i < args.length - 2; i++){
			   if(args[i].endsWith(".txt")){
				   FileInputFormat.addInputPath(job1, new Path(args[i]));
			   }
			   else{
				   FileInputFormat.addInputPath(job1, new Path(args[i] + "/*"));
			   }
		   }
	    
	   
	   /**
	     * clear out put path
	     */
	  
	  	fs = FileSystem.get(conf1);
	    /*Check if output path (args[1])exist or not*/
	    if(fs.exists(new Path(this.INTERMEDIATE_OUTPUT1))){
	       /*If exist delete the output path*/
	       fs.delete(new Path(this.INTERMEDIATE_OUTPUT1),true);
	    }
	    
	   FileOutputFormat.setOutputPath(job1, new Path(this.INTERMEDIATE_OUTPUT1));

	   
	   
	   /**
	    * job 2 driver
	    */
		Configuration conf2 = new Configuration();

	 	Job job2 = Job.getInstance(conf2, "job2");
	    job2.setJarByClass(Task2Driver.class);

	    //set mapper
	    
	    
	    job2.setMapperClass(Job2Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	//    job2.setNumReduceTasks((int) (1.75 * NUMBER_OF_NODES * REDUCE_TASKS_MAXIMUM));
	    
	    //set reducer
	    job2.setReducerClass(Job2Reducer.class);

	    //set output format
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);

	   
	    
	    //set input and output path
	    FileInputFormat.addInputPath(job2, new Path(this.INTERMEDIATE_OUTPUT1 + "/part*"));
	   
	    
	    /**
	     * clear out put path
	     */
	  
	  	fs = FileSystem.get(conf2);
	    /*Check if output path (args[1])exist or not*/
	    if(fs.exists(new Path(args[args.length - 1]))){
	       /*If exist delete the output path*/
	       fs.delete(new Path(args[args.length - 1]),true);
	    }
	    
	    FileOutputFormat.setOutputPath(job2, new Path(args[args.length - 1]));
	    
	    
	    
	    
	    job1.waitForCompletion(true);
	    return job2.waitForCompletion(true)? 0 : 1;
	}
}