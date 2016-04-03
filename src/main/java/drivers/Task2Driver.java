package drivers;

import java.util.*;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;

import mappers.*;
import reducers.*;



public class Task2Driver extends Configured implements Tool{

	private static final String INTERMEDIATE_OUTPUT1 = "/Users/chen/Desktop/task2_intermediate1";
	private static final String INTERMEDIATE_OUTPUT2 = "/Users/chen/Desktop/task2_intermediate2";
	
	public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new Task2Driver(), args);
			     System.exit(res);
	}

	public int run(String[] args) throws Exception {


		/**
		 * Job1 driver
		 */

		Configuration conf1 = new Configuration();

	 	Job job1 = Job.getInstance(conf1, "job1");
	    job1.setJarByClass(Task2Driver.class);

	    //set mapper
	 //   job1.setMapperClass(Job1Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	    //set reducer
	    
	    /**
	     * set reducer number    a partitioner?
	     */
	    job1.setNumReduceTasks(3);
	    job1.setReducerClass(Job1Reducer.class);

	    //set output format
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    //set input and output path
	  //  FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
	   MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Job1Mapper1.class);
	   MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Job1Mapper2.class);

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

	    //set reducer
	    job2.setReducerClass(Job2Reducer.class);

	    //set output format
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);

	    //set input and output path
	    FileInputFormat.addInputPath(job2, new Path(this.INTERMEDIATE_OUTPUT1 + "/part*"));
	   
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    
	    
	    
	    
	    job1.waitForCompletion(true);
	    return job2.waitForCompletion(true)? 0 : 1;
	}
}