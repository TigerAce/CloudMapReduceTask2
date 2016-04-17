package mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper1
			extends Mapper<Object, Text, Text, Text>{
			

			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {

				String[] splitRecord = value.toString().split("\t");
				String splitKey[] = splitRecord[0].split(" ");
				String city = splitKey[1];
				String country = splitKey[2];
			
				context.write(new Text(country), new Text(city + "/" + splitRecord[1]));
			}
}