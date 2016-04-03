package mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
			private Text country = new Text();
			private Text info = new Text();

			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {

				String[] splitRecord = value.toString().split("\t");
				country.set(splitRecord[0]);
				info.set(splitRecord[1]);
				
				context.write(country, info);
			}
}