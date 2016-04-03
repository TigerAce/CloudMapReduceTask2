package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * this Mapper is for processing all photo files
 * INPUT = (photo.txt ... )
 * OUTPUT = (place-id, owner)
 */
public class Job1Mapper1
			extends Mapper<Object, Text, Text, Text>{
			

			private Text place_id = new Text();
			private Text owner = new Text();
			
			
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and owner
				String[] split = value.toString().split("\t");
				
				place_id.set(split[4]);
				owner.set("++++" + split[1]);
				context.write(place_id, owner);

			}
}