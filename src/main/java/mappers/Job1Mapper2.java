package mappers;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * this Mapper is used to process place file
 * 
 * INPUT = (place.txt)
 * OUTPUT = (place-id, place-type-id + place-url)
 */
public class Job1Mapper2
			extends Mapper<Object, Text, Text, Text>{
			
	
			private Text place_id = new Text();
			private Text locality = new Text();
			
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
			
			
				/**
				 * place-id \t woeid \t latitude \t longitude \t place-name \t place-type-id \t place-url
				 */		
				
				
		
				String[] splitRecord = value.toString().split("\t");
				
				place_id.set(splitRecord[0]);
				
				//filter: only accept place-type-id = {7,9,22}
				if(splitRecord[5].equals("7") || splitRecord[5].equals("9")){
					
					//the record is locality level
					String placeURL = splitRecord[6];
					String[] splitURL = placeURL.split("/");
					//System.out.println(splitURL[0]);
					String country = splitURL[splitURL.length - 3];
					if(country.equals("")){
						country = splitURL[splitURL.length - 2];
					}
					String city = splitURL[splitURL.length - 1];
					String l = splitRecord[5] + " " + country + " " + city;		
					locality.set("----" + l);
					
			
					context.write(place_id, locality);
				
					
				}else if(splitRecord[5].equals("22")){
					//or neighborhood
					String placeURL = splitRecord[6];
					String[] splitURL = placeURL.split("/");
					String country = splitURL[splitURL.length - 4];
					if(country.equals("")){
						country = splitURL[splitURL.length - 3];
					}
					String city = splitURL[splitURL.length - 2];
					String neighbor = splitURL[splitURL.length - 1];
					String l = splitRecord[5] + " " + country + " " + city + " " +  neighbor;		
					locality.set("----" + l);
					
			
					context.write(place_id, locality);
				}
					

			}
}