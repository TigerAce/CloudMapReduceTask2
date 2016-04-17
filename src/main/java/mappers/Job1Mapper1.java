package mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1Mapper1
			extends Mapper<Object, Text, Text, Text>{
			

			private Hashtable<String, String> ht = new Hashtable<String,String>();

			protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.setup(context);
						try{
					Configuration conf = context.getConfiguration();
					
		            Path pt=new Path(conf.get("place"));
		            FileSystem fs = FileSystem.get(new Configuration());
		            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		            String line;
		            line=br.readLine();
		            while (line != null){
		            	//place-id \t woeid \t latitude \t longitude \t place-name \t place-type-id \t place-url
		            	
		            	/**
		            	 * TODO: note that some neighbor's name under different city may be identical.
		            	 */
		                    String[] splitRecord = line.split("\t");
		        			if(splitRecord[5].equals("7")){
		    					
		    					//the record is locality level
		    					String placeURL = splitRecord[6];
		    					String[] splitURL = placeURL.split("/");
		    					//System.out.println(splitURL[0]);
		    					String country = splitURL[splitURL.length - 3];
		    					if(country.equals("")){
		    						country = splitURL[splitURL.length - 2];
		    					}
		    					String city = splitURL[splitURL.length - 1];
		    					String l = country + " " + city;		
		    				//	locality.set("----" + l);
		    					
		    					ht.put(splitRecord[0], l);
		    			//		context.write(place_id, locality);
		    				
		    					
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
		    					String l = country + " " + city + " " +  neighbor;		
		    					//locality.set("----" + l);
		    					ht.put(splitRecord[0], l);
		    			
		    					//context.write(place_id, locality);
		    				}

		                   // ht.put(splitLine[0], splitLine[6]);
		                    line=br.readLine();
		            }
		            }catch(Exception e){
		            }
			
			}
			
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and owner
				String[] split = value.toString().split("\t");
				
				String placeId = split[4];
				
				if(ht.containsKey(placeId)){
					String splitLocality[] = ht.get(placeId).split(" ");
					String country = splitLocality[0];
					String city = splitLocality[1];
					String neighbor = null;
					String owner = split[1];
					if(splitLocality.length > 2){
						neighbor = splitLocality[2];
					}
					
					if(neighbor != null){
						context.write(new Text(owner + " " + city + " " + country), new Text(neighbor));
					}else{
						context.write(new Text(owner + " " + city + " " + country), new Text("NULL"));
					}
					
					
				}
//				place_id.set(split[4]);
//				owner.set("++++" + split[1]);
//				context.write(place_id, owner);

			}
}