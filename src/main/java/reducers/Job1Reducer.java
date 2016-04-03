package reducers;

import java.io.IOException;

import java.util.Hashtable;
import java.util.TreeSet;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer
		extends Reducer<Text,Text,Text,Text> {
	
		private Hashtable<String, Integer> userMap = new Hashtable<String, Integer>();
	
		
		private Text country = new Text();
		private Text data = new Text();
	
		public void reduce(Text key, Iterable<Text> values,
		                Context context
		                ) throws IOException, InterruptedException {

	
		String temp = null;
		String nation = null;
		String city = null;
		String neighbor = null;
		userMap.clear();
		
		
		for (Text val : values) {
			
			
			String value = val.toString();
			if(!value.equals("")){		
					if(value.startsWith("----")){
						//value from mapper 2 --> place
						temp = value.substring(4);
						String[] dat = temp.split(" ");
					//	String type_id = dat[0];
						nation = dat[1];
						city = dat[2];
						
						if(dat.length > 3){
							neighbor = dat[3];
						}
					//	String city = dat[2];
				
					}else if(value.startsWith("++++")){
						String owner = value.substring(4);
						userMap.put(owner, 1);
					}
			}
		}
	
		if(nation != null){
		
			String o;
			if(neighbor != null)
				o = userMap.size() + "/" + city + " " + neighbor;
			else
				o = userMap.size() + "/" + city;
		
			data.set(o);
			country.set(nation);
			

		
			context.write(country, data);
		}
	
	}
		
	
	
}