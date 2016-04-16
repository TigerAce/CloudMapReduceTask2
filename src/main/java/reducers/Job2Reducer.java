package reducers;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	//private Text value = new Text();
	
	private final int CITY_LIMIT = 10;
	
	private Hashtable<String, Integer> cityTable = new Hashtable<String, Integer>();
	
	private Hashtable<String, Entry<String, Integer>> neighborTable = new Hashtable<String, Entry<String, Integer>>();
	
	/**
	 * Generic function to sort a map by its values
	 * @param map
	 * @return
	 */
	static <K,V extends Comparable<? super V>> 
    List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
		
		Collections.sort(sortedEntries, 
		    new Comparator<Entry<K,V>>() {
		        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
		            return e2.getValue().compareTo(e1.getValue());
		        }
		    }
		);
	
		return sortedEntries;
	}
	
	
	/**
	 * reduce function
	 */	
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {


		cityTable.clear();
		neighborTable.clear();
		
		for(Text t : values){
			String currVal = t.toString();
		
			String[] splitVal = currVal.split("/");
			
			int userCount = Integer.parseInt(splitVal[0]);
			
			/**
			 * 
			 */
			if(userCount != 0){
				String[] splitLocality = splitVal[1].split(" ");
				String city = splitLocality[0];
				
				String neighbor = null;
				if(splitLocality.length > 1){
					neighbor = splitLocality[1];
					
					
					//assign neighbor to its relevant city
					if(neighborTable.containsKey(city)){
						Entry<String, Integer> e = neighborTable.get(city);
						if(e != null){
							if(e.getKey().equals(neighbor)){
								e.setValue(e.getValue() + userCount);
							}else{
								if(e.getValue() < userCount){
									neighborTable.put(city, new AbstractMap.SimpleEntry<String, Integer>(neighbor, userCount));
								}
							}
						}
					}else{
						neighborTable.put(city, new AbstractMap.SimpleEntry<String, Integer>(neighbor, userCount));
					}
				}
				
				//add user count to the same city
				if(cityTable.containsKey(city)){
					cityTable.put(city, cityTable.get(city) + userCount);
				}else{
					cityTable.put(city, userCount);
				}
				
				
			}
			
		
		}
		
	
		//countryName\t{(localityName:numOfUsers, neighborhoodName :numOfUsers)}+
		
		/**
		 * sort cityTable by values to get top 10 city for each country
		 */
		
		List<Entry<String, Integer>> sortedCity = entriesSortedByValues(this.cityTable);
		if(sortedCity.size() != 0){
				int counter = 0;
				
				if(sortedCity.size() < this.CITY_LIMIT){
					counter = sortedCity.size();
				}else{
					counter = this.CITY_LIMIT;
				}
				
				String o = null;
				for(int i = 0; i < counter; i++){
					Entry<String, Integer> currCity = sortedCity.get(i);
					Entry<String, Integer> currNeighbor = neighborTable.get(currCity.getKey());
			
					String currRes;
					if(currNeighbor != null){
						currRes = "(" + currCity.getKey().replace("+", " ") + ":" + currCity.getValue() + ", "
						+ currNeighbor.getKey() + ":" + currNeighbor.getValue() + ")";
					}else{
						currRes = "(" + currCity.getKey().replace("+", " ") + ":" + currCity.getValue() + ")";
					}
					
					
					if(o == null){
						o = currRes;
					}else{
						o += currRes;
					}
				}
				
	
				Text value = new Text();
				value.set(o);
				context.write(key, value);
			}
	}
	
	
}
	