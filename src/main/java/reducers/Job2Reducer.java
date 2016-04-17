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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	//private Text value = new Text();
	
	private final int CITY_LIMIT = 10;
	
	private Hashtable<String, Integer> cityTable = new Hashtable<String, Integer>();
	private Hashtable<String, Hashtable<String, Integer>> neighborTable = new Hashtable<String, Hashtable<String, Integer>>();
	
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
			String[] splitVal = t.toString().split("/");
			String city = splitVal[0];
			String[] neighbors = splitVal[1].split(" ");
			
			
			//add user count to the same city
		
			if(cityTable.containsKey(city)){
				cityTable.put(city, cityTable.get(city) + 1);
			}else{
				cityTable.put(city, 1);
			}
			
			//add neighbors
			if(!neighbors[0].equals("NULL")){
				for(int i = 0; i < neighbors.length; i++){
					if(neighborTable.containsKey(city)){
						Hashtable<String, Integer> nt = neighborTable.get(city);
						if(nt.containsKey(neighbors[i])){
							nt.put(neighbors[i],nt.get(neighbors[i]) + 1);
						}else{
							nt.put(neighbors[i], 1);
						}
						
					}else{
						Hashtable<String, Integer> nt = new Hashtable<String, Integer>();
						nt.put(neighbors[i], 1);
						neighborTable.put(city, nt);
					}
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
					Hashtable<String, Integer> neighbors = neighborTable.get(currCity.getKey());
					
					
					String currRes = "";
					
					if(neighbors != null){
						List<Entry<String, Integer>> sortedNeighbor = entriesSortedByValues(neighbors);
						
						if(sortedNeighbor.size() != 0){
							Entry<String, Integer> currNeighbor = sortedNeighbor.get(0);
							currRes = "(" + currCity.getKey().replace("+", " ") + ":" + currCity.getValue() + ", "
									+ currNeighbor.getKey() + ":" + currNeighbor.getValue() + ")";
						}
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
	