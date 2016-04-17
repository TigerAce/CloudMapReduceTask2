package reducers;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer
		extends Reducer<Text,Text,Text,Text> {
	
		private HashSet<String> hs = new HashSet<String>();

		public void reduce(Text key, Iterable<Text> values,
		                Context context
		                ) throws IOException, InterruptedException {


		hs.clear();

		for (Text val : values) {
			String neighbor = val.toString();
			if(!neighbor.equals("NULL")){
				
				String splitNeighbor[] = neighbor.split(" ");
				for(int i = 0; i < splitNeighbor.length; i++){
					hs.add(splitNeighbor[i]);
				}
			}
		
		}
		
		String out = null;
		for(java.util.Iterator<String> iter = hs.iterator(); iter.hasNext();){
			String curr = iter.next();
			if(out == null){
				out = curr;
			}
			else{
				out += " " + curr;
			}
		}
		if(out != null)
			context.write(key, new Text(out));
		else
			context.write(key, new Text("NULL"));
	
											
	}
		
	
	
}