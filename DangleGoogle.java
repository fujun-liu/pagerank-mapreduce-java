
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class DangleGoogle {
	public static class dangleGoogleMapper extends Mapper<Object, Text, Text, Text>{
		// the mapper does nothing
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
			Node nodeInfo = new Node(value.toString());
			if(nodeInfo.id.isEmpty()){ // bad input
				return;
			}
		   // output itself first
		   context.write(new Text("graph"), new Text(nodeInfo.id + " " + nodeInfo.strLinks()));
		}
		
	}
	
	public static class dangleGoogleReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
			
			String allNodes = "";
			for (Text val:values){
				String tmp = val.toString();
				String[] strs = tmp.split("\\s+");
				String id = strs[0]; // the first is node, remaining outgoing edges
				allNodes += id + "  ";
			}
			context.write(new Text("GraphNodes:"), new Text(allNodes));
		}
	}
}
