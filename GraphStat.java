import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class GraphStat {
public static class statMapper extends Mapper<Object, Text, Text, Text>{
		
	    // in this case, each value is one line of text
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      // split the line into words with white space
	      Node nodeInfo = new Node(value.toString());
	      if(nodeInfo.id.isEmpty()){ // bad input
	    	  return;
	      }
	      // output itself first
	      
	      context.write(new Text("graph"), new Text(String.format("%s  %d  %f", 
	    		  						nodeInfo.id, nodeInfo.outlinks.size(), nodeInfo.pr)));
	      
	    }
	 }
	  
	 public static class statReducer extends Reducer<Text,Text,Text,Text> {
		    // sort node by pr value in in creasing order
		    private class NodeComparator implements Comparator<Node>{
		    	public int compare(Node n1, Node n2){
		    		return new Double(n1.pr).compareTo(n2.pr);
		    	}
		    }
		    // sort node by pr in decreasing order
		    private class NodeComparatorDecrease implements Comparator<Node>{
		    	public int compare(Node n1, Node n2){
		    		return new Double(n2.pr).compareTo(n1.pr);
		    	}
		    }
		    
		    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		      Comparator<Node> nodeComparator = new NodeComparator();
		      int K = 10;
		      // only keep top k nodes with highest pr values
		      PriorityQueue<Node> pq = new PriorityQueue<Node>(K, nodeComparator);
		      int n = 0, minOut = Integer.MAX_VALUE, maxOut = 0, sumOut = 0;
		      
		      for (Text val : values) {
		        String nodeInfo = val.toString();
		        nodeInfo.trim();
		        String[] tmp = nodeInfo.split("\\s+");
		        // node id nOut pr
		        String id = tmp[0];
		        int nOut = Integer.parseInt(tmp[1]);
		        double pr = Double.parseDouble(tmp[2]);
		        
		        Node node = new Node(id, pr);
		        // add node to fix sized priority queue
		        if (pq.size() < K){
		        	pq.add(node);
		        }else{
		        	Node head = pq.peek();
		        	if (node.pr > head.pr){ // add only if bigger than min value 
		        		pq.poll(); // remove head
		        		pq.add(node); // add current node
		        	}
		        }
		        
		        
		        ++n;
		        sumOut += nOut;
		        minOut = Math.min(nOut, minOut);
		        maxOut = Math.max(nOut, maxOut);
		      }
		      double avgOut = sumOut/n;
		      // write stat: # of nodes, # of edges, (min, max, avg) of out-degree
		      String stat = String.format("# of nodes:%d, # of edges:%d, (min, max, avg) of out-degree:%d, %d, %f"
		    		  ,n, sumOut, minOut, maxOut, avgOut);
		      context.write(new Text("graph stat info:"), new Text(stat));
		      context.write(new Text("Top ten nodes of PageRank value:"), new Text(""));
		      // write top 10 rank values
		      
		      Node[] topKNodes = pq.toArray(new Node[0]);
		      Comparator<Node> nodeCompDe = new NodeComparatorDecrease();
		      Arrays.sort(topKNodes, nodeCompDe);
		      for(Node node:topKNodes){
		    	  context.write(new Text(node.id), new Text(String.format("%f", node.pr)));
		      }
		    }
	 }
}
