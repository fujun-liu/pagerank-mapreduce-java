

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class PageRankGoogle {
	public static double counterNorm = 1.0e10;
	public static enum PGCounters {RESIDUAL};
	public static double ConvergeThresh = 1.0e-5;
	
	public static class prMapper extends Mapper<Object, Text, Text, Text>{
		
		private ArrayList<String> allNodes = new ArrayList<String>();
		
		public void setup(Context context) throws IOException, FileNotFoundException {
	    	Path[] graphFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	    	if (graphFile == null || graphFile.length < 1){
	    		return;
	    	}
	    	for (Path path:graphFile){// only one pattern file case was tested
				
	    		BufferedReader fis = new BufferedReader(new FileReader(path.toString()));
				String line = fis.readLine(); // only one line
				line.trim();
				String[] tmp = line.split("\\s+");
				for (int i = 1; i < tmp.length; ++ i){
					 allNodes.add(tmp[i]);
				}
	    	}
	    }
		
	    // in this case, each value is one line of text
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      // split the line into words with white space
	      Node nodeInfo = new Node(value.toString());
	      if(nodeInfo.id.isEmpty()){ // bad input
	    	  return;
	      }
	      // output itself first
	      context.write(new Text(nodeInfo.id), new Text(nodeInfo.toString()));
	      int nout = nodeInfo.outlinks.size();
	      if (nout > 0){
	    	  // c
	    	  Text pr = new Text(String.format("%f", nodeInfo.pr/nout));
	    	  for (String targetlink:nodeInfo.outlinks){
	    		  context.write(new Text(targetlink), pr);
	    	  }
	      }else{ // handle dangling nodes
	    	  if (allNodes.size() > 0){
	    		  int numNodes = allNodes.size();
		    	  Text pr = new Text(String.format("%f", nodeInfo.pr/numNodes));
		    	  for (String targetlink:allNodes){
		    		  context.write(new Text(targetlink), pr);
		    	  }
	    	  }
	    	  
	      }
	    }
	  }
	  
	  // reduce class: do aggregate 
	  public static class prReducer extends Reducer<Text,Text,Text,Text> {
		 private int numNodes = 0;
			
		 public void setup(Context context) throws IOException, FileNotFoundException {
		    	Path[] graphFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		    	if (graphFile == null || graphFile.length < 1){
		    		return;
		    	}
		    	for (Path path:graphFile){// only one pattern file case was tested
					BufferedReader fis = new BufferedReader(new FileReader(path.toString()));
					String line = fis.readLine();
					line.trim();
					String[] nodeIDs = line.split("\\s+");
					numNodes = nodeIDs.length - 1; // first is not
		    	}
		 }
			
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	      double pr = (float) 0;
	      double oldPr = .0;
	      String nodestr = "";
	      for (Text val : values) {
	        String prInfo = val.toString();
	        if (prInfo.contains("pr:")){
	        	nodestr = prInfo;
	        	Node tmp = new Node(prInfo);
	        	oldPr = tmp.pr;
	        }else{
	        	pr += Double.parseDouble(prInfo);
	        }
	      }
	      Node nodeInfo = new Node(nodestr);
	      if (numNodes > 0){
	    	  nodeInfo.pr = 0.85*pr + 0.15/numNodes;
	      }else{
	    	  nodeInfo.pr = pr;
	      }
	      double diff = Math.abs(nodeInfo.pr - oldPr);
	      context.getCounter(PageRankGoogle.PGCounters.RESIDUAL).increment((long) (diff * PageRankGoogle.counterNorm));
	      
	      context.write(key, new Text(nodeInfo.toString()));
	    }
	  }
	  
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    
		    if (otherArgs.length < 3) {
		      System.err.println("Usage: pagerank <in> <out> [iterations]");
		      System.exit(2);
		    }
		    
		    // specify the input path and output path
		    int iterations = 1;
		    if (args.length == 4){
		    	iterations = Integer.parseInt(args[3]);
		    }
		    boolean checkConverge = false;
		    if (args.length == 5){
		    	String doConvergeTest = args[4];
		    	int convergeFlag = Integer.parseInt(doConvergeTest);
		    	checkConverge = convergeFlag > 0;
		    }
		    
		    //String inputPath = "input/prog2-sample-medium.txt";
		    String inputPath = otherArgs[1];
		    String outputPath = otherArgs[2];
		    String outputPathNodes = outputPath + "Nodes";
		    // handle dangle links
		    {
		    	Job job = new Job(conf, "page rank");
			    job.setJarByClass(PageRank.class);
			    job.setMapperClass(DangleGoogle.dangleGoogleMapper.class);
			    job.setReducerClass(DangleGoogle.dangleGoogleReducer.class);
			    job.setNumReduceTasks(1);
			    
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);
			    
			    FileInputFormat.addInputPath(job, new Path(inputPath));
			    FileOutputFormat.setOutputPath(job, new Path(outputPathNodes));
			    
			    job.waitForCompletion(true);
		    }
		    
		    for (int iter = 0; iter < iterations; ++ iter){
		    	
		    	Job job = new Job(conf, "page rank");
			    job.setJarByClass(PageRank.class);
			    job.setMapperClass(prMapper.class);
			    job.setReducerClass(prReducer.class);
			    
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);
			    
			    
		    	if (iter > 0){
		    		inputPath = outputPath;
		    		outputPath += iter;
		    	}
		    	FileInputFormat.addInputPath(job, new Path(inputPath));
			    FileOutputFormat.setOutputPath(job, new Path(outputPath));
			 // add distributed cache
			    DistributedCache.addCacheFile(new Path(outputPathNodes + "/part-r-00000").toUri(), job.getConfiguration());
			    
			    job.waitForCompletion(true);
			    if (iter > 0){
			    	double diffPG = ((double) job.getCounters().findCounter(PageRankGoogle.PGCounters.RESIDUAL).getValue()) / counterNorm;
			    	 // check if the program converged
				    if (diffPG < ConvergeThresh){
				    	break;
				    }
			    }
			    
		    }
		    
		    
		    String statinputPath = outputPath;
    		String statoutputPath = outputPath + "-stat";
		    {
		    	
		    	Job job = new Job(conf, "page rank stat");
			    job.setJarByClass(PageRank.class);
			    job.setMapperClass(GraphStat.statMapper.class);
			    job.setReducerClass(GraphStat.statReducer.class);
			    job.setNumReduceTasks(1); // only one reducer to achieve global information
			    
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);
			    
		    	FileInputFormat.addInputPath(job, new Path(statinputPath));
			    FileOutputFormat.setOutputPath(job, new Path(statoutputPath));
			    job.waitForCompletion(true);
		    }
		    System.exit(0);
		  }

}


