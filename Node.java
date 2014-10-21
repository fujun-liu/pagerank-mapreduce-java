import java.util.ArrayList;

public class Node{
	public String id = "";
	public double pr = 1; // pr value init to be 1
	public ArrayList<String> outlinks = new ArrayList<String>(); // its neighbors
	
	public Node(String id, double pr){
		this.id = id;
		this.pr = pr;
	}
	public Node(String value){
		value = value.trim();
		if (value.isEmpty()){
			return;
		}
		String[] nstr = value.split("\\s+"); // split into strings
		if (nstr.length == 0){
			return;
		}
		
		this.id = nstr[0]; // first one is node itself
		if (nstr.length >= 2){
			int outStartIndex = 1;
			if (nstr[1].startsWith("pr:")){// starting from second iteration, pr value will exist
				pr = Float.parseFloat(nstr[1].substring(3));
				outStartIndex = 2;
			}
			for (; outStartIndex < nstr.length; ++ outStartIndex){
				outlinks.add(nstr[outStartIndex]);
			}
		}
	}
	
	public String toString(){
		String ret = "pr:" + String.format("%f", pr);
		for (String link:outlinks){
			ret += "	" + link;
		}
		return ret;
	}
	
	public String strLinks(){
		String ret = "";
		for (String link:outlinks){
			ret += link + " ";
		}
		return ret;
	}
	
} 