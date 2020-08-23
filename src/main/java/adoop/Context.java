package adoop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Context {
	String delimiter = null; //Delimiter to separate key & value pairs in the mapper output file.
	Map<String, ArrayList<String>> keyValMapping = new HashMap<String, ArrayList<String>>();
	
	public Context() {
		this.delimiter = ",";
	}
	
	public Context(String delimiter) {
		this.delimiter = delimiter;
	}

	public void write(String key, String value) {
		if (!this.keyValMapping.containsKey(key)) {
			ArrayList<String> list = new ArrayList<String>();
			list.add(value);
			this.keyValMapping.put(key, list);
		}
		else {
			this.keyValMapping.get(key).add(value);
		}
	};
	
	public Map<String, ArrayList<String>> getMap(){
		return this.keyValMapping;
	} 
}
