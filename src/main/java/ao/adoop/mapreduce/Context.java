package ao.adoop.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Context {
	//Context class allows Mapper and Reducer to communicate with the main system. 
	//It allows them to write and (key:value) pair.
	//Each writing is directed to a certain name space. A key and value to different name spaces are
	//managed separately and the result mapping for the name space can be retrieved by getMapping method.
	private Map<String, Map<Object, ArrayList<Object>>> keyValMappingByBaseOutputPath = null;

	public Context() {
		 //Create a map to store keyValMappings by baseOutputPath
		 this.keyValMappingByBaseOutputPath = new HashMap<String, Map<Object, ArrayList<Object>>>();
		 //Create a keyValMapping (Map<Key, Values>) for the default output.
		 this.keyValMappingByBaseOutputPath.put("", new HashMap<Object, ArrayList<Object>>());
	}
	
	public void write(String key, String value) {
		this.addPairToMapping("", this.keyValMappingByBaseOutputPath.get(""), key, value);
	};
	
	private void addPairToMapping(String baseOutputPath, Map<Object, ArrayList<Object>> keyValMapping, Object key, Object value) {
		if (!keyValMapping.containsKey(key)) {
			//Create a new value list of the list for the key does not exist.
			ArrayList<Object> list = new ArrayList<Object>();
			list.add(value);
			keyValMapping.put(key, list);
			this.keyValMappingByBaseOutputPath.put(baseOutputPath, keyValMapping);
		}
		else {
			keyValMapping.get(key).add(value);
		};
	}

	protected void writeToBaseOutputPath(Object key, Object value, String baseOutputPath) {		
		Map<Object, ArrayList<Object>> keyValMapping = this.keyValMappingByBaseOutputPath.get(baseOutputPath);
		if (keyValMapping != null) {
			this.addPairToMapping(baseOutputPath, keyValMapping, key, value);
		}else {
			//Create a new key & value mapping if one for this baseOutputPath does not exit.
			keyValMapping = new HashMap<Object, ArrayList<Object>>();
			this.keyValMappingByBaseOutputPath.put(baseOutputPath, keyValMapping);
			this.addPairToMapping(baseOutputPath, keyValMapping, key, value);
		}
	};

	public void setNamedOutputs(String[] addedNamedOutputs) {

	}

	public Map<String, Map<Object, ArrayList<Object>>> getKeyValMappingsByBaseOutputPath() {
		return this.keyValMappingByBaseOutputPath;
	}
}
