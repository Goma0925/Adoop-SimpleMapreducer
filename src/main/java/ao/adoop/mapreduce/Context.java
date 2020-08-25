package ao.adoop.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Context {
	//Context class allows Mapper and Reducer to communicate with the main system. 
	//It allows them to write and (key:value) pair.
	//Each writing is directed to a certain name space. A key and value to different name spaces are
	//managed separately and the result mapping for the name space can be retrieved by getMapping method.
	private String defaultNameSpace = null;
	private Map<String, Map<String, ArrayList<String>>> keyValMappingByNamespace = new HashMap<String, Map<String, ArrayList<String>>> ();
	private Map<String, String> baseOutputPathMappingByNamespace = new HashMap<String, String>();
	public Context(String defaultNameSpace) {
		this.defaultNameSpace = defaultNameSpace;
		keyValMappingByNamespace.put(defaultNameSpace, new HashMap<String, ArrayList<String>>());
	}

	public void write(String key, String value) {
		this.addPairToMapping(this.keyValMappingByNamespace.get(this.defaultNameSpace), key, value);
	};
	
	private void addPairToMapping(Map<String, ArrayList<String>> keyValMapping, String key, String value) {
		if (!keyValMapping.containsKey(key)) {
			ArrayList<String> list = new ArrayList<String>();
			list.add(value);
			keyValMapping.put(key, list);
		}
		else {
			keyValMapping.get(key).add(value);
		};
	}
	
	public Map<String, ArrayList<String>> getMapping(String nameSpace){
		return this.keyValMappingByNamespace.get(nameSpace);
	};
	
	public Map<String, ArrayList<String>> getMapping(){
		//Return the mapping associated with the defaultNameSpace if no nameSpace is given. 
		return this.keyValMappingByNamespace.get(this.defaultNameSpace);
	}

	protected void writeToNameSpace(String nameSpace, String key, String value, String baseOutputPath) {
		Map<String, ArrayList<String>> keyValMapping = this.keyValMappingByNamespace.get(nameSpace);;
		if (keyValMapping != null) {
			this.addPairToMapping(keyValMapping, key, value);
		}else {
			keyValMapping = new HashMap<String, ArrayList<String>>();
			this.addPairToMapping(keyValMapping, key, value);
			this.keyValMappingByNamespace.put(nameSpace, keyValMapping);
		}
		this.baseOutputPathMappingByNamespace.put(nameSpace, baseOutputPath);
	};
	
	public String getBaseOutputPath(String nameSpace){
		return this.baseOutputPathMappingByNamespace.get(nameSpace);
	}
}
