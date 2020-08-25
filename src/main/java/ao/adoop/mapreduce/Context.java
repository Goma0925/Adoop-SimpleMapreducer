package ao.adoop.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.naming.InvalidNameException;

public class Context {
	//Context class allows Mapper and Reducer to communicate with the main system. 
	//It allows them to write and (key:value) pair.
	//Each writing is directed to a certain name space. A key and value to different name spaces are
	//managed separately and the result mapping for the name space can be retrieved by getMapping method.
	private Map<String, ArrayList<String>> defaultKeyValMapping = new HashMap<String, ArrayList<String>>(); 
	private Map<String, Map<String, ArrayList<String>>> keyValMappingByNamedOutputs = null;
	private Map<String, String> baseOutputPathMappingByNamedOutputs = null;

	public void write(String key, String value) {
		this.addPairToMapping(this.defaultKeyValMapping, key, value);
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
	
	public Map<String, ArrayList<String>> getDefaultMapping(){
		return this.defaultKeyValMapping;
	};
	
	public Map<String, ArrayList<String>> getNamedMapping(String namedOutput){
		//Return the mapping associated with the defaultNamedOutput if no namedOutput is given. 
		return this.keyValMappingByNamedOutputs.get(namedOutput);
	}

	protected void writeToNamedOutput(String namedOutput, String key, String value, String baseOutputPath) throws InvalidNameException {
		//If no namedOutput is set.
		if (this.keyValMappingByNamedOutputs == null) {
			throw new InvalidNameException("Named output '" + namedOutput + "' is not set.");
		}
		
		Map<String, ArrayList<String>> keyValMapping = this.keyValMappingByNamedOutputs.get(namedOutput);;
		if (keyValMapping == null) {
			throw new InvalidNameException("Named output '" + namedOutput + "' is not set.");
		}else {
			this.addPairToMapping(keyValMapping, key, value);
		}
		this.baseOutputPathMappingByNamedOutputs.put(namedOutput, baseOutputPath);
	};
	
	public String getBaseOutputPath(String namedOutput){
		return this.baseOutputPathMappingByNamedOutputs.get(namedOutput);
	}

	public void setNamedOutputs(ArrayList<String> namedOutputs) {
		//This method sets namedOutputs to which a client can record a key & value pair, separately 
		//from the defaultKeyValMapping.
		this.baseOutputPathMappingByNamedOutputs = new HashMap<String, String>();
		this.keyValMappingByNamedOutputs = new HashMap<String, Map<String, ArrayList<String>>>();
		for (String namedOutput: namedOutputs) {
			this.baseOutputPathMappingByNamedOutputs.put(namedOutput, "");
			this.keyValMappingByNamedOutputs.put(namedOutput, new HashMap<String, ArrayList<String>>());
		}
	}
}
