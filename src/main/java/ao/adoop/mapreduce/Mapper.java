package ao.adoop.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import ao.adoop.io.DataLoader;

public abstract class Mapper implements Runnable {
	protected String workerId = null;
	protected int startIndex = 0;
	protected int endIndex = 0;
	protected File inputFile = null;
	protected Configuration config = null;
	protected String[] addedNamedOutputs = null;
	
	public Mapper(String workerId, Configuration config, File inputFile, Integer startIndex, Integer endIndex, String[] addedNamedOutputs) {
		this.workerId = workerId;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.inputFile = inputFile;
		this.config = config;
		this.addedNamedOutputs = addedNamedOutputs;
	};
	
	//This method is intended to be overwritten when the sub class mapper wants to use MutipleOutputs 
	// in order to write results to different output locations.
	protected void setup(Context context) {};

	public void run() {
		//Run mapping
		Context resultContext = null;
		try {
			resultContext = this.runMap();
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//Write results to file
		try {
			this.writeToFiles(resultContext);
		} catch (IOException e) {
			e.printStackTrace();
		}
	};
	
	public Context runMap() throws InstantiationException, IllegalAccessException, IOException {		
		Context tempoContext = new Context();
		DataLoader loader = new DataLoader();
		String[] inputLines = null;
		int chunkStartIndex = this.startIndex;
		//Run the setup method
		tempoContext.setNamedOutputs(this.addedNamedOutputs);
		this.setup(tempoContext);
		//Read the input file
		inputLines = loader.loadChunkByLineIndices(inputFile, startIndex, endIndex);
		//Map process
		for (int i=0; i<inputLines.length; i++) {
			this.map(Integer.toString(chunkStartIndex), inputLines[i], tempoContext);
		}
		return tempoContext;
	};
	
	private void writeToFiles(Context resultContext) throws IOException {
		//Write the results to files. Each key's associated values will be written in different files.
		
		//Get the key & value mappings organized by the baseOutputPath string.
		Map<String, Map<Object, ArrayList<Object>>> keyValMappingByBaseOutputPath = resultContext.getKeyValMappingsByBaseOutputPath(); 
		//Write key & value to each baseOutputPath's 
		Path targetOutputDir = null;
		Map<Object, ArrayList<Object>> keyValMapping = null;
		Path finalOutputBaseDir = this.config.finalOutputDir;
		for (String baseOutputPath: keyValMappingByBaseOutputPath.keySet()) {
			if (baseOutputPath.equals("")) {
				//Regular output goes into the mapper output buffer directory.
				targetOutputDir =  this.config.mapOutputBufferDir;
			}else {
				//Multiple output to a particular outputPath which was 
				//specified by user goes into the final output directory. 
				targetOutputDir = Paths.get(finalOutputBaseDir.toString(), baseOutputPath);
			}
			keyValMapping = keyValMappingByBaseOutputPath.get(baseOutputPath);
			this.writeEachMapping(targetOutputDir, keyValMapping);
		};
	};
	
	private void writeEachMapping(Path baseBufferOutputDir, Map<Object, ArrayList<Object>> keyValMapping) throws IOException {
		String key;
		Path keyDir;
		ArrayList<Object> valueList;
		for (Entry<Object, ArrayList<Object>> entry : keyValMapping.entrySet()) {
	        key = entry.getKey().toString();
	        valueList = entry.getValue();
	        keyDir = Paths.get(config.mapOutputBufferDir.toString(), key);
	        if (!Files.exists(keyDir)){
	        	keyDir.toFile().mkdir();
	        }
	        File outputFile = new File(keyDir.toFile(), config.getMapOutputFileName(this.workerId));
	        FileWriter fr = new FileWriter(outputFile, true);
			BufferedWriter br = new BufferedWriter(fr);
			br.write(key);
			for (Object value: valueList) {
				br.write("\n"+value.toString());;
			}
			br.close();
			fr.close();
	    };

	}

	public abstract void map(String key, String value, Context context);
}
