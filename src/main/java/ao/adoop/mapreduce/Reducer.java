package ao.adoop.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.InvalidNameException;

import ao.adoop.io.DataLoader;
import javafx.util.Pair;

public abstract class Reducer implements Runnable{
	protected String workerId = null;
	protected ArrayList<File> inputFiles = null;
	protected Configuration config = null;
	protected String[] addedNamedOutputs = null;

	public Reducer(String workerId, Configuration config, ArrayList<File> inputFiles) {
		this.workerId = workerId;
		this.inputFiles = inputFiles;
		this.config = config;
	}
	
	//This method is intended to be overwritten when the sub class reducer wants to use MutipleOutputs 
	// in order to write results to different output locations.
	protected void setup(Context context) {};

	public void run() {
		//Shuffle
		Pair<String, ArrayList<String>> keyAndValueList = null;
		try {
			keyAndValueList = this.runShuffle(this.inputFiles);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//Reduce
		Context resultContext = null;
		try {
			resultContext = this.runReduce(keyAndValueList);
		} catch (InvalidNameException e) {
			e.printStackTrace();
		}
		try {
			this.writeToFiles(resultContext);
		} catch (Exception e) {
			e.printStackTrace();
		}
	};

	public Pair<String, ArrayList<String>> runShuffle(ArrayList<File> inputFiles) throws IOException {
		DataLoader loader = new DataLoader();
		String key = "";
		int numberOfFiles = this.inputFiles.size();
		ArrayList<String> tempoInputLines = new ArrayList<String>(numberOfFiles);
		for (int i=0; i<numberOfFiles; i++) {
			ArrayList<String> newInputLines = loader.loadFile(inputFiles.get(i));
			if (key == "") {
				key = newInputLines.get(0); //Get the first row that represents the key of the mapping outputs.
			};
			newInputLines.remove(0);//Remove the first element of the lines because it is a key.
			tempoInputLines.addAll(newInputLines);
		};
		return new Pair<String, ArrayList<String>>(key, tempoInputLines);
	};
	
	public Context runReduce(Pair<String, ArrayList<String>> keyAndValueList) throws InvalidNameException  {
		Context resultContext = new Context();
		resultContext.setNamedOutputs(this.addedNamedOutputs);
		//Run the setup method
		this.setup(resultContext);
		this.reduce(keyAndValueList.getKey(), keyAndValueList.getValue(), resultContext);		
		return resultContext;
	}
	
	private void writeToFiles(Context resultContext) throws IOException {
		//Write the results to multiple files. One key per one file.
		Path reduceOutputBaseDir = this.config.getFinalOutputDir();
		
		//Write the default key & value mapping
		Map<String, Map<Object, ArrayList<Object>>> keyValMappingByBaseOutputPath = resultContext.getKeyValMappingsByBaseOutputPath(); 
		
		//Write key & value to each each baseOutputPath's 
		Path targetOutputDir = null;
		Map<Object, ArrayList<Object>> keyValMapping = null;
		for (String baseOutputPath: keyValMappingByBaseOutputPath.keySet()) {
			targetOutputDir =  Paths.get(reduceOutputBaseDir.toString(), baseOutputPath);
			keyValMapping = keyValMappingByBaseOutputPath.get(baseOutputPath);
			if (keyValMapping != null) {
				this.writeEachMapping(targetOutputDir, keyValMapping);
			};
		};
		
	};
	
	private void writeEachMapping(Path baseBufferOutputDir, Map<Object, ArrayList<Object>> keyValMapping) throws IOException {
		//Write only when the mapping is not empty
		if (!keyValMapping.isEmpty()) {
			//Create the output directory if it doesn't exits.
			if (!baseBufferOutputDir.toFile().exists()) {
				baseBufferOutputDir.toFile().mkdirs();
			}
			int keyCount = 0;
			int keyNum = keyValMapping.keySet().size();
	        File outputFile = new File(baseBufferOutputDir.toString(),this.config.generateReduceOutputFileName(this.workerId));
			FileWriter fr = new FileWriter(outputFile, true);
			String stringBuffer = "";
			BufferedWriter br = new BufferedWriter(fr);
			
			for (Entry<Object, ArrayList<Object>> entry : keyValMapping.entrySet()) {
		        Object key = entry.getKey();
		        ArrayList<Object> valueList = entry.getValue();
				int valueListSize = valueList.size();
				
				//Add all the key & value pairs in the string buffer
				for (int i=0; i<valueListSize; i++) {
					stringBuffer += key.toString() + "," + valueList.get(i).toString();
					if (i != (valueListSize-1)) {
						//If the value is not the last element of the key, add a line break.
						stringBuffer += "\n";	
					};
				}
				keyCount += 1;
				if (keyCount != keyNum) {
					//If it is not the last key, add a line break to continue other sets of key & value pairs.
					stringBuffer += "\n";
				};
				br.write(stringBuffer);
				stringBuffer = "";
		    }
			br.close();
			fr.close();
		}
	};

	public abstract void reduce(String key, ArrayList<String> values, Context context) throws InvalidNameException;
}
