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

import javax.naming.InvalidNameException;

import ao.adoop.io.DataLoader;
import javafx.util.Pair;

public abstract class Reducer implements Runnable{
	protected String workerId = null;
	protected ArrayList<File> inputFiles = null;
	protected Configuration config = null;
	protected String[] addedNamedOutputs = null;
	protected String assignedKey = null;

	public Reducer(String workerId, Configuration config, ArrayList<File> inputFiles, String[] addedNamedOutputs) {
		this.workerId = workerId;
		this.inputFiles = inputFiles;
		this.config = config;
		this.addedNamedOutputs = addedNamedOutputs;
	}
	
	//This method is intended to be overwritten when the sub class reducer wants to use MutipleOutputs 
	// in order to write results to different output locations.
	protected void setup(Context context) {};

	public void run() {
		System.out.println(this.workerId + ":Running Reducing Process...");
		System.out.println(this.workerId + ":Loading files...");
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
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvalidNameException e) {
			e.printStackTrace();
		}
		try {
			this.writeToFiles(resultContext);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(this.workerId + ":Loaded files for the key:"+keyAndValueList.getKey());
	};

	public Pair<String, ArrayList<String>> runShuffle(ArrayList<File> inputFiles) throws IOException {
		DataLoader loader = new DataLoader();
		String key = "";
		ArrayList<String> tempoInputLines = new ArrayList<String>();
		int numberOfFiles = this.inputFiles.size();
		for (int i=0; i<numberOfFiles; i++) {
			ArrayList<String> newInputLines = loader.loadFile(inputFiles.get(i));
			if (key == "") {
				key = newInputLines.get(0); //Get the first row that represents the key of the mapping outputs.
				this.assignedKey = key;
			};
			newInputLines.remove(0);//Remove the first element of the lines because it is a key.
			tempoInputLines.addAll(newInputLines);
		}
		return new Pair<String, ArrayList<String>>(key, tempoInputLines);
	};
	
	public Context runReduce(Pair<String, ArrayList<String>> keyAndValueList) throws InstantiationException, IllegalAccessException, InvalidNameException {
		Context resultContext = new Context();
		resultContext.setNamedOutputs(this.addedNamedOutputs);
		//Run the setup method
		this.setup(resultContext);
		this.reduce(keyAndValueList.getKey(), keyAndValueList.getValue(), resultContext);		
		System.out.println(this.workerId + ":Done processing:" + Integer.toString(keyAndValueList.getValue().size()));
		return resultContext;
	}
	
	private void writeToFiles(Context resultContext) throws IOException {
		//Write the results to multiple files. One key per one file.
		Path reduceOutputBaseDir = this.config.finalOutputDir;
		String reduceOutputFileExtension = this.config.reduceOutputFileExtension;
        Path outputFile = Paths.get(reduceOutputBaseDir.toString(), "part-r-" + this.workerId + reduceOutputFileExtension);
		System.out.println(this.workerId + ":Writing to file:" +  outputFile.toString());
        FileWriter fr = new FileWriter(outputFile.toFile(), true);
		BufferedWriter br = new BufferedWriter(fr);
		
		//Write the default key & value mapping
		Map<String, ArrayList<String>> keyValMapping = resultContext.getDefaultMapping(); 
		String stringBuffer = "";
		ArrayList<String> valueList = null;
		int valueListSize = null;
		for (String key: keyValMapping.keySet()) {
			valueListSize = valueList.size();
			valueList = keyValMapping.get(key);
			for (int i=0; i<valueListSize; i++) {
				stringBuffer = key + "," + valueList.get(i);
				if (i != (valueList.size()-1)) {
					//If not the last element, add a line break
					stringBuffer += "\n";
				}
				br.write(stringBuffer);
			}			
		}
		br.close();
		fr.close();
		//Write each namedOutput's key & value mappings
//		for (String namedOutput: this.addedNamedOutputs) {
//			Path baseBufferOutputDir = Paths.get(this.config.namedReduceOutputBufferDir.toString(), namedOutput);
//			String baseFinalOutputPath = resultContext.getBaseOutputPath(namedOutput);
//			keyValMapping = resultContext.getNamedMapping(namedOutput);
//			if (keyValMapping != null) {
//				this.writeEachMapping(baseBufferOutputDir, keyValMapping, reduceOutputFileExtension);
//				this.writeFinalOutputBasePath(baseBufferOutputDir, namedOutput, baseFinalOutputPath);
//			}
//		};
		
	};
	
	
	
	private void writeEachMapping(Path baseBufferOutputDir, Map<String, ArrayList<String>> keyValMapping, String reduceOutputFileExtension) throws IOException {
		for (Map.Entry<String, ArrayList<String>> entry : keyValMapping.entrySet()) {
	        String key = entry.getKey();
	        ArrayList<String> valueList = entry.getValue();
	        if (!Files.exists(baseBufferOutputDir)){
	        	baseBufferOutputDir.toFile().mkdirs();;
	        }
	        Path outputFile = Paths.get(baseBufferOutputDir.toString(), "part-r-" + this.workerId + reduceOutputFileExtension);
			System.out.println(this.workerId + ":Writing to file..");
	        FileWriter fr = new FileWriter(outputFile.toFile(), true);
			BufferedWriter br = new BufferedWriter(fr);
			String stringBuffer = "";
			int valueListSize = valueList.size();
			for (int i=0; i<valueListSize; i++) {
				stringBuffer = key + "," + valueList.get(i);
				if (i != (valueList.size()-1)) {
					//If not the last element, add a line break
					stringBuffer += "\n";
				}
				br.write(stringBuffer);
			}
			br.close();
			fr.close();
	    }
	};
	
	private void writeFinalOutputBasePath(Path baseBufferOutputDir, String namedOutput, String baseFinalOutputDir) throws IOException {
		//This method creates a new file named "baseOutputDir.txt" in the specified directory
		//and record a baseOutputDir for each namedOutput's buffer, where the final output will be.
		if (!baseFinalOutputDir.equals("")) {
			File finalOutputBasePathFile = Paths.get(baseBufferOutputDir.toString(), this.assignedKey, "baseOutputDir.txt").toFile();
			FileWriter fr = new FileWriter(finalOutputBasePathFile, true);
			BufferedWriter br = new BufferedWriter(fr);
			br.write(baseFinalOutputDir);
			br.close();
			fr.close();	
		}
	}
	

	public abstract void reduce(String key, ArrayList<String> values, Context context) throws InvalidNameException;
}
