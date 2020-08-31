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

import ao.adoop.io.DataLoader;

public abstract class Mapper implements Runnable {
	protected String workerId = null;
	protected int startIndex = 0;
	protected int endIndex = 0;
	protected File inputFile = null;
	protected Context resultContext = null; 
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
		try {
			this.runMap();
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//Write results to file
		try {
			this.writeToFiles();
		} catch (IOException e) {
			e.printStackTrace();
		}
	};
	
	public void runMap() throws InstantiationException, IllegalAccessException, IOException {		
		System.out.println(this.workerId + ":Running process...");
		Context tempoContext = new Context();
		DataLoader loader = new DataLoader();
		String[] inputLines = null;
		int chunkStartIndex = this.startIndex;
		System.out.println(this.workerId + ":Loading a chunk["+Integer.toString(startIndex) + ","+Integer.toString(endIndex)+"]...");
		//Run the setup method
		tempoContext.setNamedOutputs(this.addedNamedOutputs);
		this.setup(tempoContext);
		//Read the input file
		inputLines = loader.loadChunkByLineIndices(inputFile, startIndex, endIndex);
		System.out.println(this.workerId + ":Done loading a chunk of size:" + Integer.toString(inputLines.length));
		//Map process
		for (int i=0; i<inputLines.length; i++) {
			this.map(Integer.toString(chunkStartIndex), inputLines[i], tempoContext);
		}
		System.out.println(this.workerId + ":Done processing:" + Integer.toString(inputLines.length));
		this.resultContext  = tempoContext;
	};
	
	private void writeToFiles() throws IOException {
		//Write the results to files. Each key's associated values will be written in different files.
		System.out.println(this.workerId + ":Writing to file.. :" + this.config.mapOutputBufferDir.toString());
		String key;
		Path keyDir;
		ArrayList<String> valueList;
		Configuration config = this.config;
		for (Map.Entry<String, ArrayList<String>> entry : this.resultContext.getDefaultMapping().entrySet()) {
	        key = entry.getKey();
	        valueList = entry.getValue();
	        keyDir = Paths.get(config.mapOutputBufferDir.toString() + "/"+ key);
	        if (!Files.exists(keyDir)){
	        	keyDir.toFile().mkdir();
	        }
	        File outputFile = new File(keyDir.toFile(), config.getMapOutputFileName(key, this.workerId));
			FileWriter fr = new FileWriter(outputFile, true);
			BufferedWriter br = new BufferedWriter(fr);
			br.write(key);
			for (String value: valueList) {
				br.write("\n"+value);;
			}
			br.close();
			fr.close();
	    };
	    
	}

	public abstract void map(String key, String value, Context context);
}
