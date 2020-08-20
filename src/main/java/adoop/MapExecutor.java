package adoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

import exceptions.InvalidMapperException;
import settings.SystemPathSettings;

public class MapExecutor implements Runnable {
	//This class is a worker that handles the mapping phase.
	protected int startIndex = 0;
	protected int endIndex = 0;
	protected File inputFile = null;
	protected Class<?> mapperClass = null;
	protected String workerId = null;
	protected Context resultContext = null;   
	public MapExecutor(String workerId, Class<?> mapperClass, File inputFile, int startIndex, int endIndex) throws InvalidMapperException {
		if (!Mapper.class.isAssignableFrom(mapperClass)) {
			throw new InvalidMapperException(mapperClass);
		}
		this.workerId = workerId;
		this.mapperClass = mapperClass;
		this.inputFile = inputFile;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		System.out.println("Worker("+this.workerId+"):"+Integer.toString(startIndex) + "/" + Integer.toString(endIndex));
	}

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
		Mapper mapper = (Mapper) this.mapperClass.newInstance();
		System.out.println(this.workerId + ":Loading a chunk...");
		//Read the input file
		inputLines = loader.loadChunkByLineIndices(inputFile, startIndex, endIndex);
		System.out.println(this.workerId + ":Loaded a chunk of size:" + Integer.toString(inputLines.length));
		System.out.println(this.workerId + ":Last index:" + inputLines[inputLines.length-1]);
		//Map process
		for (int i=0; i<inputLines.length; i++) {
			mapper.map("ID="+this.workerId+"|"+Integer.toString(i), inputLines[i], tempoContext);
		}
		System.out.println(this.workerId + ":Done processing:" + Integer.toString(inputLines.length));
		this.resultContext  = tempoContext;
	}
	
	private void writeToFiles() throws IOException {
		//Write the results to files. Each key's associated values will be written in different files.
		System.out.println(this.workerId + ":Writing to file..");
		String key;
		Path keyDir;
		ArrayList<String> valueList;
		for (Map.Entry<String, ArrayList<String>> entry : this.resultContext.getMap().entrySet()) {
	        key = entry.getKey();
	        valueList = entry.getValue();
	        keyDir = Paths.get(SystemPathSettings.mapOutputBaseDir.toString() + "/"+ key);
	        System.out.println("Target map output dir:" + keyDir.toString() +"|| FILE_EXiSTS=" +  Boolean.toString(Files.exists(keyDir)));
	        if (!Files.exists(keyDir)){
	        	keyDir.toFile().mkdir();
		        System.out.println("Creating map output dir:" + keyDir.toString());
	        }
	        File outputFile = new File(keyDir.toFile(), SystemPathSettings.getMapOutputFileName(key, this.workerId));
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
}

