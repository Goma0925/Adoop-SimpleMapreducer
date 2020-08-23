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

import exceptions.InvalidReducerException;
import javafx.util.Pair;
import settings.SystemPathSettings;

public class ReduceExecutor implements Runnable{
	protected ArrayList<File> inputFiles = null;
	protected Class<?> reducerClass = null;
	protected String workerId = null;
	protected SystemPathSettings systemPathSetting;
	
	public ReduceExecutor(String workerId, Class<?> reducerClass, SystemPathSettings systemPathSetting, ArrayList<File> inputFiles) throws InvalidReducerException {
		if (!Reducer.class.isAssignableFrom(reducerClass)) {
			throw new InvalidReducerException(reducerClass);
		}
		this.workerId = workerId;
		this.reducerClass = reducerClass;
		this.inputFiles = inputFiles;
		this.systemPathSetting = systemPathSetting;
	}

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
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
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
			};
			newInputLines.remove(0);//Remove the first element of the lines because it is a key.
			tempoInputLines.addAll(newInputLines);
		}
		return new Pair<String, ArrayList<String>>(key, tempoInputLines);
	};
	
	public Context runReduce(Pair<String, ArrayList<String>> keyAndValueList) throws InstantiationException, IllegalAccessException {
		Context resultContext = new Context();
		Reducer reducer = (Reducer) this.reducerClass.newInstance();
		reducer.reduce(keyAndValueList.getKey(), keyAndValueList.getValue(), resultContext);		
		System.out.println(this.workerId + ":Done processing:" + Integer.toString(keyAndValueList.getValue().size()));
		return resultContext;
	}
	
	
	private void writeToFiles(Context resultContext) throws IOException {
		//Write the results to multiple files. One key per one file.
		System.out.println(this.workerId + ":Writing to file..");
		SystemPathSettings pathSettings = this.systemPathSetting;
		for (Map.Entry<String, ArrayList<String>> entry : resultContext.getMap().entrySet()) {
	        String key = entry.getKey();
	        ArrayList<String> valueList = entry.getValue();
	        Path keyDir = Paths.get(pathSettings.reduceOutputBaseDir.toString() + "/"+ key);
	        if (!Files.exists(keyDir)){
	        	Files.createDirectory(keyDir);
	        }
	        File outputFile = new File(pathSettings.reduceOutputBaseDir.toString() + "/"+ key +"/[" + key + "]-" + this.workerId + pathSettings.reduceOutputFileExtension);
			FileWriter fr = new FileWriter(outputFile, true);
			BufferedWriter br = new BufferedWriter(fr);
			String stringBuffer = "";
			for (int i=0; i<valueList.size(); i++) {
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
	}
}
