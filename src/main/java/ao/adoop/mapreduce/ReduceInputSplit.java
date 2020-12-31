package ao.adoop.mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javafx.util.Pair;

public class ReduceInputSplit {
	private Path[] inputFiles = null;
	public ReduceInputSplit(Path[] inputFiles) {
		this.inputFiles = inputFiles;
	};
	
	public Pair<String, ArrayList<String>> getLines() throws IOException {
		String key = "";
		int numberOfFiles = this.inputFiles.length;
		ArrayList<String> tempoInputLines = new ArrayList<String>();
		for (int i=0; i<numberOfFiles; i++) {
			List<String> newInputLines = Files.readAllLines(this.inputFiles[i]);
			if (key == "") {
				key = newInputLines.get(0); //Get the first row that represents the key of the mapping outputs.
			};
			newInputLines.remove(0);//Remove the first element of the lines because it is a key.
			tempoInputLines.addAll(newInputLines);
		};
		return new Pair<String, ArrayList<String>>(key, tempoInputLines);
	}

}
