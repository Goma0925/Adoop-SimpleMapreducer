package ao.adoop.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputSplit {
	private int startIndex = 0;
	private int endIndex = -1;
	private Path inputFile = null;
	public InputSplit(Path inputFile, int startIndex, int endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.inputFile = inputFile;
	};
	
	public InputSplit(Path inputFile) {
		//Use this constructor to assign the entire file to a inputSplit.
		this.startIndex = -1;
		this.endIndex = -1;
		this.inputFile = inputFile;
	};
	
	public String[] getLines() throws IOException {
		//This function loads a particular chunk in a file based on the line start and the end indices.
		//return| String[] loadedLines: An array containing all the lines of the specified chunk.
		
		String[] loadedLines = new String[endIndex-startIndex]; 
		FileReader fReader = new FileReader(this.inputFile.toFile());
		BufferedReader bReader = new BufferedReader(fReader);
		boolean readInRange = (endIndex >= 0); //When EndIndex is set to -1, read the whole file.
		if (readInRange) {
			int arrIndex = 0;
			for (int lineInFile=0; lineInFile<endIndex; lineInFile++) {
				//Read line only when it is in the specified range.
				if (startIndex <= lineInFile && lineInFile<endIndex) {
					loadedLines[arrIndex] = bReader.readLine();
					//If there is only "\n", which is an empty line, add an empty string rather than null
					if (loadedLines[arrIndex] == null) {
						loadedLines[arrIndex] = "";
					}
					arrIndex++;
				}else {
					bReader.readLine();
				}
			}
			bReader.close();
		}else {
			loadedLines = (String[]) Files.readAllLines(this.inputFile).toArray();
		}
		return loadedLines;
	};

}
