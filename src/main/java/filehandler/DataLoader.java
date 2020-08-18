package filehandler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataLoader {
	public int countLines(File file) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(file));
	    try {
	        byte[] c = new byte[1024];

	        int readChars = is.read(c);
	        if (readChars == -1) {
	            // bail out if nothing to read
	            return 0;
	        }
	        // make it easy for the optimizer to tune this loop
	        int count = 0;
	        while (readChars == 1024) {
	            for (int i=0; i<1024;) {
	                if (c[i++] == '\n') {
	                    ++count;
	                }
	            }
	            readChars = is.read(c);
	        }
	        // count remaining characters
	        while (readChars != -1) {
	            for (int i=0; i<readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	            readChars = is.read(c);
	        }
	        return count == 0 ? 1 : count+1;
	    } finally {
	        is.close();
	    }
	}
	
	public ArrayList<int[]> getChunkIndices(File file, long maxChunkSize, String unit) throws Exception {
		//This function takes the size of each chunk and 
		// retrieves the the start and the end indices in a 2D array. Each pair of the start and the end indices 
		// represent each chunk of to be distributed where the chunk is approximately the size of maxChunkSize. 
		// Args| 	file: input file to analyze
		//			long maxChunkSize: the number of bytes, magabytes, kilobytes, etc
		//			String: Unit for the size. B(bytes), KB(kilo), MG(Mega bytes), GB(Giga bytes), TB(Tela bytes) are available.
		//	*WARMING:This function treats 1000B = 1KB, 1000KB = 1MB, so on. 
		// Return| int[] chunkLineNumbers: Each element represents the number of lines to be distributed 
		
		//Check if the unit is valid.
		String[] availableUnits = {"B", "KB", "MB", "GB", "TB"};
		int unitTableIndex = 0;
		List<String> availableUnitList = Arrays.asList(availableUnits);
        if(!availableUnitList.contains(unit)){
            throw new Exception("'" + unit + "' is not supported in getChunkLineNumbers() function. Available units are " + availableUnitList.toString());
        }else {
        	unitTableIndex = availableUnitList.indexOf(unit);
        }
        
		long fileSizeInByte = Files.size(Paths.get(file.getPath())); //Size of the target file.
		long maxChunkSizeInByte = maxChunkSize * (long)Math.pow(1000, unitTableIndex); //Size of each chunk in byte
		double numberOfLinesInFile = this.countLines(file); //Number of lines contained in the target file.
		System.out.println("numberOfLinesInFile="+Integer.toString((int)numberOfLinesInFile));
		double lineSizeInByte = fileSizeInByte / numberOfLinesInFile;//Size of each line in the target file in byte.
		int lineNumberInEachChunk = (int) (maxChunkSizeInByte / lineSizeInByte); //Number of lines for each chunk.
		
		//Construct the indices for each chunk
		int numberOfFullChunks = Math.floorDiv((int) numberOfLinesInFile, lineNumberInEachChunk); //Number of chunks that contain the full amount of data that can be allocated.
		ArrayList<int[]> chunkIndices = new ArrayList<int[]>();
		int currentIndex = 0;
		for (int i=0; i<numberOfFullChunks; i++) {
			int[] indexPair = new int[2];
			indexPair[0] = currentIndex;
			indexPair[1] = currentIndex + lineNumberInEachChunk;
			chunkIndices.add(indexPair);
			currentIndex = currentIndex + lineNumberInEachChunk;
		};
		//Add the last chunk indices
		int[] lastIndexPair = {currentIndex, (int) numberOfLinesInFile};
		chunkIndices.add(lastIndexPair);
		return chunkIndices;
	}
	
	public String[] loadChunkByLineIndices(File file, int startIndex, int endIndex) throws IOException {
		//This function loads a particular chunk in a file based on the line start and the end indices.
		//return| String[] loadedLines: An array containing all the lines of the specified chunk.
		System.out.println("Reading:["+Integer.toString(startIndex) + ","+Integer.toString(endIndex) + "]");
		String[] loadedLines = new String[endIndex-startIndex]; 
		FileReader fReader = new FileReader(file);
		BufferedReader bReader = new BufferedReader(fReader);
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
		
		return loadedLines;
	}
	
	public ArrayList<String> loadFile(File file) throws IOException {
		//Read all lines in a file
		ArrayList<String> lines = new ArrayList<String>();
		String line;
		FileReader fReader = new FileReader(file);
		BufferedReader bReader = new BufferedReader(fReader);
        while ((line = bReader.readLine()) != null) {
            lines.add(line);
         };
         bReader.close();
		return lines;
	}
}
