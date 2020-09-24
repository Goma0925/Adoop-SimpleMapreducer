package ao.adoop.mapreduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileInputFormat {

	public static void addInputPath(Job job, Path inputPath) throws Exception {
		FileInputFormat.addInputPathByMapper(job, inputPath, job.getDefaultMapperClass());
	};
	
	protected static void addInputPathByMapper(Job job, Path inputPath, Class<? extends Mapper> mapperClass) throws Exception {
		if (!Files.exists(inputPath)) {
			throw new InvalidPathException(inputPath.toAbsolutePath().toString(), "Input directory or file does not exists.");
		}
		long maxSplitSize = job.config.threadMaxThreashhold;
		String dataUnit = job.config.threadMaxThreashholdUnit;
		if (Files.isDirectory(inputPath)) {
			for (File fileOrDir: inputPath.toFile().listFiles()) {
				if (fileOrDir.isFile()) {
					job.addMapTasks(FileInputFormat.createMapTasks(Paths.get(fileOrDir.toURI()), mapperClass, maxSplitSize, dataUnit));
				}
			}
		}else {
			job.addMapTasks(FileInputFormat.createMapTasks(inputPath, mapperClass, maxSplitSize, dataUnit));
		}
	}
	
	protected static ArrayList<MapTask> createMapTasks(Path inputFile, Class<? extends Mapper> mapperClass, long maxSplitSize, String dataUnit) throws Exception {
		// Each map task consits of a mapperClass and an inputSplit
		ArrayList<int[]> splitStartEndList = FileInputFormat.getSplitStartEndList(inputFile, maxSplitSize, dataUnit);
		ArrayList<MapTask> mapTasks = new ArrayList<MapTask>();
		InputSplit inputSplit = null;
		for (int[] startAndEndIndices: splitStartEndList) {
			inputSplit = new InputSplit(inputFile, startAndEndIndices[0], startAndEndIndices[1]);
			mapTasks.add(new MapTask(mapperClass, inputSplit));
		};
		return mapTasks;
	};
	
	private static int countLines(Path file) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()));
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
	};
	
	private static ArrayList<int[]> getSplitStartEndList(Path file, long maxChunkSize, String unit) throws Exception {
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
            throw new Exception("'" + unit + "' is not supported in getChunkLineNumbers() function. Available data units are " + availableUnitList.toString());
        }else {
        	unitTableIndex = availableUnitList.indexOf(unit);
        }
        
		long fileSizeInByte = Files.size(file); //Size of the target file.
		long maxChunkSizeInByte = maxChunkSize * (long)Math.pow(1000, unitTableIndex); //Size of each chunk in byte
		double numberOfLinesInFile = FileInputFormat.countLines(file); //Number of lines contained in the target file.
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
	};

}
