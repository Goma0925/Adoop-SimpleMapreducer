package adooptest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ao.adoop.io.DataLoader;
import ao.adoop.io.FileSystemManager;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.InvalidMapperException;
import ao.adoop.mapreduce.Mapper;
import ao.adoop.test.test_usermodules.UnitTestMapper;
import ao.adoop.test.utils.SimpleFileLoader;
import testsettings.TestConfiguration;

class MapperTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fileSystemManager = new FileSystemManager(this.config);
	
	@Test
	void matchInputAndOutput() throws IOException {
		//Set up the file storage
		this.fileSystemManager.initFileSystem();
		this.fileSystemManager.clearMapOutputBufferDir();
		File outputDir = config.mapOutputBufferDir.toFile();
		File outputFile = null;

		
		int startIndex = 0;
		int endIndex = 50;
		String path = "src/test/resources/map-input-files/map-input.csv";
		String mapperId = "Test-ID";
		File inputFile = new File(path);
		Mapper mapper = new UnitTestMapper(mapperId, this.config, inputFile, startIndex, endIndex, new String[0]);
		mapper.run();
		
		//Check output
		int outputLineLength = -1;
		String key = null;
		//The input.csv file contains 5 different keys. 5 files should be created for each key and 
		// each file should contain 10 keys
		for (int i=0; i<5; i++) {
			key = "key"+Integer.toString(i);
			outputFile = new File(outputDir, key + "/" + config.getMapOutputFileName(mapperId));
			String[] outputLines = SimpleFileLoader.readFile(outputFile);
			//Check if the first line of each file contains the key
			Assertions.assertEquals(outputLines[0], key);  
			//Check the rest of the lines have the correct values
			for (int j=1; j<outputLineLength; j++) {
				Assertions.assertEquals(outputLines[i], "value="+Integer.toString(i));
			}
			//Check if the mapper output contains the correct number of values that should be in the output
			Assertions.assertEquals(outputLines.length, 11);
		}
	}

}
