package adooptest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import adoop.MapExecutor;
import exceptions.InvalidMapperException;
import filehandler.DataLoader;
import filehandler.FileSystemManager;
import settings.SystemPathSettings;
import test_usermodules.InvalidClass;
import test_usermodules.TestMapper;
import testsettings.TestPathSettings;

class MapExecutorTest {
	SystemPathSettings pathSettings = new TestPathSettings();
	FileSystemManager fileSystemManager = new FileSystemManager(this.pathSettings);
	@Test
	void matchInputAndOutput() throws InvalidMapperException, InstantiationException, IllegalAccessException, IOException {
		//Set up the file storage
		this.fileSystemManager.initFileSystem();
		this.fileSystemManager.clearMapOutputDir();
		
		int startIndex = 0;
		int endIndex = 50;
		String path = "src/test/resources/map-input.csv";
		String mapperId = "Test-ID";
		File inputFile = new File(path);
		DataLoader loader = new DataLoader();
		MapExecutor mExcecutor = new MapExecutor(mapperId, TestMapper.class, this.pathSettings, inputFile, startIndex, endIndex);
		mExcecutor.run();
		
		File outputDir = pathSettings.mapOutputBaseDir.toFile();
		File outputFile = null;
		int outputLineLength = -1;
		String key = null;
		//The input.csv file contains 5 different keys. 5 files should be created for each key and 
		// each file should contain 10 keys
		for (int i=0; i<5; i++) {
			key = "key"+Integer.toString(i);
			outputFile = new File(outputDir, key + "/" + pathSettings.getMapOutputFileName(key, mapperId));
			ArrayList<String> outputLines = loader.loadFile(outputFile);
			//Check if the first line of each file contains the key
			Assertions.assertEquals(outputLines.get(0), key);  
			//Check the rest of the lines have the correct values
			for (int j=1; j<outputLineLength; j++) {
				Assertions.assertEquals(outputLines.get(j), "value="+Integer.toString(i));
			}
			//Check if the mapper output contains the correct number of values that should be in the output
			Assertions.assertEquals(outputLines.size(), 11);
		}
	}
	
	@Test
	void passInvalidMapper(){
		String path = "src/test/resources/input.csv";
		File inputFile = new File(path);
		int startIndex = 0;
		int endIndex = 50;
		try {
			@SuppressWarnings("unused")
			MapExecutor mExcecutor = new MapExecutor("Test-ID", InvalidClass.class, this.pathSettings, inputFile, startIndex, endIndex);
			fail("	MapExecutor failed to catch an exception when being passed a non-mapper class");
		} catch (InvalidMapperException e) {
			System.out.println("	MapExecutor successfully raised an error when being passed a non-mapper class.");
		}
	}

}
