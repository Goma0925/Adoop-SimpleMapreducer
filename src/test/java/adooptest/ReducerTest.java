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
import ao.adoop.mapreduce.InvalidReducerException;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.test.test_usermodules.UnitTestReducer;
import testsettings.TestConfiguration;

class ReducerTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fileSystemManager = new FileSystemManager(this.config);
	
	@Test
	void test() throws InvalidReducerException, IOException {
		this.fileSystemManager.initFileSystem();
		this.fileSystemManager.clearReduceOutputBufferDir();
		
		//Set up test
		String reducerId = "Test-Reduce-process";
		DataLoader loader = new DataLoader();
		String reduceInputFileDirPath = "src/test/resources/reduce-input-files";
		String reduceOutputAnswerFilePath = "src/test/resources/reduce-test-answer/reduce-unit-test-answer.csv";
		String outputFilePathStr = this.config.finalOutputDir.toString() + "/output.csv";
		
		//Get the reduce input file paths
		File[] reduceInputFilesInArray = new File(reduceInputFileDirPath).listFiles();
		ArrayList<File> reduceInputFiles = new ArrayList<File>();
		
		for (File inputFile: reduceInputFilesInArray) {
			reduceInputFiles.add(inputFile);
		};
		
		//Run reduce
		Reducer reducer = new UnitTestReducer(reducerId, this.config, reduceInputFiles, new String[0]);
		reducer.run();
		
		//Merge all the reduce outputs to a single output file specified at outputFilePath.
//		this.fileSystemManager.mergeReduceOutputs(new File(outputFilePathStr));
		
		//Check the output
		ArrayList<String> answerLines = loader.loadFile(new File(reduceOutputAnswerFilePath));
		ArrayList<String> outputLines = loader.loadFile(new File(outputFilePathStr));
		
		//Assert if the numbers of lines in the answer and the output are the same
		Assertions.assertEquals(answerLines.size(), outputLines.size());
		//Assert if the answer and output are the same.
		Assertions.assertArrayEquals(answerLines.toArray(), outputLines.toArray());
	};


}
