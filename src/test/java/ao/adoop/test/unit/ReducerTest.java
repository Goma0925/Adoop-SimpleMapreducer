package ao.adoop.test.unit;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ao.adoop.io.DataLoader;
import ao.adoop.io.FileSystemManager;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.InvalidReducerException;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.test.utils.SimpleFileLoader;
import ao.adoop.test.utils.configurations.TestConfiguration;
import ao.adoop.test.utils.usermodules.UnitTestMultipleOutputReducer;
import ao.adoop.test.utils.usermodules.UnitTestReducer;
import ao.adoop.test.utils.CustomAssertions;

class ReducerTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fileSystemManager = new FileSystemManager(this.config);

	
	@BeforeEach
	void clean() throws IOException {
		this.fileSystemManager.initFileSystem();
		this.fileSystemManager.clearFinalOutputDir();
	}
	
	@Test
	void testSingleOutput() throws InvalidReducerException, IOException {	
		//Set up test
		String reducerId = "Test-Reduce-process";
		Path reduceInputFileDir = Paths.get("src/test/resources/reduce-input-files");
		Path reduceOutputAnswerFileDir = Paths.get("src/test/resources/reduce-test-answer/single-output");
		Path outputDir = this.config.finalOutputDir;
		
		//Get the reduce input file paths
		File[] reduceInputFilesInArray = reduceInputFileDir.toFile().listFiles();
		ArrayList<File> reduceInputFiles = new ArrayList<File>();
		
		for (File inputFile: reduceInputFilesInArray) {
			reduceInputFiles.add(inputFile);
		};
		
		//Run reduce
		Reducer reducer = new UnitTestReducer(reducerId, this.config, reduceInputFiles);
		reducer.run();
				
		//Load the outputs and the expected answers.
		File[] answerFiles = SimpleFileLoader.getChildFiles(reduceOutputAnswerFileDir);
		File[] outputFiles = SimpleFileLoader.getChildFiles(outputDir);
		
		//Assert if the numbers of files in the answer and the output are the same
		Assertions.assertEquals(answerFiles.length, outputFiles.length);
		
		//Assert if the answer and output are the same.
		String[] answerLines = null;
		String[] outputLines = null;
		for (int i=0; i<outputFiles.length; i++) {
			answerLines = SimpleFileLoader.readFile(answerFiles[i]);
			outputLines = SimpleFileLoader.readFile(outputFiles[i]);
			Assertions.assertArrayEquals(answerLines, outputLines);			
		}
	};
	
	@Test
	void testMultipleOutput() throws IOException {
		//Set up test
		String reducerId = "Test-Reduce-process";
		Path reduceInputFileDir = Paths.get("src/test/resources/reduce-input-files");
		Path answerDir1 = Paths.get("src/test/resources/reduce-test-answer/multiple-outputs/output-dir1");
		Path answerDir2 = Paths.get("src/test/resources/reduce-test-answer/multiple-outputs/output-dir2");
		Path outputDir1 = Paths.get(this.config.finalOutputDir.toString(), "output-dir1");
		Path outputDir2 = Paths.get(this.config.finalOutputDir.toString(), "output-dir2");
		
		//Get the reduce input file paths
		File[] reduceInputFilesInArray = reduceInputFileDir.toFile().listFiles();
		ArrayList<File> reduceInputFiles = new ArrayList<File>();
		
		for (File inputFile: reduceInputFilesInArray) {
			reduceInputFiles.add(inputFile);
		};
		
		//Run reduce
		Reducer reducer = new UnitTestMultipleOutputReducer(reducerId, this.config, reduceInputFiles);
		reducer.run();
		
		CustomAssertions.assertEachFileContent(answerDir1, outputDir1);
		CustomAssertions.assertEachFileContent(answerDir2, outputDir2);
	}


}
