package ao.adoop.test.unit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ao.adoop.io.FileSystemManager;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.ReduceInputSplit;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.test.utils.CustomAssertions;
import ao.adoop.test.utils.SimpleFileLoader;
import ao.adoop.test.utils.configurations.TestConfiguration;
import ao.adoop.test.utils.usermodules.UnitTestMultipleOutputReducer;
import ao.adoop.test.utils.usermodules.UnitTestReducer;

class ReducerTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fileSystemManager = new FileSystemManager(this.config);
	Path outputDir = Paths.get(this.config.getBaseDir().toAbsolutePath().toString(), "final-outputs");

	public ReducerTest() throws IOException {
		this.config.setFinalOutputDir(this.outputDir);
		this.fileSystemManager.initFileSystem();
	};
	

	@BeforeEach
	void clean() throws IOException {
		this.fileSystemManager.clearDir(this.outputDir);
	}
	
	@Test
	void testSingleOutput() throws IOException {	
		//Set up test
		String reducerId = "Test-Reduce-process";
		Path reduceInputFileDir = Paths.get("src/test/resources/reduce-input-files");
		Path reduceOutputAnswerFileDir = Paths.get("src/test/resources/reduce-test-answer/single-output");
		Path outputDir = this.outputDir;
		
		//Get the reduce input file paths
		File[] reduceInputFilesInArray = reduceInputFileDir.toFile().listFiles();
		ArrayList<Path> reduceInputFiles = new ArrayList<Path>();
		for (File inputFile: reduceInputFilesInArray) {
			reduceInputFiles.add(Paths.get(inputFile.getAbsolutePath()));
		};
		
		//Run reduce
		ReduceInputSplit inputSplit = new ReduceInputSplit(reduceInputFiles.toArray(new Path[reduceInputFiles.size()]));
		Reducer reducer = new UnitTestReducer(reducerId, this.config, inputSplit);
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
		Path outputDir1 = Paths.get(this.outputDir.toString(), "output-dir1");
		Path outputDir2 = Paths.get(this.outputDir.toString(), "output-dir2");
		
		//Get the reduce input file paths
		File[] reduceInputFilesInArray = reduceInputFileDir.toFile().listFiles();
		ArrayList<Path> reduceInputFiles = new ArrayList<Path>();
		for (File inputFile: reduceInputFilesInArray) {
			reduceInputFiles.add(Paths.get(inputFile.getAbsolutePath()));
		};
		
		//Run reduce
		ReduceInputSplit inputSplit = new ReduceInputSplit(reduceInputFiles.toArray(new Path[reduceInputFiles.size()]));
		Reducer reducer = new UnitTestMultipleOutputReducer(reducerId, this.config, inputSplit);
		reducer.run();
		
		CustomAssertions.assertEachFileContent(answerDir1, outputDir1);
		CustomAssertions.assertEachFileContent(answerDir2, outputDir2);
	}


}
