package ao.adoop.test.integration;

import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ao.adoop.io.FileSystemManager;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.FileInputFormat;
import ao.adoop.mapreduce.FileOutputFormat;
import ao.adoop.mapreduce.Job;
import ao.adoop.mapreduce.MultipleInputs;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.settings.SystemPathSettings;
import ao.adoop.test.test_usermodules.MapperForIntegrationTest1;
import ao.adoop.test.test_usermodules.MapperForIntegrationTest2;
import ao.adoop.test.test_usermodules.MultipleOutputReducerForIntegrationTest;
import ao.adoop.test.test_usermodules.ReducerForIntegrationTest;
import ao.adoop.test.utils.SimpleFileLoader;
import testsettings.TestPathSettings;

class IntegrationTest {
	SystemPathSettings pathSettings = new TestPathSettings();
	FileSystemManager fManager = new FileSystemManager(this.pathSettings);

	@Before
	void setup() throws IOException {
	}
	
	@BeforeEach
	void cleanBuffers() throws IOException {
		this.fManager.initFileSystem();
		this.fManager.clearMapOutputBufferDir();
		this.fManager.clearReduceOutputBufferDir();
	}
	
//	@Test
	void testSingleMapperSingleReducer() throws Exception {
		//This test checks for the case:
		//	1 input file
		//	1 mapper class for the input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path outputFilePath = Paths.get(this.pathSettings.mapOutputBufferDir.toString() + "/output.csv");
		Path answerFilePath = Paths.get("src/test/resources/integration-test-answers/single-mapper-single-reducer.csv");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set a mapper and a reducer class for the job.
		job.setMapperClass(MapperForIntegrationTest1.class);
		job.setReducerClass(ReducerForIntegrationTest.class);
		
		//Set an input file and an output file. 
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputFilePath(job, outputFilePath);
		
		job.waitForCompletion(true);
		Assertions.assertArrayEquals(SimpleFileLoader.readFile(outputFilePath.toFile()), 
				SimpleFileLoader.readFile(answerFilePath.toFile()));
	};
	
//	@Test
	void testMultipleMapperSingleReducer() throws Exception {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFilePath2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputFilePath = Paths.get(this.pathSettings.mapOutputBufferDir.toString() + "/output.csv");
		Path answerFilePath = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer.csv");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFilePath1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFilePath2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(ReducerForIntegrationTest.class);

		//Set an output file. 
		FileOutputFormat.setOutputFilePath(job, outputFilePath);

		job.waitForCompletion(true);
		Assertions.assertArrayEquals(SimpleFileLoader.readFile(outputFilePath.toFile()),
				SimpleFileLoader.readFile(answerFilePath.toFile()));
	};
	
	@Test
	void testMultipleMapperSingleReducerWithMutipleOutputFiles() throws Exception {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  2 output files.
		Path inputFilePath1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFilePath2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputDirPath = Paths.get(this.pathSettings.mapOutputBufferDir.toString());
		Path outputFilePath1 = Paths.get(outputDirPath.toString() + "/group1/output1.csv");
		Path outputFilePath2 = Paths.get(outputDirPath.toString() + "/group2/output2.csv");
		Path answerFilePath1 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs1.csv");
		Path answerFilePath2 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs2.csv");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFilePath1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFilePath2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(MultipleOutputReducerForIntegrationTest.class);
		
		//Set an output file. 
		FileOutputFormat.setOutputFilePath(job, outputDirPath);
				
		//Set named outputs. 
		MultipleOutputs.addNamedOutput(job,"GROUP-1");
		MultipleOutputs.addNamedOutput(job,"GROUP-2");

		job.waitForCompletion(true);
		Assertions.assertEquals(SimpleFileLoader.readFile(outputFilePath1.toFile()),
				SimpleFileLoader.readFile(answerFilePath1.toFile()));
		Assertions.assertEquals(SimpleFileLoader.readFile(outputFilePath2.toFile()),
				answerFilePath2.toFile());
	};
}
