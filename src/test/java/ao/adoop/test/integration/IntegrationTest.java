package ao.adoop.test.integration;

import java.io.File;
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
import ao.adoop.test.test_usermodules.MapperForIntegrationTest1;
import ao.adoop.test.test_usermodules.MapperForIntegrationTest2;
import ao.adoop.test.test_usermodules.MultipleOutputReducerForIntegrationTest;
import ao.adoop.test.test_usermodules.ReducerForIntegrationTest;
import ao.adoop.test.utils.SimpleFileLoader;
import testsettings.TestConfiguration;

class IntegrationTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fManager = new FileSystemManager(this.config);

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
		Path outputDir = Paths.get(this.config.mapOutputBufferDir.toString() + "/output.csv");
		Path answerFileDir = Paths.get("src/test/resources/integration-test-answers/single-mapper-single-reducer.csv");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set a mapper and a reducer class for the job.
		job.setMapperClass(MapperForIntegrationTest1.class);
		job.setReducerClass(ReducerForIntegrationTest.class);
		
		//Set an input file and an output file. 
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.waitForCompletion(true);
		
		//Check answers
		File[] answerFiles = SimpleFileLoader.getChildFiles(answerFileDir);
		File[] resultFiles = SimpleFileLoader.getChildFiles(outputDir);
		for (int i=1; i<resultFiles.length; i++) {
			Assertions.assertArrayEquals(
					SimpleFileLoader.readFile(answerFiles[i]), 
					SimpleFileLoader.readFile(resultFiles[i]));			
		}
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
		Path outputDir = this.config.mapOutputBufferDir;
		Path answerFileDir = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFilePath1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFilePath2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(ReducerForIntegrationTest.class);

		//Set an output file. 
		FileOutputFormat.setOutputPath(job, outputDir);

		//Check answers
		File[] answerFiles = SimpleFileLoader.getChildFiles(answerFileDir);
		File[] resultFiles = SimpleFileLoader.getChildFiles(outputDir);
		for (int i=1; i<resultFiles.length; i++) {
			Assertions.assertArrayEquals(
					SimpleFileLoader.readFile(answerFiles[i]), 
					SimpleFileLoader.readFile(resultFiles[i]));			
		}
	};
	
	@Test
	void testMultipleMapperSingleReducerWithMutipleOutputFiles() throws Exception {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  2 output files.
		Path inputFile1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFile2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputDir = Paths.get(this.config.mapOutputBufferDir.toString());
		Path outputDir1 = Paths.get(outputDir.toString() + "/group1/");
		Path outputDir2 = Paths.get(outputDir.toString() + "/group2/");
		Path answerFileDir1 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs1");
		Path answerFileDir2 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs2");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFile1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFile2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(MultipleOutputReducerForIntegrationTest.class);
		
		//Set an output file. 
		FileOutputFormat.setOutputPath(job, outputDir);
				
		//Set named outputs. 
		MultipleOutputs.addNamedOutput(job,"GROUP-1");
		MultipleOutputs.addNamedOutput(job,"GROUP-2");

		job.waitForCompletion(true);
		
		//Check answers 1
		File[] answerFiles = SimpleFileLoader.getChildFiles(answerFileDir1);
		File[] resultFiles = SimpleFileLoader.getChildFiles(outputDir);
		for (int i=1; i<resultFiles.length; i++) {
			Assertions.assertArrayEquals(
					SimpleFileLoader.readFile(answerFiles[i]), 
					SimpleFileLoader.readFile(resultFiles[i]));			
		};
		
		//Check answers 2
		File[] answerFiles2 = SimpleFileLoader.getChildFiles(answerFileDir1);
		File[] resultFiles2 = SimpleFileLoader.getChildFiles(outputDir2);
		for (int i=1; i<resultFiles2.length; i++) {
			Assertions.assertArrayEquals(
					SimpleFileLoader.readFile(answerFiles2[i]), 
					SimpleFileLoader.readFile(resultFiles2[i]));			
		}
	};
}
