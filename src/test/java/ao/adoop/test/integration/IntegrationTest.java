package ao.adoop.test.integration;

import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ao.adoop.io.FileSystemManager;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.FileInputFormat;
import ao.adoop.mapreduce.FileOutputFormat;
import ao.adoop.mapreduce.Job;
import ao.adoop.mapreduce.MultipleInputs;
import ao.adoop.test.utils.CustomAssertions;
import ao.adoop.test.utils.configurations.TestConfiguration;
import ao.adoop.test.utils.usermodules.MapperForIntegrationTest1;
import ao.adoop.test.utils.usermodules.MapperForIntegrationTest2;
import ao.adoop.test.utils.usermodules.MultipleOutputReducerForIntegrationTest;
import ao.adoop.test.utils.usermodules.ReducerForIntegrationTest;

class IntegrationTest {
	Configuration config = new TestConfiguration();
	FileSystemManager fManager = new FileSystemManager(this.config);
	Path outputDir = Paths.get(this.config.getBaseDir().toAbsolutePath().toString(), "final-outputs");

	public IntegrationTest() throws NotDirectoryException {
		//For a testing purpose, set the output directory that is usually set at the starting phase.
		this.config.setFinalOutputDir(this.outputDir);
	}

	@BeforeEach
	void cleanBuffers() throws IOException {
		this.fManager.clearDir(this.outputDir);
	}
	
	@Test
	void testSingleMapperSingleReducer() throws Exception {
		//This test checks for the case:
		//	1 input file
		//	1 mapper class for the input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path outputDir = this.outputDir;
		Path answerFileDir = Paths.get("src/test/resources/integration-test-answers/single-mapper-single-reducer");
		
		Configuration config = new TestConfiguration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set a mapper and a reducer class for the job.
		job.setMapperClass(MapperForIntegrationTest1.class);
		job.setReducerClass(ReducerForIntegrationTest.class);
		
		//Set an input file and an output file. 
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.waitForCompletion(true);
		
		//Check answers
		CustomAssertions.assertEachFileContent(answerFileDir,outputDir);

	};
	
	@Test
	void testMultipleMapperSingleReducer() throws Exception {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFilePath2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputDir = this.outputDir;
		Path answerFileDir = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer");
		
		Configuration config = new TestConfiguration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFilePath1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFilePath2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(ReducerForIntegrationTest.class);

		//Set an output file. 
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);
		
		//Check answers
		CustomAssertions.assertEachFileContent(answerFileDir,outputDir);

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
		Path outputDir = this.outputDir;
		Path outputDir1 = Paths.get(outputDir.toString() + "/GROUP1/"); //Output group1 dir
		Path outputDir2 = Paths.get(outputDir.toString() + "/GROUP2/"); //Output group2 dir
		Path answerFileDir1 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-multiple-outputs1");
		Path answerFileDir2 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-multiple-outputs2");
		
		Configuration config = new TestConfiguration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFile1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFile2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(MultipleOutputReducerForIntegrationTest.class);
		
		//Set an output path. 
		FileOutputFormat.setOutputPath(job, outputDir);
				
		job.waitForCompletion(true);
		
		//Check answer output to group1
		CustomAssertions.assertEachFileContent(answerFileDir1, outputDir1);
		
		//Check answer output to group2
		CustomAssertions.assertEachFileContent(answerFileDir2, outputDir2);
	};
}
