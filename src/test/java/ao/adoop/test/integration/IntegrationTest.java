package ao.adoop.test.integration;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Assertions;
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
import testsettings.TestPathSettings;

class IntegrationTest {
	SystemPathSettings pathSettings = new TestPathSettings();
	FileSystemManager fManager = new FileSystemManager(this.pathSettings);

//	@BeforeEach
//	void setup() throws NotDirectoryException {
//		this.fManager.clearMapOutputDir();
//		this.fManager.clearReduceOutputDir();
//	}
	
	@Test
	void testSingleMapperSingleReducer() {
		//This test checks for the case:
		//	1 input file
		//	1 mapper class for the input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path outputFilePath = Paths.get(this.pathSettings.mapOutputBaseDir.toString() + "/output.csv");
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
		Assertions.assertEquals(outputFilePath.toFile(), answerFilePath.toFile());
	};
	
	@Test
	void testMultipleMapperSingleReducer() {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  1 output files.
		Path inputFilePath1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFilePath2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputFilePath = Paths.get(this.pathSettings.mapOutputBaseDir.toString() + "/output.csv");
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
		Assertions.assertEquals(outputFilePath.toFile(), answerFilePath.toFile());
	};
	
	@Test
	void testMultipleMapperSingleReducerWithMutipleOutputFiles() {
		//This test checks for the case:
		//	2 input file
		//	2 mapper classes for each input file
		//	1 reducer class.
		//  2 output files.
		Path inputFilePath1 = Paths.get("src/test/resources/map-input-files/integration-test-input1.csv");
		Path inputFilePath2 = Paths.get("src/test/resources/map-input-files/integration-test-input2.csv");
		Path outputFilePath1 = Paths.get(this.pathSettings.mapOutputBaseDir.toString() + "/output.csv");
		Path outputFilePath2 = Paths.get(this.pathSettings.mapOutputBaseDir.toString() + "/output2.csv");
		Path answerFilePath1 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs1.csv");
		Path answerFilePath2 = Paths.get("src/test/resources/integration-test-answers/multiple-mapper-single-reducer-mutiple-outputs2.csv");
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job");
		
		//Set two mapper classes and two input files
		MultipleInputs.addInputPath(job, inputFilePath1, MapperForIntegrationTest1.class);
		MultipleInputs.addInputPath(job, inputFilePath2, MapperForIntegrationTest2.class);
		
		//Set a reducer class
		job.setReducerClass(ReducerForIntegrationTest.class);

		//Set an output file. 
		MultipleOutputs.addNamedOutput(job,"OUTPUT-1");
		MultipleOutputs.addNamedOutput(job,"OUTPUT-2");

		job.waitForCompletion(true);
		Assertions.assertEquals(outputFilePath1.toFile(), answerFilePath1.toFile());
		Assertions.assertEquals(outputFilePath2.toFile(), answerFilePath2.toFile());
	};
}
