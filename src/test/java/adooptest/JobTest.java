package adooptest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.FileInputFormat;
import ao.adoop.mapreduce.FileOutputFormat;
import ao.adoop.mapreduce.Job;
import ao.adoop.mapreduce.Mapper;
import ao.adoop.mapreduce.MultipleInputs;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.test.test_usermodules.UnitTestMapper;
import ao.adoop.test.test_usermodules.UnitTestMapper2;
import ao.adoop.test.test_usermodules.UnitTestMapper3;
import javafx.util.Pair;

public class JobTest {
	//This unit test tests Job, Configuration, and FileInputFormat's functionalities.

	@Test
	void testSingleInput() {
		//Test if Job can configure a job with a single mapper and a single input file.
		Path inputFilePath = Paths.get("src/test/resources/map-input.csv");
		Class<? extends Mapper> mapperClass = UnitTestMapper.class;
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test job name");
		job.setMapperClass(mapperClass);
		FileInputFormat.addInputPath(job, inputFilePath);
		ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = job.getMapTasks();
		for (Pair<Path, Class<? extends Mapper>> task: mapTasks) {
			Assertions.assertEquals(inputFilePath.toString(), task.getKey().toString());
			Assertions.assertEquals(mapperClass, task.getValue());
		};
	};
	
	@Test
	void testMutipleInputs() {
		//Test if Job can configure a job with multiple mappers and a multiple input files.
		Path[] inputFilePaths = {
				Paths.get("src/test/resources/map-input-files/map-input.csv"),
				Paths.get("src/test/resources/map-input-files/map-input2.csv"),
				Paths.get("src/test/resources/map-input-files/map-input3.csv")
		};
		ArrayList<Class<? extends Mapper>> mapperClasses = new ArrayList<Class<? extends Mapper>>();
			mapperClasses.add(UnitTestMapper.class);
			mapperClasses.add(UnitTestMapper2.class);
			mapperClasses.add(UnitTestMapper3.class);
		
		//Check the number of inputFilePaths and mapperClasses are the same.
		Assertions.assertEquals(inputFilePaths.length, mapperClasses.size());
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test job name");
		for (int i=0; i<inputFilePaths.length; i++) {
			MultipleInputs.addInputPath(job, inputFilePaths[i], mapperClasses.get(i));
		};
		
		ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = job.getMapTasks();
		for (int i=0; i<inputFilePaths.length; i++) {
			Path storedPath = mapTasks.get(i).getKey();
			Class<? extends Mapper> storedMapperClass = mapTasks.get(i).getValue();
			Assertions.assertEquals(inputFilePaths[i], storedPath);
			Assertions.assertEquals(mapperClasses.get(i), storedMapperClass);
		};
	};
	
	@Test
	void testSingleOutput() {
		Path outputFilePath = Paths.get("some/output/dir/path");
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test job name");
		
		FileOutputFormat.setOutputPath(job, outputFilePath);
		Assertions.assertEquals(outputFilePath, job.getOutputPath());
	}
	
	@Test
	void testMutipleOutputs() {
		//Test if Job holds on to the namedOutput information.
		//This feature is used when a job outputs results to multiple files.
		String[] outputNames = {"OUTPUT-1", "OUTPUT-2", "OUTPUT-3"};
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test job name");
		for (String outputName: outputNames) {
			MultipleOutputs.addNamedOutput(job, outputName);
		}
		ArrayList<String> storedOutputNames = job.getNamedOutputs();
		Assertions.assertArrayEquals(outputNames, storedOutputNames.toArray());
	}
}
