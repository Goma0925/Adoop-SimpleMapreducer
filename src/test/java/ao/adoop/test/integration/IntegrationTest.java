package ao.adoop.test.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.FileInputFormat;
import ao.adoop.mapreduce.Job;
import ao.adoop.test.test_usermodules.MapperForIntegrationTest1;

class IntegrationTest {

	@Before
	void setup() {
		
	}
	
	@Test
	void testSingleMapperSingleReducer() {
		String inputPathStr = "src/test/resources/map-input-files/integration-test-input1.csv";
		String outputPathStr = ""
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job1");
		job.setMapperClass(MapperForIntegrationTest1.class);
		job.setReducerClass(ReducerForIntegrationTest.class);
		FileInputFormat.addInputPath(job, inputPath);
	}

}
