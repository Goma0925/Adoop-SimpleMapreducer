# Adoop - Simple MapReduce program

This application is a Java program to perform mapreduce algorithm on text files in format like CSV, TSV, or other txt files. The program is inspired by [Hadoop](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) framework and was created to provide for text file multiprocessing. While Hadoop provides distributed computing capability, this program is aimed at taking advantage of multiple CPU on a single machine.

## Distribution

This project is a Maven project including unit and integration tests. You can start using by importing it in [Eclipse](https://www.lagomframework.com/documentation/1.6.x/java/EclipseMavenInt.html) or other IDE as well. The exe

## What Adoop can do

Adoop allows a user to write a simple multiprocessing program that runs on different threads using Java multiprocessing library and MapReduce programing pattern. The foundational idea of MapReduce can be found in Hadoop articles. (Reference: [MapReduce](https://en.wikipedia.org/wiki/MapReduce)) 

Using the program, you can achieve things like counting rows in a CSV file that has a particular ID , combining rows from multiple files based on the same string, etc.

## How to use Adoop - API guide

Adoop follows several Hadoop's API in a similar manner but with limited capability to perform the operations. To run a program, you would need to create at least a driver class, a Mapper class, and a Reducer class. 

### Writing MapReduce programs

1. Simple example: Count a number of rows by ID in a single CSV file

    ```java
    // Driver.java
    // First, write a driver that runs the program.
    package your.package;
    import java.nio.file.Path;
    import java.nio.file.Paths;
    import ao.adoop.mapreduce.*; 

    public class Driver {
    	public static void main(String[] args) throws Exception {
    		// Input path can be either a directory or a file. If a directory is specified, 
    		// all the files in the directory will be loaded for the mapper.
    		Path inputFilePath = Paths.get("path/input.csv");
    		Path outputDir = Paths.get("/output/"); // The directory for the output file
    		
    		//Create a configuration instance that contains a job-specifc settings.
    		Configuration config = new Configuration();
    		//Create a job instance that represents a MapReduce job.
    		Job job = Job.getInstance(config, "Test Job"); //String to identify different jobs.
    		
    		//Set a mapper and a reducer class to the job.
    		job.setMapperClass(TestMapper.class);
    		job.setReducerClass(TestReducer.class);
    		
    		//Set an input file path and an output directory to the job. 
    		FileInputFormat.addInputPath(job, inputFilePath);
    		FileOutputFormat.setOutputPath(job, outputDir);
    		
    		//Run the job. True argument turns on verbose and displays progress of the process.
    		job.waitForCompletion(true);
    		
    		//Once the program finishes running, you can find the output file in the outputDir.
    	}
    }
    ```

    ```java
    // TestMapper.java
    // Write a Mapper class to determine the Map phase's process.
    package your.package;
    import ao.adoop.mapreduce.Configuration;
    import ao.adoop.mapreduce.Context;
    import ao.adoop.mapreduce.MapInputSplit;
    import ao.adoop.mapreduce.Mapper;

    public class TestMapper extends Mapper{
    	// A mapper instance represents a worker node running on a single thread in Map phase. At runtime, several mapper classes are created.  
    	public TestMapper(String workerId, Configuration config, MapInputSplit inputSplit) {
    		super(workerId, config, inputSplit);
    	}

    	@Override
    	public void map(String key, String value, Context context) {
    		// Each row in the input file is passed to map function
    		// with key being the row index and the value being the text in the row. 
    		try {
    			String[] parts = value.split(",");
    			String id = parts[0];
    			String text = parts[1];
    			// Collect key:value pair for the next reducer phase
    			context.write(id, text); 
    		}catch (Exception e) {
    			System.out.println("Error input: ");
    			System.out.println(value);
    			System.out.println(e.getMessage());
    		}
    	}
    }
    ```

    ```java
    // TestReducer.java
    // Write a Reducer class to determine the Reducer's process.
    package your.package;
    import java.util.ArrayList;
    import ao.adoop.mapreduce.Configuration;
    import ao.adoop.mapreduce.Context;
    import ao.adoop.mapreduce.ReduceInputSplit;
    import ao.adoop.mapreduce.Reducer;

    public class TestReducer extends Reducer{
    	// A Reducer instance represents a worker node running on a single thread in Reducer phase.
    	// All the value lists of the same key are passed to single reducer. 
    	// Redcuducers are created as many as the number of keys generated in the map phase.
    	public TestReducer(String workerId, Configuration config, ReduceInputSplit inputSplits) {
    		super(workerId, config, inputSplits);
    	}

    	@Override
    	public void reduce(String key, ArrayList<String> values, Context context) {
    		// Reduce funtion takes a key from the map phase and an array list of all values 
    		// that were written to the Context in the mapper.
    		// Reduce function is run as many as the number of keys written to the Map's context.
    		String id = key;
    		context.write(key, Integer.toString(values.size()));
    	}
    }
    ```

2. Input from two different files. (Two input files, two mappers, single reducer)  

    You can set multiple input files of different format by using MultipleInputs class and feed the map results into a single reducer.

    ```java
    //Driver.java
    package your.package;
    import java.nio.file.Path;
    import java.nio.file.Paths;
    import ao.adoop.mapreduce.*; 

    public class Driver {
    	public static void main(String[] args) throws Exception {
    		//Set input files
    		Path inputFilePath1 = Paths.get("path/test-input1.csv");
    		Path inputFilePath2 = Paths.get("path/test-input2.csv");
    		Path outputDir = Paths.get("/output/"); // The directory for the output file
    		
    		Configuration config = new Configuration();
    		Job job = Job.getInstance(config, "Test - Multiple inputs");
    		
    		//Set mappers to map each input file with.
    		MultipleInputs.addInputPath(job, inputFilePath1, TestMapper1.class);
    		MultipleInputs.addInputPath(job, inputFilePath2, TestMapper2.class);
    		
    		//Set a reducer class. Results from both mappers are fed into this reducer.
    		job.setReducerClass(ReducerForIntegrationTest.class);

    		//Set an output file. 
    		FileOutputFormat.setOutputPath(job, outputDir);

    		job.waitForCompletion(true);
    		//Once the program finishes running, you can find the output file in the outputDir.
    	}
    };
    ```

    *The mapper classes and reducer class are omitted for brevity. Refer to the first example.

3. Output to mutiple files. 

You can set write results into different files based on the key.

```java
// Driver.java
package your.package;
import java.nio.file.Path;
import java.nio.file.Paths;
import ao.adoop.mapreduce.*; 

public class Driver {
	public static void main(String[] args) throws Exception {
		Path inputFilePath = Paths.get("path/input.csv");
		Path outputDir = Paths.get("/output/"); // The directory for the output file
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Test Job"); //String to identify different jobs.
		
		//Set a mapper and a reducer class to the job.
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(MultiplOutputReducer .class);
		
		//Set a reducer class
		job.setReducerClass(MultipleOutputReducer.class);
		
		//Set an output directory. 
		FileOutputFormat.setOutputPath(job, outputDir);
				
		job.waitForCompletion(true);
	}
};
```

```java
// TestReducer.java
package ao.adoop.test.utils.usermodules;

import java.util.ArrayList;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.mapreduce.ReduceInputSplit;
import ao.adoop.mapreduce.Reducer;

public class MultipleOutputReducerForIntegrationTest extends Reducer {
	public MultipleOutputReducerForIntegrationTest(String workerId, Configuration config,
			ReduceInputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	MultipleOutputs multipleOutputs = null;
	protected void setup(Context context) {
		this.multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) {
//		Map<String, Integer> counts = new HashMap<String, Integer>();
		String error = "";
		int count = 0;
		int length = values.size();
		for (int i=0; i<length; i++) {
			try {
				count += Integer.parseInt(values.get(i));			
			}catch(Exception e){
				// Record the error value and its key.
				error += key + " : " + values.get(i) + "\n";
			}
		};
		
		//Direct outputs of different keys to different output directories.
		if (key.equals("KEY=1") || key.equals("KEY=2")) {
			// The third parameter expresses a relative output path within the outputDir specified in Driver.
			// The results of KEY=1 and KEY=2 will be written to output-dir/GROUP1
			this.multipleOutputs.write(key, Integer.toString(count), "/GROUP1/");
		}else {
			this.multipleOutputs.write(key, Integer.toString(count), "/GROUP2/");
		}
		if (!error.equals("")) {
			// You can also direct error output to different file.
			this.multipleOutputs.write("ERROR", error, "/ERROR/");
		}
		
	}
}
```

### Configuration

You can also set several configuration about the MapReduce process. 

```java
// Driver.java
package your.package;
import java.nio.file.Path;
import java.nio.file.Paths;
import ao.adoop.mapreduce.*; 

public class Driver {
	public static void main(String[] args) throws Exception {
		Path inputFilePath = Paths.get("path/input.csv");
		Path outputDir = Paths.get("/output/"); 
		
		Configuration config = new Configuration();
		// Set the maximum thread that can be used in rumtime.
		config.setMaxThreadNum(10);
		// Set the amount of data each thread processes. You can set by number and unit.
		// Set each thread's processing limit to 30MB (Approximately)
		config.setThreadMaxThreashholdUnit("MB");
		config.setThreadMaxThreashold(30);

		Job job = Job.getInstance(config, "Test Job"); //String to identify different jobs.
		
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.waitForCompletion(true);
	}
}
```