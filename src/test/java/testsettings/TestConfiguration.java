package testsettings;

import java.nio.file.Path;
import java.nio.file.Paths;

import ao.adoop.mapreduce.Configuration;

public class TestConfiguration extends Configuration{

	public TestConfiguration() {
		super();
		//Change the super class's path settings.
		this.systemBaseDir = Paths.get("./target/test-file-outputs");
		this.jobConfigDir = Paths.get(systemBaseDir.toString(), "user-job-configs");
		this.inputDir = Paths.get(systemBaseDir.toString(), "user-inputs");
		this.mapOutputBufferDir = Paths.get(systemBaseDir.toString(), "map-output-buffer");
		this.mapOutputFileExtension = ".csv";
		this.reduceOutputBufferDir = Paths.get(systemBaseDir.toString(), "reduce-output-buffer");;
		this.reduceOutputFileExtension = ".csv";
		this.finalOutputDir = Paths.get(systemBaseDir.toString(), "final-outputs");
		this.finalOutputFileName = "output";
		this.finalOutputFileExtension = ".csv";
		this.namedReduceOutputBufferDir = Paths.get(systemBaseDir.toString(), "named-reduce-output-buffer");
		
	};

};
	