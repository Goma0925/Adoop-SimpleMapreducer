package ao.adoop.test.utils.configurations;

import java.nio.file.Path;
import java.nio.file.Paths;

import ao.adoop.mapreduce.Configuration;

public class TestConfiguration extends Configuration{

	public TestConfiguration() {
		super();
		//Change the super class's path settings.
		this.setBaseDir(Paths.get("./target/test-file-outputs"));

	};
	
	public Path getTestOutputDir() {
		return Paths.get(this.getBaseDir().toAbsolutePath().toString(), "final-outputs");
	}

};
	