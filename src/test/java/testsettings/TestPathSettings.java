package testsettings;

import java.nio.file.Path;
import java.nio.file.Paths;

import ao.adoop.settings.SystemPathSettings;

public class TestPathSettings extends SystemPathSettings{

	public TestPathSettings() {
		super();
		//Change the super class's path settings.
		this.systemBaseDir = Paths.get("./target/test-file-outputs");
		this.jobConfigDir = Paths.get(systemBaseDir.toString(), "user-job-configs");
		this.inputDir = Paths.get(systemBaseDir.toString(), "user-inputs");
		this.mapOutputBaseDir = Paths.get(systemBaseDir.toString(), "map-outputs");
		this.mapOutputFileExtension = ".csv";
		this.reduceOutputBaseDir = Paths.get(systemBaseDir.toString(), "reduce-outputs");;
		this.reduceOutputFileExtension = ".csv";	
		this.finalOutputDir = Paths.get(systemBaseDir.toString(), "final-outputs");
		this.finalOutputFileName = "output";
		this.finalOutputFileExtension = ".csv";
		
	};

};
	