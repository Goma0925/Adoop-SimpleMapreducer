package ao.adoop.settings;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SystemPathSettings {
	//This class stores path settings.
	public Path systemBaseDir = Paths.get("/Users/Amon/Downloads/Save");
	public Path jobConfigDir = Paths.get(systemBaseDir.toString(), "user-job-configs");
	public Path inputDir = Paths.get(systemBaseDir.toString(), "user-inputs");
	
	public Path mapOutputBaseDir = Paths.get(systemBaseDir.toString(), "map-outputs");
	public String mapOutputFileExtension = ".csv";
	
	public Path reduceOutputBaseDir = Paths.get(systemBaseDir.toString(), "reduce-outputs");
	public String reduceOutputFileName = "output";
	public String reduceOutputFileExtension = ".csv";
	
	public Path finalOutputDir = Paths.get(systemBaseDir.toString(), "final-outputs");
	public String finalOutputFileName = "output";
	public String finalOutputFileExtension = ".csv";
	
	public String getMapOutputFileName(String key, String workerId) {
		return "[" + key + "]-" + workerId + this.mapOutputFileExtension.toString();
	}
}
