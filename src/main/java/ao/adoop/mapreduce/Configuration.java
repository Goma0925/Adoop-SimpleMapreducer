package ao.adoop.mapreduce;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Configuration {
	public Path systemBaseDir = Paths.get("/Users/Amon/Downloads/Save");
	public Path jobConfigDir = Paths.get(systemBaseDir.toString(), "user-job-configs");
	public Path inputDir = Paths.get(systemBaseDir.toString(), "user-inputs");
	
	public Path mapOutputBufferDir = Paths.get(systemBaseDir.toString(), "map-output-buffer");
	public String mapOutputFileExtension = ".csv";
	
//	public Path reduceOutputBufferDir = Paths.get(systemBaseDir.toString(), "reduce-output-buffer");
//	public Path namedReduceOutputBaseDir = Paths.get(systemBaseDir.toString(), "named-reduce-output-buffer");
//	public String reduceOutputFileName = "output";
//	public String reduceOutputFileExtension = ".csv";
		
	public Path finalOutputDir = Paths.get(systemBaseDir.toString(), "final-outputs");
	public String finalOutputFileName = "output";
	public String finalOutputFileExtension = ".csv";
	
	public String getMapOutputFileName(String key, String workerId) {
		return "[" + key + "]-" + workerId + this.mapOutputFileExtension.toString();
	}

	public String generateReduceOutputFileName(String workerId) {
		return "part-r" + workerId + this.finalOutputFileExtension;
	}
}
