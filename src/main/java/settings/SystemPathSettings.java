package adoop;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Constants {
	//This class stores static settings.
	public static final Path systemBaseDir = Paths.get("/Users/Amon/Downloads/Save");
	public static final Path jobConfigDir = Paths.get(systemBaseDir.toString(), "user-job-configs");
	public static final Path inputDir = Paths.get(systemBaseDir.toString(), "user-inputs");
	public static final Path mapOutputBaseDir = Paths.get(systemBaseDir.toString(), "map-outputs");
	public static final String mapOutputFileExtension = ".csv";
	public static final Path reduceOutputBaseDir = Paths.get(systemBaseDir.toString(), "reduce-outputs");;
	public static final String reduceOutputFileExtension = ".csv";
}
