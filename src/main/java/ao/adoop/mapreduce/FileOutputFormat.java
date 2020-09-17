package ao.adoop.mapreduce;

import java.nio.file.NotDirectoryException;
import java.nio.file.Path;

public class FileOutputFormat {
	public static void setOutputPath(Job job, Path outputDirPath) {
		if (!outputDirPath.toFile().isDirectory()) {
			job.setOutputPath(outputDirPath);
		}else {
			new NotDirectoryException("The output path must be a directory: " + outputDirPath.toString());
		}
	};
	
}
