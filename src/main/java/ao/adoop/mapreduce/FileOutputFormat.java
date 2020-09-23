package ao.adoop.mapreduce;

import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;

public class FileOutputFormat {
	public static void setOutputPath(Job job, Path outputDirPath) throws NotDirectoryException {
		if (Files.isDirectory(outputDirPath)) {
			job.setOutputPath(outputDirPath);
		}else {
			throw new NotDirectoryException(outputDirPath.toAbsolutePath().toString());
		}
	};
	
}
