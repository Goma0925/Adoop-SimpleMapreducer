package ao.adoop.mapreduce;

import java.nio.file.Path;

public class FileOutputFormat {

	public static void setOutputFilePath(Job job, Path outputFilePath) {
		job.setOutputPath(outputFilePath);
	}

}
