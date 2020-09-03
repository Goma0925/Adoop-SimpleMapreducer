package ao.adoop.mapreduce;

import java.nio.file.Path;

public class FileInputFormat {

	public static void addInputPath(Job job, Path inputPath) {
		job.setInputPath(inputPath);
	}

	public static void setOutputPath(Job job, Path outputDir) {
		job.setFinalOutputDir(outputDir);
	}
}
