package ao.adoop.mapreduce;

import java.nio.file.Path;

public class FileOutputFormat {

	public static void setOutputPath(Job job, Path outputDir) {
		job.setFinalOutputDir(outputDir);
	}
}
