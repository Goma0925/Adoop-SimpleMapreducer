package ao.adoop.io;

import java.nio.file.Path;

import ao.adoop.mapreduce.Job;

public class FileInputFormat {

	public static void addInputPath(Job job, Path inputPath) {
		job.setInputPath(inputPath);
	}
}
