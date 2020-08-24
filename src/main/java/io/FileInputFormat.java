package io;

import java.nio.file.Path;

import adoop.Job;

public class FileInputFormat {

	public static void addInputPath(Job job, Path inputPath) {
		job.setInputPath(inputPath);
	}
}
