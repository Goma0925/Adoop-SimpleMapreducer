package io;

import java.nio.file.Path;

import adoop.Job;
import adoop.Mapper;

public class MutipleInputs {

	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) {
		job.addInputAndMapperPair(inputPath, mapperClass);
	}
	
}
