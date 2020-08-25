package ao.adoop.io;

import java.nio.file.Path;

import ao.adoop.mapreduce.Job;
import ao.adoop.mapreduce.Mapper;

public class MutipleInputs {

	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) {
		job.addInputAndMapperPair(inputPath, mapperClass);
	}
	
}
