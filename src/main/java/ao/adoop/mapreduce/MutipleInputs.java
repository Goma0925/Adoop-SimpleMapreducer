package ao.adoop.mapreduce;

import java.nio.file.Path;

public class MutipleInputs {

	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) {
		job.addInputAndMapperPair(inputPath, mapperClass);
	}
	
}
