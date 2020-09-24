package ao.adoop.mapreduce;

import java.nio.file.Path;

public class MultipleInputs {
	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) throws Exception {		
		FileInputFormat.addInputPathByMapper(job, inputPath, mapperClass);
	}
}

