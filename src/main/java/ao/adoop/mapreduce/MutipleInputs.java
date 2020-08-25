package ao.adoop.mapreduce;

import java.nio.file.Path;

public class MutipleInputs {
	Context context = null;
	public MutipleInputs(Context context) {
		this.context = context;
	}

	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) {
		job.addInputAndMapperPair(inputPath, mapperClass);
	}

	public void write(String nameSpace, String key, String string, String baseOutputPath) {
		// TODO Auto-generated method stub
		
	}
	
}
