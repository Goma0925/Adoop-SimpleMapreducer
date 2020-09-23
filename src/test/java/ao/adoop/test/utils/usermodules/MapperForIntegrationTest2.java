package ao.adoop.test.utils.usermodules;

import java.io.File;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;

public class MapperForIntegrationTest2 extends Mapper {


	public MapperForIntegrationTest2(String workerId, Configuration config, File inputFile,
			int startIndex, int endIndex) {
		super(workerId, config, inputFile, startIndex, endIndex);
	}

	@Override
	public void map(String key, String value, Context context) {
		String[] parts = value.split(",");
		context.write(parts[2], parts[3]);
	}

}
