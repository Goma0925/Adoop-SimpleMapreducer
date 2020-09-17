package ao.adoop.test.utils.usermodules;

import java.io.File;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;

public class MapperForIntegrationTest1 extends Mapper {

	public MapperForIntegrationTest1(String workerId, Configuration config, File inputFile,
			int startIndex, int endIndex, String[] addedNamedOutputs) {
		super(workerId, config, inputFile, startIndex, endIndex, addedNamedOutputs);
	}

	@Override
	public void map(String key, String value, Context context) {
		String[] parts = value.split(",");
		context.write(parts[1], parts[2]);
	}

}
