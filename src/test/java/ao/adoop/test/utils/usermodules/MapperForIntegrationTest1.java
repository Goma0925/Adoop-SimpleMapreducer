package ao.adoop.test.utils.usermodules;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MapInputSplit;
import ao.adoop.mapreduce.MapTask;
import ao.adoop.mapreduce.Mapper;

public class MapperForIntegrationTest1 extends Mapper {

	public MapperForIntegrationTest1(String workerId, Configuration config, MapInputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	@Override
	public void map(String key, String value, Context context) {
		String[] parts = value.split(",");
		context.write(parts[1], parts[2]);
	}

}
