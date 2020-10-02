package ao.adoop.test.utils.usermodules;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MapInputSplit;
import ao.adoop.mapreduce.Mapper;

public class MapperForIntegrationTest2 extends Mapper {


	public MapperForIntegrationTest2(String workerId, Configuration config, MapInputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	@Override
	public void map(String key, String value, Context context) {
		String[] parts = value.split(",");
		context.write(parts[2], parts[3]);
	}

}
