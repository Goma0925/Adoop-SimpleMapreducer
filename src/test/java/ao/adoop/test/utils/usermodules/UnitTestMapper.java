package ao.adoop.test.utils.usermodules;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MapInputSplit;
import ao.adoop.mapreduce.Mapper;

public class UnitTestMapper extends Mapper{


	public UnitTestMapper(String workerId, Configuration config, MapInputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	public void map(String key, String value, Context context) {
		String[] arrOfStr = value.split("/", 3);
		context.write(arrOfStr[1], arrOfStr[2]);
	};
}
