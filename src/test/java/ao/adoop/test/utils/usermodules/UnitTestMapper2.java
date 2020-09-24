package ao.adoop.test.utils.usermodules;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.InputSplit;
import ao.adoop.mapreduce.Mapper;

public class UnitTestMapper2 extends Mapper{

	public UnitTestMapper2(String workerId, Configuration config, InputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
