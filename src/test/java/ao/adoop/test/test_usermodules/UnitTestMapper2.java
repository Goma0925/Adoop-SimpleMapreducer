package ao.adoop.test.test_usermodules;

import java.io.File;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;

public class UnitTestMapper2 extends Mapper{

	public UnitTestMapper2(String workerId, Configuration config, File inputFile, Integer startIndex,
			Integer endIndex, String[] addedNamedOutputs) {
		super(workerId, config, inputFile, startIndex, endIndex, addedNamedOutputs);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
