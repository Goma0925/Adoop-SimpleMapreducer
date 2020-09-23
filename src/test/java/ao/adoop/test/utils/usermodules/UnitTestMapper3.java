package ao.adoop.test.utils.usermodules;

import java.io.File;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;

public class UnitTestMapper3 extends Mapper{

	public UnitTestMapper3(String workerId, Configuration config, File inputFile, int startIndex, int endIndex) {
		super(workerId, config, inputFile, startIndex, endIndex);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
