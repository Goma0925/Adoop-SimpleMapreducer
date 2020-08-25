package ao.adoop.test.test_usermodules;

import java.io.File;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;
import ao.adoop.settings.SystemPathSettings;

public class UnitTestMapper2 extends Mapper{

	public UnitTestMapper2(String workerId, SystemPathSettings pathSettings, File inputFile, int startIndex, int endIndex) {
		super(workerId, pathSettings, inputFile, startIndex, endIndex);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
