package test_usermodules;

import java.io.File;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;
import ao.adoop.settings.SystemPathSettings;

public class UnitTestMapper3 extends Mapper{

	public UnitTestMapper3(String workerId, SystemPathSettings pathSettings, File inputFile, int startIndex, int endIndex) {
		super(workerId, pathSettings, inputFile, startIndex, endIndex);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
