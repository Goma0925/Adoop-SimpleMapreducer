package test_usermodules;

import java.io.File;

import adoop.Context;
import adoop.Mapper;
import settings.SystemPathSettings;

public class TestMapper3 extends Mapper{

	public TestMapper3(String workerId, SystemPathSettings pathSettings, File inputFile, int startIndex, int endIndex) {
		super(workerId, pathSettings, inputFile, startIndex, endIndex);
	}

	@Override
	public void map(String key, String value, Context context) {
		context.write("Key", "Value");
	}

}
