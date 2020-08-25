package ao.adoop.test.integration;

import java.io.File;
import java.util.ArrayList;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.settings.SystemPathSettings;

public class ReducerForIntegrationTest extends Reducer {

	public ReducerForIntegrationTest(String workerId, SystemPathSettings systemPathSetting,
			ArrayList<File> inputFiles) {
		super(workerId, systemPathSetting, inputFiles);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void reduce(String key, ArrayList<String> inputLines, Context context) {
		// TODO Auto-generated method stub

	}

}
