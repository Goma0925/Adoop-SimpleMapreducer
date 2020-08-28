package ao.adoop.test.test_usermodules;

import java.io.File;
import java.util.ArrayList;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.settings.SystemPathSettings;

public class ReducerForIntegrationTest extends Reducer {

	public ReducerForIntegrationTest(String workerId, SystemPathSettings systemPathSetting,
			ArrayList<File> inputFiles) {
		super(workerId, systemPathSetting, inputFiles);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) {
		int sum = 0;
		for (String value: values) {
			sum += Integer.parseInt(value);
		};
		context.write(key, Integer.toString(sum));
	}

}
