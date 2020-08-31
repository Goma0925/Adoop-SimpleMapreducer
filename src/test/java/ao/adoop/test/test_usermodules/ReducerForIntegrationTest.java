package ao.adoop.test.test_usermodules;

import java.io.File;
import java.util.ArrayList;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.settings.SystemPathSettings;

public class ReducerForIntegrationTest extends Reducer {

	public ReducerForIntegrationTest(String workerId, SystemPathSettings systemPathSetting, ArrayList<File> inputFiles,
			String[] addedNamedOutputs) {
		super(workerId, systemPathSetting, inputFiles, addedNamedOutputs);
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
