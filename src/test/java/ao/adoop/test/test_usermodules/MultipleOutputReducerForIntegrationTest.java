package ao.adoop.test.test_usermodules;

import java.io.File;
import java.util.ArrayList;

import javax.naming.InvalidNameException;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.mapreduce.Reducer;
import ao.adoop.settings.SystemPathSettings;

public class MultipleOutputReducerForIntegrationTest extends Reducer {
	public MultipleOutputReducerForIntegrationTest(String workerId, SystemPathSettings systemPathSetting,
			ArrayList<File> inputFiles, String[] addedNamedOutputs) {
		super(workerId, systemPathSetting, inputFiles, addedNamedOutputs);
	}

	MultipleOutputs multipleOutputs = null;
	protected void setup(Context context) {
		this.multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) throws InvalidNameException {
		for (String value: values) {
			if (key.equals("KEY=1") || key.equals("KEY=2")) {
				this.multipleOutputs.write("GROUP-1", key, value, "/key-1-and-2/");
			}else if (key.equals("KEY=3") || key.equals("KEY=4")) {
				this.multipleOutputs.write("GROUP-2", key, value, "/key-3-and-4/");
			}
		}

	}

}
