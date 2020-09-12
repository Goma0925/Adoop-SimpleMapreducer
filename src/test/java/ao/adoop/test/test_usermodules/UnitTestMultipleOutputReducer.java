package ao.adoop.test.test_usermodules;

import java.io.File;
import java.util.ArrayList;

import javax.naming.InvalidNameException;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.mapreduce.Reducer;

public class UnitTestMultipleOutputReducer extends Reducer {
	MultipleOutputs multipleOutput = null;
	public UnitTestMultipleOutputReducer(String workerId, Configuration config, ArrayList<File> inputFiles,
			String[] addedNamedOutputs) {
		super(workerId, config, inputFiles, addedNamedOutputs);
	}

	@Override
	public void setup(Context context) {
		this.multipleOutput = new MultipleOutputs(context);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) throws InvalidNameException {
		int targetCount = 0;
		int otherCount = 0;
		for (String value: values) {
			if (value.contains("target-value")) {
				targetCount += 1;
			}else {
				otherCount += 1;
			}
		};
		this.multipleOutput.write("target-count", Integer.toString(targetCount), "output-dir1");
		this.multipleOutput.write("other-count", Integer.toString(otherCount), "output-dir2");

	}

}
