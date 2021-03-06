package ao.adoop.test.utils.usermodules;

import java.util.ArrayList;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.ReduceInputSplit;
import ao.adoop.mapreduce.Reducer;

public class ReducerForIntegrationTest extends Reducer {

	public ReducerForIntegrationTest(String workerId, Configuration config, ReduceInputSplit inputSplit) {
		super(workerId, config, inputSplit);
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
