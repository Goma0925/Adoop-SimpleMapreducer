package ao.adoop.test.utils.usermodules;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.naming.InvalidNameException;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.mapreduce.Reducer;

public class MultipleOutputReducerForIntegrationTest extends Reducer {
	public MultipleOutputReducerForIntegrationTest(String workerId, Configuration config,
			ArrayList<File> inputFiles) {
		super(workerId, config, inputFiles);
	}

	MultipleOutputs multipleOutputs = null;
	protected void setup(Context context) {
		this.multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) throws InvalidNameException {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		int length = values.size();
		String error = "";
		for (int i=0; i<length; i++) {
			try {
				counts.putIfAbsent(key, 0);
				counts.put(key, counts.get(key) + Integer.parseInt(values.get(i)));				
			}catch(Exception e){
				error += values.get(i);
			}
		};
		if (key.equals("KEY=1") || key.equals("KEY=2")) {
			this.multipleOutputs.write(key, Integer.toString(counts.get(key)), "/GROUP1/");
		}else {
			this.multipleOutputs.write(key, Integer.toString(counts.get(key)), "/GROUP2/");
		}
		if (!error.equals("")) {
			this.multipleOutputs.write("ERROR", error, "/ERROR/");
		}
		
	}

}
