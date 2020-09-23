package ao.adoop.test.utils.usermodules;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Reducer;

public class UnitTestReducer extends Reducer {

	public UnitTestReducer(String workerId, Configuration config, ArrayList<File> inputFiles) {
		super(workerId, config, inputFiles);
	}

	public void reduce(String key, ArrayList<String> inputLines, Context context) {
		HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		int size = inputLines.size();
		String current = null;
		for (int i=0; i<size; i++) {
			current = inputLines.get(i);
			if (current.contains("target-value")) {
				countMap.put(current, countMap.getOrDefault(current, 0)+1);
			}
		};
		for (String keyStr: countMap.keySet()) {
			context.write(keyStr, Integer.toString(countMap.get(keyStr)));
		}
	};
}
