package test_usermodules;

import java.util.ArrayList;
import adoop.Context;
import adoop.Reducer;

public class TestReducer implements Reducer {

	public void reduce(String key, ArrayList<String> inputLines, Context context) {
		context.write(key, Integer.toString(inputLines.size()));
	}

}
