package adoop;

import java.util.ArrayList;

public interface Reducer {
	void reduce(String key, ArrayList<String> inputLines, Context context);
}
