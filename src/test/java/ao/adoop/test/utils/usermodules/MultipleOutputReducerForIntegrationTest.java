package ao.adoop.test.utils.usermodules;

import java.util.ArrayList;
import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.MultipleOutputs;
import ao.adoop.mapreduce.ReduceInputSplit;
import ao.adoop.mapreduce.Reducer;

public class MultipleOutputReducerForIntegrationTest extends Reducer {
	public MultipleOutputReducerForIntegrationTest(String workerId, Configuration config,
			ReduceInputSplit inputSplit) {
		super(workerId, config, inputSplit);
	}

	MultipleOutputs multipleOutputs = null;
	protected void setup(Context context) {
		this.multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	public void reduce(String key, ArrayList<String> values, Context context) {
//		Map<String, Integer> counts = new HashMap<String, Integer>();
		String error = "";
		int count = 0;
		int length = values.size();
		for (int i=0; i<length; i++) {
			try {
				count += Integer.parseInt(values.get(i));			
			}catch(Exception e){
				// Record the error value and its key.
				error += key + " : " + values.get(i) + "\n";
			}
		};
		
		//Direct outputs of different keys to different output directories.
		if (key.equals("KEY=1") || key.equals("KEY=2")) {
			// The third parameter expresses a relative output path within the outputDir specified in Driver.
			// The results of KEY=1 and KEY=2 will be written to output-dir/GROUP1
			this.multipleOutputs.write(key, Integer.toString(count), "/GROUP1/");
		}else {
			this.multipleOutputs.write(key, Integer.toString(count), "/GROUP2/");
		}
		if (!error.equals("")) {
			// You can also direct error output to different file.
			this.multipleOutputs.write("ERROR", error, "/ERROR/");
		}
		
	}

}
