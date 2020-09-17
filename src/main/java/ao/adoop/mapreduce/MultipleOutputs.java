package ao.adoop.mapreduce;

import java.util.ArrayList;

import javax.naming.InvalidNameException;

public class MultipleOutputs {
	Context context = null;
	
	public MultipleOutputs(Context context) {
		this.context = context;
	}
	public void write(String key, String value, String baseOutputPath) {
		this.context.writeToBaseOutputPath(key, value, baseOutputPath);
		
	}
	public static void addNamedOutput(Job job, String namedOutput) {
		job.addNamedOutput(namedOutput);
	}
}
