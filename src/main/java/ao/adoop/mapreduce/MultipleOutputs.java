package ao.adoop.mapreduce;

import java.util.ArrayList;

import javax.naming.InvalidNameException;

public class MultipleOutputs {
	Context context = null;
	
	public MultipleOutputs(Context context) {
		this.context = context;
	}
	public void write(String namedOutput, String key, String value, String baseOutputPath) throws InvalidNameException {
		this.context.writeToNamedOutput(namedOutput, key, value, baseOutputPath);
		
	}
	public static void addNamedOutput(Job job, String namedOutput) {
		job.addOutputNameSpace(namedOutput);
	}
}
