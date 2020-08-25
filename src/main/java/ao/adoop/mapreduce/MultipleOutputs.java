package ao.adoop.mapreduce;

public class MultipleOutputs {
	Context context = null;
	
	public MultipleOutputs(Context context) {
		this.context = context;
	}
	public void write(String namedOutput, String key, String value, String baseOutputPath) {
		this.context.writeToNameSpace(namedOutput, key, value, baseOutputPath);
		
	}
	public static void addNamedOutput(Job job, String namedOutput) {
		// TODO Auto-generated method stub
		
	}
}
