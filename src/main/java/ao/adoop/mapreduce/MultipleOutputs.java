package ao.adoop.mapreduce;

public class MultipleOutputs {
	Context context = null;
	
	public MultipleOutputs(Context context) {
		this.context = context;
	}
	public void write(String nameSpace, String key, String value, String baseOutputPath) {
		this.context.writeToNameSpace(nameSpace, key, value, baseOutputPath);
		
	}
}
