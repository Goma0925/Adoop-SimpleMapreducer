package ao.adoop.mapreduce;

public class MapTask {

	private InputSplit inputSplit;
	private Class<? extends Mapper> mapperClass;

	public MapTask(Class<? extends Mapper> mapperClass, InputSplit inputSplit) {
		this.mapperClass = mapperClass;
		this.inputSplit = inputSplit;
	};
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class<? extends Mapper> getMapperClass() {
		return this.mapperClass;
	};
	
	public InputSplit getInputSplit() {
		return this.inputSplit;
	}

}
