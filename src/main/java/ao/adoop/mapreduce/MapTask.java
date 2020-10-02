package ao.adoop.mapreduce;

public class MapTask {

	private MapInputSplit inputSplit;
	private Class<? extends Mapper> mapperClass;

	public MapTask(Class<? extends Mapper> mapperClass, MapInputSplit inputSplit) {
		this.mapperClass = mapperClass;
		this.inputSplit = inputSplit;
	};
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class<? extends Mapper> getMapperClass() {
		return this.mapperClass;
	};
	
	public MapInputSplit getInputSplit() {
		return this.inputSplit;
	}

}
